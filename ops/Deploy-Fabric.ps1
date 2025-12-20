param(
  [Parameter(Mandatory = $true)][string]$TenantId,
  [Parameter(Mandatory = $true)][string]$ClientId,
  [Parameter(Mandatory = $true)][string]$ClientSecret,

  [Parameter(Mandatory = $true)][string]$PipelineName,

  [Parameter(Mandatory = $true)][string]$SourceStage,
  [Parameter(Mandatory = $true)][string]$TargetStage,

  [string]$Note = "CI/CD deploy via GitHub Actions",

  # Optional JSON array of items to deploy:
  # e.g. '[{"itemDisplayName":"LH_BRONZE","itemType":"Lakehouse"}]'
  [string]$ItemsJson = "",

  # Exclude types that should NOT be deployed automatically (esp. env-specific)
  [string[]]$ExcludeItemTypes = @(
    "VariableLibrary",
    "SemanticModel",
    "Report",
    "Dashboard",
    "SQLEndpoint"
  ),

  # Exclude internal staging artifacts created by dataflows
  [string[]]$ExcludeNamePatterns = @(
    "StagingLakehouseForDataflows_",
    "StagingWarehouseForDataflows_"
  )
)

$ErrorActionPreference = "Stop"
$base = "https://api.fabric.microsoft.com/v1"

function Get-FabricToken {
  param([string]$TenantId,[string]$ClientId,[string]$ClientSecret)

  $tokenUri = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
  $body = @{
    client_id     = $ClientId
    client_secret = $ClientSecret
    scope         = "https://api.fabric.microsoft.com/.default"
    grant_type    = "client_credentials"
  }

  Write-Host "Requesting Fabric access token..."
  (Invoke-RestMethod -Method POST -Uri $tokenUri -Body $body -ContentType "application/x-www-form-urlencoded").access_token
}

function GetJson([string]$Uri, [hashtable]$Headers) {
  Write-Host "GET $Uri"
  Invoke-RestMethod -Method GET -Uri $Uri -Headers $Headers
}

function PostLro([string]$Uri, [hashtable]$Headers, [object]$Body) {
  Write-Host "POST $Uri"
  $json = $Body | ConvertTo-Json -Depth 50

  $resp = Invoke-WebRequest -Method POST -Uri $Uri -Headers $Headers `
                            -Body $json -ContentType "application/json" `
                            -MaximumRedirection 0 -ErrorAction SilentlyContinue

  if ($resp.StatusCode -ne 202 -and $resp.StatusCode -ne 200) {
    Write-Host $resp.Content
    throw "Deployment POST failed with status $($resp.StatusCode)"
  }

  $opUrl = $resp.Headers["Operation-Location"]
  if (-not $opUrl) {
    return ($resp.Content | ConvertFrom-Json)
  }

  while ($true) {
    Start-Sleep -Seconds 5
    $op = Invoke-RestMethod -Method GET -Uri $opUrl -Headers $Headers
    $status = $op.status
    Write-Host "Operation status: $status"
    if ($status -eq "Running" -or $status -eq "NotStarted") { continue }
    if ($status -eq "Succeeded") { return $op }
    throw "Deployment failed. Status=$status Details=$($op | ConvertTo-Json -Depth 20)"
  }
}

function Is-ExcludedByName([string]$name, [string[]]$patterns) {
  foreach ($p in $patterns) {
    if ($name -like "*$p*") { return $true }
  }
  return $false
}

# ---------------- Auth ----------------
$token = Get-FabricToken -TenantId $TenantId -ClientId $ClientId -ClientSecret $ClientSecret
$headers = @{ Authorization = "Bearer $token" }

# ---------------- Resolve Pipeline ----------------
$pipes = (GetJson "$base/deploymentPipelines" $headers).value
$pipe = $pipes | Where-Object { $_.displayName -eq $PipelineName } | Select-Object -First 1
if (-not $pipe) { throw "Deployment pipeline '$PipelineName' not found." }
$pipeId = $pipe.id
Write-Host "Pipeline: $($pipe.displayName) [$pipeId]"

# ---------------- Resolve Stages ----------------
$stages = (GetJson "$base/deploymentPipelines/$pipeId/stages" $headers).value

Write-Host "Stages returned by API:"
foreach ($s in $stages) {
  Write-Host " - displayName='$($s.displayName)', workspaceName='$($s.workspaceName)', workspaceId='$($s.workspaceId)'"
}

$src = $stages | Where-Object { $_.displayName -eq $SourceStage } | Select-Object -First 1
$dst = $stages | Where-Object { $_.displayName -eq $TargetStage } | Select-Object -First 1

if (-not $src) { throw "Source stage '$SourceStage' not found in pipeline." }
if (-not $dst) { throw "Target stage '$TargetStage' not found in pipeline." }

Write-Host "Source stage: $($src.displayName) [$($src.id)] (workspaceName='$($src.workspaceName)')"
Write-Host "Target stage: $($dst.displayName) [$($dst.id)] (workspaceName='$($dst.workspaceName)')"

# ---------------- Discover Items from Stage ----------------
$stageItemsResp = GetJson "$base/deploymentPipelines/$pipeId/stages/$($src.id)/items" $headers
$stageItemsRaw  = $stageItemsResp.value

$discovered = @()
if ($stageItemsRaw) {
  $discovered = $stageItemsRaw | Where-Object {
    $_.sourceItemId -and ($_.sourceItemId -match '^[0-9a-fA-F-]{36}$')
  } | ForEach-Object {
    [PSCustomObject]@{
      sourceItemId    = $_.sourceItemId
      itemType        = $_.itemType
      itemDisplayName = $_.itemDisplayName
    }
  }
}

if (-not $discovered -or $discovered.Count -eq 0) {
  throw "No items discovered from stage endpoint. Ensure the source stage has content and is assigned to a workspace."
}

Write-Host "Items discovered from stage:"
foreach ($i in $discovered) {
  Write-Host " - $($i.itemDisplayName) [$($i.itemType)] (id=$($i.sourceItemId))"
}

# ---------------- Build Items to Deploy ----------------
$itemsToDeploy = @()

if ($ItemsJson -and $ItemsJson.Trim()) {
  Write-Host "Selective deploy based on ItemsJson..." -ForegroundColor Yellow
  $requested = $ItemsJson | ConvertFrom-Json

  foreach ($req in $requested) {
    $disp = ($req.itemDisplayName ?? "").Trim()
    $typ  = ($req.itemType ?? "").Trim()

    if (-not $disp -or -not $typ) { continue }

    $match = $discovered | Where-Object {
      $_.itemDisplayName -ieq $disp -and $_.itemType -ieq $typ
    } | Select-Object -First 1

    if ($match) {
      $itemsToDeploy += $match
    } else {
      Write-Host "Requested item not found in stage: '$disp' [$typ] — skipping."
    }
  }
}
else {
  # Full deploy mode
  $itemsToDeploy = $discovered
}

# Apply exclusions
$final = @()
foreach ($it in $itemsToDeploy) {

  if ($ExcludeItemTypes -contains $it.itemType) {
    Write-Host "Skipping item due to ExcludeItemTypes: $($it.itemDisplayName) [$($it.itemType)]"
    continue
  }

  if (Is-ExcludedByName $it.itemDisplayName $ExcludeNamePatterns) {
    Write-Host "Skipping item due to ExcludeNamePatterns: $($it.itemDisplayName) [$($it.itemType)]"
    continue
  }

  $final += $it
}

if (-not $final -or $final.Count -eq 0) {
  throw "After filtering, no items remain to deploy."
}

Write-Host "Items to deploy:"
foreach ($i in $final) {
  Write-Host " - $($i.itemDisplayName) [$($i.itemType)] (id=$($i.sourceItemId))"
}

# ---------------- Deploy ----------------
$body = @{
  sourceStageId = $src.id
  targetStageId = $dst.id
  note          = $Note
  options       = @{
    allowOverwriteArtifact = $true
    allowCreateArtifact    = $true
  }
  items = ($final | ForEach-Object {
    @{
      sourceItemId = $_.sourceItemId
      itemType     = $_.itemType
    }
  })
}

$deployUri = "$base/deploymentPipelines/$pipeId/deploy"
$result = PostLro $deployUri $headers $body

Write-Host "✅ Deployment succeeded."
$result | ConvertTo-Json -Depth 20
