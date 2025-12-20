param(
  [Parameter(Mandatory = $true)][string]$TenantId,
  [Parameter(Mandatory = $true)][string]$ClientId,
  [Parameter(Mandatory = $true)][string]$ClientSecret,

  [Parameter(Mandatory = $true)][string]$PipelineName,
  [Parameter(Mandatory = $true)][string]$SourceStage,
  [Parameter(Mandatory = $true)][string]$TargetStage,

  [string]$Note = "CI/CD deploy via GitHub Actions",

  # Optional JSON array for selective deployments:
  # Example:
  # [
  #   { "itemDisplayName": "LH_BRONZE", "itemType": "Lakehouse" },
  #   { "itemDisplayName": "NB_SILVER", "itemType": "Notebook" }
  # ]
  [string]$ItemsJson = "",

  # Default exclusions (enterprise-friendly: don’t overwrite environment-specific things)
  [string[]]$ExcludeItemTypes = @(
    "VariableLibrary",
    "SemanticModel",
    "Report",
    "Dashboard",
    "SQLEndpoint"
  ),

  # Dataflow staging artifacts (skip)
  [string[]]$ExcludeNamePatterns = @(
    "StagingLakehouseForDataflows_",
    "StagingWarehouseForDataflows_"
  )
)

$ErrorActionPreference = "Stop"
$base = "https://api.fabric.microsoft.com/v1"

# Types we understand; anything else will be ignored safely
$SupportedTypes = @(
  "Lakehouse",
  "DataPipeline",
  "Notebook",
  "Warehouse",
  "Dataflow",
  "DataflowGen2",
  "VariableLibrary",
  "SemanticModel",
  "Report",
  "Dashboard",
  "SQLEndpoint"
)

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
  if (-not $opUrl) { return ($resp.Content | ConvertFrom-Json) }

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

function IsGuid([string]$s) {
  return ($s -match '^[0-9a-fA-F-]{36}$')
}

function Is-ExcludedByName([string]$name, [string[]]$patterns) {
  foreach ($p in $patterns) {
    if ($name -like "*$p*") { return $true }
  }
  return $false
}

function SafeTrim([object]$v) {
  if ($null -eq $v) { return "" }
  return ($v.ToString()).Trim()
}

# ---------------- Auth ----------------
$token = Get-FabricToken -TenantId $TenantId -ClientId $ClientId -ClientSecret $ClientSecret
$headers = @{ Authorization = "Bearer $token" }

# ---------------- Resolve Pipeline ----------------
$pipes = (GetJson "$base/deploymentPipelines" $headers).value
$pipe = $pipes | Where-Object { $_.displayName -eq $PipelineName } | Select-Object -First 1
if (-not $pipe) { throw "Deployment pipeline '$PipelineName' not found." }

$pipeId = $pipe.id
Write-Host ("Pipeline: {0} [{1}]" -f $pipe.displayName, $pipeId)

# ---------------- Resolve Stages ----------------
$stages = (GetJson "$base/deploymentPipelines/$pipeId/stages" $headers).value

Write-Host "Stages returned by API:"
foreach ($s in $stages) {
  Write-Host (" - displayName='{0}', workspaceName='{1}', workspaceId='{2}'" -f $s.displayName, $s.workspaceName, $s.workspaceId)
}

$src = $stages | Where-Object { $_.displayName -eq $SourceStage } | Select-Object -First 1
$dst = $stages | Where-Object { $_.displayName -eq $TargetStage } | Select-Object -First 1

if (-not $src) { throw "Source stage '$SourceStage' not found in pipeline." }
if (-not $dst) { throw "Target stage '$TargetStage' not found in pipeline." }
if (-not $src.workspaceId) { throw "Source stage has no workspaceId assigned." }

Write-Host ("Source stage: {0} [{1}] (workspaceName='{2}')" -f $src.displayName, $src.id, $src.workspaceName)
Write-Host ("Target stage: {0} [{1}] (workspaceName='{2}')" -f $dst.displayName, $dst.id, $dst.workspaceName)

# ---------------- Discover Items (stage first; fallback to workspace) ----------------
$discovered = @()
$itemsFrom = "stage"

$stageItemsResp = GetJson "$base/deploymentPipelines/$pipeId/stages/$($src.id)/items" $headers
$stageItemsRaw  = $stageItemsResp.value

if ($stageItemsRaw) {
  foreach ($row in $stageItemsRaw) {
    $id  = $null
    if ($row.PSObject.Properties.Name -contains "sourceItemId") { $id = $row.sourceItemId }
    elseif ($row.PSObject.Properties.Name -contains "id")       { $id = $row.id }

    $typ = $row.itemType
    $nm  = $row.itemDisplayName

    if (-not (IsGuid (SafeTrim $id))) { continue }
    if (-not ($SupportedTypes -contains (SafeTrim $typ))) { continue }

    $discovered += [PSCustomObject]@{
      sourceItemId    = (SafeTrim $id)
      itemType        = (SafeTrim $typ)
      itemDisplayName = (SafeTrim $nm)
    }
  }
}

if (-not $discovered -or $discovered.Count -eq 0) {
  Write-Host ("No items returned by stage endpoint; falling back to workspace items for {0} ..." -f $src.workspaceId) -ForegroundColor Yellow
  $itemsFrom = "workspace"

  $wsItems = (GetJson "$base/workspaces/$($src.workspaceId)/items" $headers).value

  foreach ($row in $wsItems) {
    $id  = $row.id
    $typ = $row.type
    $nm  = $row.displayName

    if (-not (IsGuid (SafeTrim $id))) { continue }
    if (-not ($SupportedTypes -contains (SafeTrim $typ))) { continue }

    $discovered += [PSCustomObject]@{
      sourceItemId    = (SafeTrim $id)
      itemType        = (SafeTrim $typ)
      itemDisplayName = (SafeTrim $nm)
    }
  }
}

if (-not $discovered -or $discovered.Count -eq 0) {
  throw "No deployable items discovered from stage OR workspace. Check SPN permissions and that the workspace contains items."
}

Write-Host ("Items discovered from {0}:" -f $itemsFrom)
foreach ($i in $discovered) {
  Write-Host (" - {0} [{1}] (id={2})" -f $i.itemDisplayName, $i.itemType, $i.sourceItemId)
}

# ---------------- Build Items to Deploy ----------------
$itemsToDeploy = @()

if ($ItemsJson -and $ItemsJson.Trim()) {
  Write-Host "Selective deploy based on ItemsJson..." -ForegroundColor Yellow
  $requested = $ItemsJson | ConvertFrom-Json

  foreach ($req in $requested) {
    $disp = SafeTrim $req.itemDisplayName
    $typ  = SafeTrim $req.itemType
    if (-not $disp -or -not $typ) { continue }

    $match = $discovered | Where-Object { $_.itemDisplayName -ieq $disp -and $_.itemType -ieq $typ } | Select-Object -First 1
    if ($match) {
      $itemsToDeploy += $match
    } else {
      Write-Host ("Requested item not found: '{0}' [{1}] — skipping." -f $disp, $typ)
    }
  }
} else {
  $itemsToDeploy = $discovered
}

# Apply exclusions
$final = @()
foreach ($it in $itemsToDeploy) {

  if ($ExcludeItemTypes -contains $it.itemType) {
    Write-Host ("Skipping due to ExcludeItemTypes: {0} [{1}]" -f $it.itemDisplayName, $it.itemType)
    continue
  }

  if (Is-ExcludedByName $it.itemDisplayName $ExcludeNamePatterns) {
    Write-Host ("Skipping due to ExcludeNamePatterns: {0} [{1}]" -f $it.itemDisplayName, $it.itemType)
    continue
  }

  $final += $it
}

if (-not $final -or $final.Count -eq 0) {
  throw "After filtering, no items remain to deploy."
}

Write-Host "Items to deploy:"
foreach ($i in $final) {
  Write-Host (" - {0} [{1}] (id={2})" -f $i.itemDisplayName, $i.itemType, $i.sourceItemId)
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
