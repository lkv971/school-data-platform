<#
Deploy Dev → Prod using Fabric REST:
- Resolve pipeline + stages by NAME.
- Prefer stage items; if none deployable, FALL BACK to workspace items.
- Whitelist supported item types for the Deploy API.
- Default excludes: SemanticModel, Report, VariableLibrary, SQLEndpoint, Dashboard.
- Optional -ItemsJson to deploy a subset by displayName or explicit sourceItemId.
#>

param(
  [Parameter(Mandatory=$true)][string]$TenantId,
  [Parameter(Mandatory=$true)][string]$ClientId,
  [Parameter(Mandatory=$true)][string]$ClientSecret,

  [string]$PipelineId = "",
  [Parameter(Mandatory=$false)][string]$PipelineName = "",

  [Alias('SourceStageName')]
  [string]$SourceStage = "Development",

  [Alias('TargetStageName')]
  [string]$TargetStage = "Production",

  [string]$Note = "CI/CD deploy via GitHub Actions",

  # Optional selective-deploy JSON (array of {itemDisplayName, itemType} or {sourceItemId, itemType})
  [string]$ItemsJson = "",

  # Default exclusions; adjust as needed
  [string[]]$ExcludeItemTypes = @("SemanticModel","Report","VariableLibrary","SQLEndpoint","Dashboard")
)

$ErrorActionPreference = "Stop"

# Base Fabric API URL
$base = "https://api.fabric.microsoft.com/v1"

# Deploy API-supported item types
# (You can trim this list; we keep it broad and exclude via ExcludeItemTypes)
$SupportedTypes = @(
  "Lakehouse",
  "DataPipeline",
  "Notebook",
  "SemanticModel",
  "Report",
  "Warehouse",
  "Dataflow",
  "DataflowGen2"
)

function Get-FabricToken {
  param([string]$TenantId,[string]$ClientId,[string]$ClientSecret)
  $body = @{
    client_id     = $ClientId
    client_secret = $ClientSecret
    scope         = "https://api.fabric.microsoft.com/.default"
    grant_type    = "client_credentials"
  }
  $tokenUri = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
  Write-Host "Requesting Fabric access token..." -ForegroundColor Cyan
  (Invoke-RestMethod -Method POST -Uri $tokenUri -Body $body -ContentType "application/x-www-form-urlencoded").access_token
}

$token = Get-FabricToken $TenantId $ClientId $ClientSecret
$Headers = @{ Authorization = "Bearer $token" }

function GetJson($uri) {
  if ($uri -is [array]) { $uri = $uri[0] }
  Write-Host "GET $uri" -ForegroundColor DarkCyan
  Invoke-RestMethod -Headers $Headers -Uri $uri -Method GET
}

function PostLro($uri, $obj) {
  if ($uri -is [array]) { $uri = $uri[0] }
  Write-Host "POST $uri" -ForegroundColor DarkCyan
  $json = $obj | ConvertTo-Json -Depth 20
  Write-Host "Body: $json" -ForegroundColor DarkGray

  $resp = Invoke-WebRequest -Method POST -Uri $uri -Headers $Headers -ContentType "application/json" -Body $json -MaximumRedirection 0

  $opUrl = $resp.Headers["Operation-Location"]
  if (-not $opUrl) { $opUrl = $resp.Headers["operation-location"] }
  if (-not $opUrl) { $opUrl = $resp.Headers["Location"] }

  if ($resp.StatusCode -eq 202 -and $opUrl) {
    if ($opUrl -is [array]) { $opUrl = $opUrl[0] }

    do {
      Start-Sleep -Seconds 5
      Write-Host "Polling LRO status..." -ForegroundColor DarkCyan
      try {
        $op = Invoke-RestMethod -Method GET -Uri $opUrl -Headers $Headers
      } catch {
        Write-Host "LRO poll failed once, retrying..." -ForegroundColor DarkYellow
        continue
      }
    } while ($op.status -eq "Running" -or $op.status -eq "NotStarted")

    Write-Host "LRO final status: $($op.status)" -ForegroundColor DarkCyan
    if ($op.status -eq "Failed") {
      try {
        $res = Invoke-RestMethod -Method GET -Uri ($opUrl.TrimEnd('/') + "/result") -Headers $Headers
        $msgs = @("Deployment failed. Status: $($op.status).")
        if ($res) {
          $msgs += "Extended result:"
          $msgs += ($res | ConvertTo-Json -Depth 20)
        }
        throw ($msgs -join "`n")
      } catch {
        throw "Deployment failed. Status: $($op.status)."
      }
    }

    try {
      return Invoke-RestMethod -Method GET -Uri ($opUrl.TrimEnd('/') + "/result") -Headers $Headers
    } catch {
      return $op
    }
  }
  elseif ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 300) {
    if ($resp.Content) { return ($resp.Content | ConvertFrom-Json) } else { return $null }
  }
  else {
    throw "Unexpected response: HTTP $($resp.StatusCode) $($resp.StatusDescription) — $($resp.Content)"
  }
}

function Filter-Excluded($items, $exclude){
  $kept = @()
  foreach($i in $items){
    if ($exclude -contains $i.itemType) {
      Write-Host "Skipping item due to ExcludeItemTypes: $($i.itemDisplayName ?? $i.sourceItemId) [$($i.itemType)]"
      continue
    }
    $kept += $i
  }
  return $kept
}

# ---- Resolve pipeline (DP_LISE or by Id) ----
$pipeId = $PipelineId
if (-not $pipeId) {
  if (-not $PipelineName) { $PipelineName = "DP_LISE" }  # default for this project

  $all = (GetJson "$base/deploymentPipelines").value
  if (-not $all) { throw "No deployment pipelines visible to the service principal." }

  $targetName = ($PipelineName ?? "").Trim()
  $pipe = $all | Where-Object { (($_.displayName ?? "")).Trim() -ieq $targetName }
  if (-not $pipe) {
    Write-Host "Pipelines visible to SPN:"
    foreach($p in $all){ Write-Host " - $($p.displayName) [$($p.id)]" }
    throw "Deployment pipeline '$PipelineName' not found."
  }

  $pipeId = $pipe.id
  Write-Host "Pipeline: $($pipe.displayName) [$pipeId]"
}
else {
  $pipe = GetJson "$base/deploymentPipelines/$pipeId"
  Write-Host "Pipeline: $($pipe.displayName) [$pipeId]"
}

# ---- Resolve stages ----
$src = $pipe.stages | Where-Object { (($_.displayName ?? "")).Trim() -ieq $SourceStage.Trim() }
$dst = $pipe.stages | Where-Object { (($_.displayName ?? "")).Trim() -ieq $TargetStage.Trim() }

if (-not $src) { throw "Source stage '$SourceStage' not found in pipeline." }
if (-not $dst) { throw "Target stage '$TargetStage' not found in pipeline." }

Write-Host "Source stage: $($src.displayName) [$($src.id)]"
Write-Host "Target stage: $($dst.displayName) [$($dst.id)]"

# ---- Resolve source workspaceId from stage
$srcWsId = $src.workspaceId
if (-not $srcWsId -and $src.properties -and $src.properties.workspaceId) {
  $srcWsId = $src.properties.workspaceId
}
if (-not $srcWsId) { throw "Could not resolve the source stage workspaceId. Ensure the stage is assigned to a workspace." }

# ---- Preferred: stage items (if they include sourceItemId GUIDs)
$itemsUri = "$base/deploymentPipelines/$pipeId/stages/$($src.id)/items"
$stageItemsRaw = (GetJson $itemsUri).value

# normalize stage items and keep only SupportedTypes with valid GUIDs
$stageItems = @()
if ($stageItemsRaw) {
  $stageItems = $stageItemsRaw | Where-Object {
    $_.PSObject.Properties.Name -contains "sourceItemId" -and
    $_.sourceItemId -and
    ($_.sourceItemId -match '^[0-9a-fA-F-]{36}$') -and
    ($SupportedTypes -contains $_.itemType)
  } | ForEach-Object {
    [pscustomobject]@{
      sourceItemId    = $_.sourceItemId
      itemType        = $_.itemType
      itemDisplayName = $_.itemDisplayName
      source          = "stage"
    }
  }
}

# ---- Fallback: list items from source workspace if needed
$itemsFrom = "stage"
if (-not $stageItems -or $stageItems.Count -eq 0) {
  Write-Host "No deployable items from stage endpoint; falling back to workspace items for $srcWsId ..."
  $wsItems = (GetJson "$base/workspaces/$srcWsId/items").value

  $stageItems = $wsItems | Where-Object {
    $_.PSObject.Properties.Name -contains "id" -and
    $_.id -and
    ($_.id -match '^[0-9a-fA-F-]{36}$') -and
    ($SupportedTypes -contains $_.type)
  } | ForEach-Object {
    [pscustomobject]@{
      sourceItemId    = $_.id
      itemType        = $_.type
      itemDisplayName = $_.displayName
      source          = "workspace"
    }
  }

  $itemsFrom = "workspace"
}

Write-Host "Items discovered from $itemsFrom :" -ForegroundColor Cyan
$stageItems | ForEach-Object {
  Write-Host " - $($_.itemDisplayName) [$($_.itemType)] from $($_.source)"
}

# ---- Build items list (selective or all), then apply exclusions
$itemsToSend = @()
if ($ItemsJson -and $ItemsJson.Trim() -ne "") {
  Write-Host "Selective deploy based on ItemsJson..." -ForegroundColor Yellow
  $requested = $ItemsJson | ConvertFrom-Json

  foreach($req in $requested) {
    $disp = ($req.itemDisplayName ?? "").Trim()
    $typ  = ($req.itemType ?? "").Trim()
    $srcId = ($req.sourceItemId ?? "").Trim()

    $match = $null
    if ($srcId) {
      $match = $stageItems | Where-Object { $_.sourceItemId -eq $srcId }
    }
    elseif ($disp -and $typ) {
      $match = $stageItems | Where-Object {
        (($_.itemDisplayName ?? "").Trim() -ieq $disp) -and
        (($_.itemType ?? "").Trim() -ieq $typ)
      }
    }

    if (-not $match) {
      Write-Host "WARNING: No stage item matched ItemsJson entry: displayName='$disp', type='$typ', sourceItemId='$srcId'"
    }
    else {
      foreach($m in $match) {
        $itemsToSend += @{
          sourceItemId     = $m.sourceItemId
          itemType         = $m.itemType
          itemDisplayName  = $m.itemDisplayName
        }
      }
    }
  }
}
else {
  $itemsToSend = $stageItems | ForEach-Object {
    @{
      sourceItemId     = $_.sourceItemId
      itemType         = $_.itemType
      itemDisplayName  = $_.itemDisplayName
    }
  }
}

# Apply ExcludeItemTypes filter
if ($ExcludeItemTypes -and $ExcludeItemTypes.Count -gt 0) {
  Write-Host "Applying ExcludeItemTypes filter: $($ExcludeItemTypes -join ', ')" -ForegroundColor Cyan
  $itemsToSend = Filter-Excluded $itemsToSend $ExcludeItemTypes
}

if (-not $itemsToSend -or $itemsToSend.Count -eq 0) {
  throw "No items to deploy after filtering. Check SupportedTypes and ExcludeItemTypes."
}

Write-Host "Final list of items to deploy:" -ForegroundColor Green
foreach($i in $itemsToSend){
  Write-Host " - $($i.itemDisplayName) [$($i.itemType)]"
}

# Build deploy body
$body = @{
  sourceStageId = $src.id
  targetStageId = $dst.id
  note          = $Note
  options       = @{
    allowOverwriteArtifact = $true
    allowCreateArtifact    = $true
  }
  items = ($itemsToSend | ForEach-Object { @{ sourceItemId = $_.sourceItemId; itemType = $_.itemType } })
}

# Kick off deployment
$deployUri = "$base/deploymentPipelines/$pipeId/deploy"
Write-Host "Deploy URI: $deployUri"
$result = PostLro $deployUri $body

Write-Host "✅ Deployment Succeeded."
if ($result) { $result | ConvertTo-Json -Depth 20 }
