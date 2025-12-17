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
$base = "https://api.fabric.microsoft.com/v1"

# Deploy API-supported item types (workspace item 'type' values that are valid to send to /deploy)
# You can add/remove types here if your tenant supports more.
$SupportedTypes = @("Lakehouse","DataPipeline","Notebook","SemanticModel","Report","Warehouse")

function Get-FabricToken {
  param([string]$TenantId,[string]$ClientId,[string]$ClientSecret)
  $body = @{
    client_id     = $ClientId
    client_secret = $ClientSecret
    scope         = "https://api.fabric.microsoft.com/.default"
    grant_type    = "client_credentials"
  }
  $tokenUri = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
  (Invoke-RestMethod -Method POST -Uri $tokenUri -Body $body -ContentType "application/x-www-form-urlencoded").access_token
}

$token = Get-FabricToken -TenantId $TenantId -ClientId $ClientId -ClientSecret $ClientSecret
$Headers = @{ Authorization = "Bearer $token" }

function GetJson($uri){
  Write-Host "GET $uri"
  Invoke-RestMethod -Method GET -Uri $uri -Headers $Headers
}

function PostLro($uri, $body){
  Write-Host "POST $uri"
  $json = $body | ConvertTo-Json -Depth 50
  $resp = Invoke-WebRequest -Method POST -Uri $uri -Headers $Headers -Body $json -ContentType "application/json" -MaximumRedirection 0 -ErrorAction SilentlyContinue

  if ($resp.StatusCode -eq 202 -or $resp.StatusCode -eq 200) {
    $opUrl = $resp.Headers["Operation-Location"]
    if (-not $opUrl) {
      Write-Host "No Operation-Location header; returning raw response body."
      if ($resp.Content) { return ($resp.Content | ConvertFrom-Json) }
      return $null
    }

    Write-Host "Long-running operation URL: $opUrl"
    while($true){
      Start-Sleep -Seconds 5
      Write-Host "Polling LRO status..."
      $lro = Invoke-RestMethod -Method GET -Uri ($opUrl.TrimEnd('/') + "/result") -Headers $Headers -ErrorAction SilentlyContinue
      if ($lro -and $lro.status) {
        Write-Host "LRO status: $($lro.status)"
        if ($lro.status -ieq "succeeded") { return $lro }
        if ($lro.status -ieq "failed" -or $lro.status -ieq "cancelled") {
          throw "Deployment LRO ended with status '$($lro.status)'. Details: $($lro | ConvertTo-Json -Depth 10)"
        }
      } else {
        Write-Host "No status in LRO result yet..."
      }
    }
  } else {
    throw "Unexpected status code '$($resp.StatusCode)' when calling deploy API."
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

# ---- Resolve pipeline ----
$pipeId = $PipelineId
if (-not $pipeId) {
  $all = (GetJson "$base/deploymentPipelines").value
  if (-not $all) { throw "No deployment pipelines visible to the service principal." }
  $targetName = ($PipelineName ?? "").Trim()
  if (-not $targetName) { $targetName = "DP_LISE" }
  $pipe = $all | Where-Object { (($_.displayName ?? "")).Trim() -ieq $targetName }
  if (-not $pipe) {
    Write-Host "Pipelines visible to SPN:"
    foreach($p in $all){ Write-Host " - $($p.displayName) [$($p.id)]" }
    throw "Deployment pipeline '$PipelineName' not found."
  }
  $pipeId = $pipe.id
  Write-Host "Pipeline: $($pipe.displayName) [$pipeId]"
} else {
  $pipe = GetJson "$base/deploymentPipelines/$pipeId"
  Write-Host "Pipeline: $($pipe.displayName) [$pipeId]"
}

# ---- Resolve stages by NAME ----
$stages = (GetJson "$base/deploymentPipelines/$pipeId/stages").value

Write-Host "Stages returned by API:" -ForegroundColor Cyan
foreach($st in $stages){
  $name      = $st.displayName
  $stageType = $st.stageType
  $wsName    = $st.workspaceName
  $wsId      = $st.workspaceId
  Write-Host (" - displayName='{0}', stageType='{1}', workspaceName='{2}', workspaceId='{3}'" -f $name, $stageType, $wsName, $wsId) -ForegroundColor DarkGray
}

# Prefer stageType, fall back to displayName (so 'Development' / 'Test' / 'Production' work)
$src = $stages | Where-Object {
  (($_.stageType   ?? "").Trim() -ieq $SourceStage.Trim()) -or
  (($_.displayName ?? "").Trim() -ieq $SourceStage.Trim())
}
$dst = $stages | Where-Object {
  (($_.stageType   ?? "").Trim() -ieq $TargetStage.Trim()) -or
  (($_.displayName ?? "").Trim() -ieq $TargetStage.Trim())
}

if (-not $src) { throw "Source stage '$SourceStage' not found in pipeline '$($pipe.displayName)'. See 'Stages returned by API' above." }
if (-not $dst) { throw "Target stage '$TargetStage' not found in pipeline '$($pipe.displayName)'. See 'Stages returned by API' above." }

Write-Host "Source stage: $($src.displayName) [$($src.id)] (stageType='$($src.stageType)', workspaceName='$($src.workspaceName)')" -ForegroundColor Cyan
Write-Host "Target stage: $($dst.displayName) [$($dst.id)] (stageType='$($dst.stageType)', workspaceName='$($dst.workspaceName)')" -ForegroundColor Cyan

# Try to read the source workspaceId from the stage
$srcWsId = $null
foreach($prop in @("workspaceId","assignedWorkspaceId","workspaceGuid","workspaceObjectId")){
  if ($src.PSObject.Properties.Name -contains $prop -and $src.$prop) { $srcWsId = $src.$prop; break }
}
if (-not $srcWsId) {
  $srcStageObj = GetJson "$base/deploymentPipelines/$pipeId/stages/$($src.id)"
  foreach($prop in @("workspaceId","assignedWorkspaceId","workspaceGuid","workspaceObjectId")){
    if ($srcStageObj.PSObject.Properties.Name -contains $prop -and $srcStageObj.$prop) { $srcWsId = $srcStageObj.$prop; break }
  }
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
    [PSCustomObject]@{
      sourceItemId    = $_.sourceItemId
      itemType        = $_.itemType
      itemDisplayName = $_.itemDisplayName
      source          = "stage"
    }
  }
}

# ---- Fallback: list items from source workspace if stage items aren't usable
$itemsFrom = "stage"
if (-not $stageItems -or $stageItems.Count -eq 0) {
  Write-Host "No deployable items from stage endpoint; falling back to workspace items for $srcWsId ..."
  $wsItems = (GetJson "$base/workspaces/$srcWsId/items").value

  # Normalize, keep only items whose 'type' is supported, and that have a GUID id
  $stageItems = $wsItems | Where-Object {
    $_.PSObject.Properties.Name -contains "id" -and
    $_.id -and
    ($_.id -match '^[0-9a-fA-F-]{36}$') -and
    ($SupportedTypes -contains $_.type)
  } | ForEach-Object {
    [PSCustomObject]@{
      sourceItemId    = $_.id
      itemType        = $_.type
      itemDisplayName = $_.displayName
      source          = "workspace"
    }
  }
  $itemsFrom = "workspace"
}

if (-not $stageItems -or $stageItems.Count -eq 0) {
  throw "No deployable items found in stage or workspace (after filtering to SupportedTypes)."
}

Write-Host ("Items discovered from {0}" -f $itemsFrom) -ForegroundColor Cyan
foreach($i in $stageItems){ Write-Host " - $($i.itemDisplayName) [$($i.itemType)] (source=$($i.source))" }

# ---- Build items list (selective or all), then apply exclusions
$itemsToSend = @()

if ($ItemsJson -and $ItemsJson.Trim()){
  $wanted = $ItemsJson | ConvertFrom-Json

  foreach($w in $wanted){
    # enforce SupportedTypes even for user-specified list
    if (-not ($SupportedTypes -contains $w.itemType)) {
      Write-Host "Skipping requested item '$($w.itemDisplayName)' [$($w.itemType)] — not in SupportedTypes."
      continue
    }

    if ($w.PSObject.Properties.Name -contains "sourceItemId" -and $w.sourceItemId -match '^[0-9a-fA-F-]{36}$'){
      $itemsToSend += @{
        sourceItemId     = $w.sourceItemId
        itemType         = $w.itemType
        itemDisplayName  = $w.itemDisplayName
      }
    } else {
      if (-not ($w.PSObject.Properties.Name -contains "itemDisplayName")) {
        throw "Each item must have 'sourceItemId' or 'itemDisplayName'. Offending: $($w | ConvertTo-Json -Compress)"
      }
      $match = $stageItems | Where-Object {
        $_.itemDisplayName -eq $w.itemDisplayName -and $_.itemType -eq $w.itemType
      }
      if (-not $match) {
        Write-Host "Requested item not found in $itemsFrom list ...deployable: '$($w.itemDisplayName)' [$($w.itemType)]. Skipping."
        continue
      }
      $itemsToSend += @{
        sourceItemId     = $match.sourceItemId
        itemType         = $match.itemType
        itemDisplayName  = $match.itemDisplayName
      }
    }
  }
} else {
  # No selective list → send all discovered stage/workspace items
  $itemsToSend = $stageItems | ForEach-Object {
    @{
      sourceItemId    = $_.sourceItemId
      itemType        = $_.itemType
      itemDisplayName = $_.itemDisplayName
    }
  }
}

# Exclude unwanted types AFTER filtering/whitelisting
$itemsToSend = Filter-Excluded $itemsToSend $ExcludeItemTypes

if (-not $itemsToSend) {
  throw "After filtering to SupportedTypes and applying exclusions, no items remain to deploy."
}

Write-Host "Items to deploy (from $itemsFrom):"
foreach($i in $itemsToSend){ Write-Host " - $($i.itemDisplayName) [$($i.itemType)]" }

# Build request body
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
