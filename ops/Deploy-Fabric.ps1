<#
Deploy Dev/Test/Prod using Fabric REST:

- Resolves the deployment pipeline by name or ID.
- Resolves stages by displayName ("Development", "Test", "Production").
- Discovers items from the source stage, falling back to the workspace if needed.
- Deploys:
    - Lakehouse
    - DataPipeline (PL_BRONZE, PL_SILVER, PL_GOLD, PL_Payroll, etc.)
    - Notebook
    - Dataflow / DataflowGen2 (DF_GOLD)
- Excludes:
    - SemanticModel, Report, VariableLibrary, SQLEndpoint, Dashboard
    - Warehouse (WH_GOLD etc., due to SP limitations)
    - Staging lakehouses/warehouses (StagingLakehouseForDataflows_*, StagingWarehouseForDataflows_*)
- Optional selective deploy via -ItemsJson, otherwise deploys all discovered items minus exclusions.
#>

param(
  [Parameter(Mandatory = $true)][string]$TenantId,
  [Parameter(Mandatory = $true)][string]$ClientId,
  [Parameter(Mandatory = $true)][string]$ClientSecret,

  [string]$PipelineId = "",
  [Parameter(Mandatory = $false)][string]$PipelineName = "",

  [Alias('SourceStageName')]
  [string]$SourceStage = "Development",

  [Alias('TargetStageName')]
  [string]$TargetStage = "Production",

  [string]$Note = "CI/CD deploy via GitHub Actions",

  # Optional JSON array of items to deploy:
  # e.g. '[{"itemDisplayName":"LH_BRONZE","itemType":"Lakehouse"}]'
  [string]$ItemsJson = "",

  # Item types to exclude from deploy
  [string[]]$ExcludeItemTypes = @(
    "SemanticModel",
    "Report",
    "VariableLibrary",
    "SQLEndpoint",
    "Dashboard",
    "Warehouse"    # avoid PrincipalTypeNotSupported for Warehouse
  ),

  # Name patterns to exclude (internal/staging artifacts, etc.)
  [string[]]$ExcludeNamePatterns = @(
    "StagingLakehouseForDataflows_",
    "StagingWarehouseForDataflows_"
  )
)

$ErrorActionPreference = "Stop"
$base = "https://api.fabric.microsoft.com/v1"

# Types that the deploy API can accept (workspace item 'type' or stage item 'itemType')
$SupportedTypes = @(
  "Lakehouse",
  "DataPipeline",
  "Notebook",
  "SemanticModel",
  "Report",
  "Warehouse",
  "Dataflow",
  "DataflowGen2",
  "Dashboard",
  "SQLEndpoint",
  "VariableLibrary"
)

function Get-FabricToken {
  param(
    [string]$TenantId,
    [string]$ClientId,
    [string]$ClientSecret
  )

  $body = @{
    client_id     = $ClientId
    client_secret = $ClientSecret
    scope         = "https://api.fabric.microsoft.com/.default"
    grant_type    = "client_credentials"
  }

  $tokenUri = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
  Write-Host "Requesting Fabric access token..."
  (Invoke-RestMethod -Method POST -Uri $tokenUri -Body $body -ContentType "application/x-www-form-urlencoded").access_token
}

$token   = Get-FabricToken -TenantId $TenantId -ClientId $ClientId -ClientSecret $ClientSecret
$Headers = @{ Authorization = "Bearer $token" }

function GetJson {
  param([string]$Uri)

  Write-Host "GET $Uri"
  Invoke-RestMethod -Method GET -Uri $Uri -Headers $Headers
}

function PostLro {
  param(
    [string]$Uri,
    [object]$Body
  )

  Write-Host "POST $Uri"
  $json = $Body | ConvertTo-Json -Depth 50

  $resp = Invoke-WebRequest -Method POST -Uri $Uri -Headers $Headers -Body $json -ContentType "application/json" -MaximumRedirection 0

  # If the API returns 2xx with a body and no LRO, just return it
  if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 300 -and -not $resp.Headers["Operation-Location"]) {
    if ($resp.Content) {
      return ($resp.Content | ConvertFrom-Json)
    }
    return $null
  }

  # Long-running operation pattern (202 + Operation-Location)
  $opUrl = $resp.Headers["Operation-Location"]
  if (-not $opUrl) { $opUrl = $resp.Headers["operation-location"] }
  if (-not $opUrl) { throw "No Operation-Location header returned for async operation." }

  Write-Host "Long-running operation URL: $opUrl"

  while ($true) {
    Start-Sleep -Seconds 5
    Write-Host "Polling LRO status..."

    $lro = $null
    try {
      $lro = Invoke-RestMethod -Method GET -Uri $opUrl -Headers $Headers
    }
    catch {
      Write-Host "LRO status poll failed once, retrying..." -ForegroundColor Yellow
      continue
    }

    if ($lro -and $lro.status) {
      Write-Host "LRO status: $($lro.status)"
      if ($lro.status -eq "Succeeded") {
        return $lro
      }
      elseif ($lro.status -eq "Failed" -or $lro.status -eq "Cancelled") {
        throw "Deployment LRO ended with status '$($lro.status)'. Details: $($lro | ConvertTo-Json -Depth 10)"
      }
    }
    else {
      Write-Host "No status in LRO result yet..."
    }
  }
}

function Filter-Excluded {
  param(
    [array]$Items,
    [string[]]$Exclude,
    [string[]]$ExcludeNamePatterns
  )

  $kept = @()
  foreach ($i in $Items) {
    $name = if ($null -ne $i.itemDisplayName) { $i.itemDisplayName } else { "" }
    $type = $i.itemType

    # Type-based exclusion
    if ($Exclude -contains $type) {
      Write-Host "Skipping item due to ExcludeItemTypes: $name [$type]"
      continue
    }

    # Name-based exclusion (patterns)
    if ($name -and $ExcludeNamePatterns -and $ExcludeNamePatterns.Count -gt 0) {
      $skipByName = $false
      foreach ($pat in $ExcludeNamePatterns) {
        if ($name -like ($pat + "*")) {
          Write-Host "Skipping item due to ExcludeNamePatterns ('$pat'): $name [$type]"
          $skipByName = $true
          break
        }
      }
      if ($skipByName) { continue }
    }

    $kept += $i
  }
  return $kept
}

# ---------------------- Resolve pipeline ----------------------

$pipeId = $PipelineId
if (-not $pipeId) {
  if ([string]::IsNullOrWhiteSpace($PipelineName)) {
    throw "Either PipelineId or PipelineName must be provided."
  }

  $all = (GetJson "$base/deploymentPipelines").value
  if (-not $all) { throw "No deployment pipelines visible to the service principal." }

  $targetName = $PipelineName.Trim()

  $pipe = $all | Where-Object {
    $dn = $_.displayName
    if ($null -eq $dn) { $dn = "" }
    $dn.Trim() -ieq $targetName
  }

  if (-not $pipe) {
    $names = ($all | ForEach-Object { $_.displayName }) -join ", "
    throw "Deployment pipeline '$targetName' not found. Visible pipelines: $names"
  }

  $pipeId = $pipe.id
}
else {
  $pipe = GetJson "$base/deploymentPipelines/$pipeId"
}

Write-Host "Pipeline: $($pipe.displayName) [$pipeId]"

# ---------------------- Resolve stages ----------------------

$stages = (GetJson "$base/deploymentPipelines/$pipeId/stages").value

Write-Host "Stages returned by API:" -ForegroundColor Cyan
foreach ($st in $stages) {
  $name      = $st.displayName
  $stageType = $st.stageType
  $wsName    = $st.workspaceName
  $wsId      = $st.workspaceId
  Write-Host (" - displayName='{0}', stageType='{1}', workspaceName='{2}', workspaceId='{3}'" -f $name, $stageType, $wsName, $wsId)
}

$src = $stages | Where-Object {
  $disp = $_.displayName
  if ($null -eq $disp) { $disp = "" }
  $disp.Trim() -ieq $SourceStage.Trim()
}
$dst = $stages | Where-Object {
  $disp = $_.displayName
  if ($null -eq $disp) { $disp = "" }
  $disp.Trim() -ieq $TargetStage.Trim()
}

if ($src.Count -gt 1) {
  $matches = ($src | ForEach-Object { "'$($_.displayName)' (ID: $($_.id))" }) -join ", "
  throw "Multiple source stages match '$SourceStage': $matches"
}
if ($dst.Count -gt 1) {
  $matches = ($dst | ForEach-Object { "'$($_.displayName)' (ID: $($_.id))" }) -join ", "
  throw "Multiple target stages match '$TargetStage': $matches"
}

if (-not $src) {
  $validNames = ($stages | ForEach-Object { $_.displayName }) -join ", "
  throw "Source stage '$SourceStage' not found. Valid stages: $validNames"
}
if (-not $dst) {
  $validNames = ($stages | ForEach-Object { $_.displayName }) -join ", "
  throw "Target stage '$TargetStage' not found. Valid stages: $validNames"
}

Write-Host "Source stage: $($src.displayName) [$($src.id)] (workspaceName='$($src.workspaceName)')" -ForegroundColor Green
Write-Host "Target stage: $($dst.displayName) [$($dst.id)] (workspaceName='$($dst.workspaceName)')" -ForegroundColor Green

# ---------------------- Discover items ----------------------

$stageItemsUri = "$base/deploymentPipelines/$pipeId/stages/$($src.id)/items"
$stageItemsResult = $null
try {
  $stageItemsResult = GetJson -Uri $stageItemsUri
}
catch {
  Write-Host "Failed to get items from stage endpoint. Will fall back to workspace items." -ForegroundColor Yellow
}

$stageItems = @()
$itemsFrom  = ""

if ($stageItemsResult -and $stageItemsResult.value) {
  $stageItems = $stageItemsResult.value | Where-Object {
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

  $itemsFrom = "stage"
}

if (-not $stageItems -or $stageItems.Count -eq 0) {
  Write-Host "No deployable items from stage endpoint; falling back to workspace items for $($src.workspaceId) ..." -ForegroundColor Yellow

  $wsItems = (GetJson "$base/workspaces/$($src.workspaceId)/items").value

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
foreach ($i in $stageItems) {
  Write-Host " - $($i.itemDisplayName) [$($i.itemType)] (id=$($i.sourceItemId), source=$($i.source))"
}

# ---------------------- Build list of items to deploy ----------------------

$itemsToSend = @()

if ($ItemsJson -and $ItemsJson.Trim()) {
  Write-Host "Selective deploy based on ItemsJson..." -ForegroundColor Yellow
  $requested = $ItemsJson | ConvertFrom-Json

  foreach ($req in $requested) {
    $typ   = if ($null -ne $req.itemType) { $req.itemType.Trim() } else { "" }
    $disp  = if ($null -ne $req.itemDisplayName) { $req.itemDisplayName.Trim() } else { "" }
    $srcId = if ($null -ne $req.sourceItemId) { $req.sourceItemId.Trim() } else { "" }

    if (-not ($SupportedTypes -contains $typ)) {
      Write-Host "Skipping requested item '$disp' [$typ] — not in SupportedTypes."
      continue
    }

    $match = $null

    if ($srcId -and $srcId -match '^[0-9a-fA-F-]{36}$') {
      $match = $stageItems | Where-Object { $_.sourceItemId -eq $srcId }
    }
    elseif ($disp -and $typ) {
      $match = $stageItems | Where-Object {
        $dName = if ($null -ne $_.itemDisplayName) { $_.itemDisplayName } else { "" }
        $iType = if ($null -ne $_.itemType)        { $_.itemType        } else { "" }

        ($dName.Trim() -ieq $disp) -and
        ($iType.Trim() -ieq $typ)
      }
    }

    if (-not $match) {
      Write-Host "Requested item not found among discovered items: '$disp' [$typ]. Skipping."
      continue
    }

    foreach ($m in $match) {
      $itemsToSend += [PSCustomObject]@{
        sourceItemId    = $m.sourceItemId
        itemType        = $m.itemType
        itemDisplayName = $m.itemDisplayName
      }
    }
  }
}
else {
  # No selective list → deploy all discovered items
  $itemsToSend = $stageItems | ForEach-Object {
    [PSCustomObject]@{
      sourceItemId    = $_.sourceItemId
      itemType        = $_.itemType
      itemDisplayName = $_.itemDisplayName
    }
  }
}

# Apply ExcludeItemTypes + name patterns
$itemsToSend = Filter-Excluded -Items $itemsToSend -Exclude $ExcludeItemTypes -ExcludeNamePatterns $ExcludeNamePatterns

if (-not $itemsToSend -or $itemsToSend.Count -eq 0) {
  throw "After filtering to SupportedTypes and applying ExcludeItemTypes/NamePatterns, no items remain to deploy."
}

Write-Host "Items to deploy (from $itemsFrom):"
foreach ($i in $itemsToSend) {
  Write-Host " - $($i.itemDisplayName) [$($i.itemType)] (id=$($i.sourceItemId))"
}

# ---------------------- Call deploy API ----------------------

$body = @{
  sourceStageId = $src.id
  targetStageId = $dst.id
  note          = $Note
  options       = @{
    allowOverwriteArtifact = $true
    allowCreateArtifact    = $true
  }
  items = ($itemsToSend | ForEach-Object {
    @{
      sourceItemId = $_.sourceItemId
      itemType     = $_.itemType
    }
  })
}

$deployUri = "$base/deploymentPipelines/$pipeId/deploy"
Write-Host "Deploy URI: $deployUri"

$result = PostLro -Uri $deployUri -Body $body

Write-Host "✅ Deployment Succeeded."
if ($result) {
  $result | ConvertTo-Json -Depth 20
}
