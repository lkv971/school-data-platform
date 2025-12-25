param(
  [Parameter(Mandatory = $true)][string]$TenantId,
  [Parameter(Mandatory = $true)][string]$ClientId,
  [Parameter(Mandatory = $true)][string]$ClientSecret,

  [Parameter(Mandatory = $true)][string]$PipelineName,
  [Parameter(Mandatory = $true)][string]$SourceStage,
  [Parameter(Mandatory = $true)][string]$TargetStage,

  [string]$Note = "CI/CD deploy via GitHub Actions",

  # Optional JSON array for selective deployments
  [string]$ItemsJson = "",

  # Default safe exclusions (Warehouse excluded: SPN deploy may return PrincipalTypeNotSupported)
  [string[]]$ExcludeItemTypes = @(
    "Warehouse",
    "VariableLibrary",
    "SemanticModel",
    "Report",
    "Dashboard",
    "SQLEndpoint"
  ),

  # Exclude dataflow staging artifacts
  [string[]]$ExcludeNamePatterns = @(
    "StagingLakehouseForDataflows_",
    "StagingWarehouseForDataflows_"
  ),

  # Enterprise reliability knobs
  [int]$DeployMaxRetries = 10,
  [int]$DeployRetryDelaySeconds = 60,
  [int]$OperationPollSeconds = 5
)

$ErrorActionPreference = "Stop"
$base = "https://api.fabric.microsoft.com/v1"

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

function IsGuid([string]$s) { return ($s -match '^[0-9a-fA-F-]{36}$') }

function SafeTrim([object]$v) {
  if ($null -eq $v) { return "" }
  return ($v.ToString()).Trim()
}

function Is-ExcludedByName([string]$name, [string[]]$patterns) {
  foreach ($p in $patterns) { if ($name -like "*$p*") { return $true } }
  return $false
}

function Pick-ItemIdFromStageRow($row) {
  # Prefer the stage item id first (crucial for Test -> Prod).
  $candidates = @()
  if ($row.PSObject.Properties.Name -contains "id")          { $candidates += (SafeTrim $row.id) }
  if ($row.PSObject.Properties.Name -contains "itemId")      { $candidates += (SafeTrim $row.itemId) }
  if ($row.PSObject.Properties.Name -contains "sourceItemId") { $candidates += (SafeTrim $row.sourceItemId) }

  foreach ($c in $candidates) {
    if (IsGuid $c) { return $c }
  }
  return ""
}

function TryParse-ErrorJson([string]$content) {
  if (-not $content) { return $null }
  try { return ($content | ConvertFrom-Json) } catch { return $null }
}

function Invoke-DeployPost([string]$Uri, [hashtable]$Headers, [string]$JsonBody) {
  # We use Invoke-WebRequest to get Operation-Location header reliably
  try {
    return Invoke-WebRequest -Method POST -Uri $Uri -Headers $Headers `
                             -Body $JsonBody -ContentType "application/json" `
                             -MaximumRedirection 0 -ErrorAction Stop
  }
  catch {
    # PowerShell's Web cmdlets throw on non-2xx. Extract status + body.
    $ex = $_.Exception
    $statusCode = $null
    $content = $null

    if ($ex.PSObject.Properties.Name -contains "Response" -and $ex.Response) {
      $resp = $ex.Response
      try { $statusCode = [int]$resp.StatusCode } catch {}
      try {
        if ($resp.Content) { $content = $resp.Content.ReadAsStringAsync().Result }
      } catch {}
    }

    # Fallback: sometimes ErrorDetails.Message carries the JSON body
    if (-not $content -and $_.ErrorDetails -and $_.ErrorDetails.Message) {
      $content = $_.ErrorDetails.Message
    }

    return [PSCustomObject]@{
      __isError   = $true
      StatusCode  = $statusCode
      Content     = $content
      Exception   = $ex
    }
  }
}

function PostDeploy([string]$Uri, [hashtable]$Headers, [object]$Body) {
  Write-Host "POST $Uri"
  $json = $Body | ConvertTo-Json -Depth 50

  $retryableErrorCodes = @(
    "WorkspaceMigrationOperationInProgress",
    "TooManyRequests",
    "Throttled",
    "ServiceUnavailable"
  )
  $retryableHttpStatus = @(429, 500, 502, 503, 504)

  for ($attempt = 1; $attempt -le $DeployMaxRetries; $attempt++) {
    Write-Host ("POST attempt {0}/{1}" -f $attempt, $DeployMaxRetries)

    $resp = Invoke-DeployPost -Uri $Uri -Headers $Headers -JsonBody $json

    # If we got a real response object, check status code
    if ($resp -and -not ($resp.PSObject.Properties.Name -contains "__isError")) {
      if ($resp.StatusCode -eq 202 -or $resp.StatusCode -eq 200) {
        $opUrl = $resp.Headers["Operation-Location"]
        if (-not $opUrl) {
          if ($resp.Content) { return ($resp.Content | ConvertFrom-Json) }
          return $null
        }

        while ($true) {
          Start-Sleep -Seconds $OperationPollSeconds
          $op = Invoke-RestMethod -Method GET -Uri $opUrl -Headers $Headers
          $status = $op.status
          Write-Host "Operation status: $status"
          if ($status -eq "Running" -or $status -eq "NotStarted") { continue }
          if ($status -eq "Succeeded") { return $op }
          throw "Deployment failed. Status=$status Details=$($op | ConvertTo-Json -Depth 20)"
        }
      }

      # Non-success but non-throw (rare)
      Write-Host ("HTTP Status: {0}" -f $resp.StatusCode) -ForegroundColor Yellow
      if ($resp.Content) { Write-Host $resp.Content }
      throw "Deployment POST failed with status $($resp.StatusCode)"
    }

    # Error wrapper from catch
    $statusCode = $resp.StatusCode
    $content = $resp.Content
    $errObj = TryParse-ErrorJson -content $content

    $errCode = $null
    $errMsg  = $null
    if ($errObj) {
      $errCode = $errObj.errorCode
      $errMsg  = $errObj.message
    }

    if ($statusCode) {
      Write-Host ("HTTP Status: {0}" -f $statusCode) -ForegroundColor Yellow
    }
    if ($errCode) {
      Write-Host ("Fabric errorCode: {0}" -f $errCode) -ForegroundColor Yellow
    }
    if ($errMsg) {
      Write-Host ("Message: {0}" -f $errMsg) -ForegroundColor Yellow
    }
    if ($content -and -not $errObj) {
      Write-Host $content -ForegroundColor Yellow
    }

    $shouldRetry =
      (($errCode -and ($retryableErrorCodes -contains $errCode)) -or
       ($statusCode -and ($retryableHttpStatus -contains [int]$statusCode)))

    if ($shouldRetry -and $attempt -lt $DeployMaxRetries) {
      Write-Host ("Retrying in {0} seconds..." -f $DeployRetryDelaySeconds) -ForegroundColor Yellow
      Start-Sleep -Seconds $DeployRetryDelaySeconds
      continue
    }

    # Not retryable or max attempts exceeded
    if ($errCode) {
      throw ("Deployment POST failed. errorCode={0} message={1}" -f $errCode, $errMsg)
    }
    if ($statusCode) {
      throw ("Deployment POST failed with HTTP {0}" -f $statusCode)
    }
    throw "Deployment POST failed with an unknown error."
  }

  throw ("Deployment blocked/retrying exceeded max attempts ({0})." -f $DeployMaxRetries)
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
    $id  = Pick-ItemIdFromStageRow $row
    $typ = SafeTrim $row.itemType
    $nm  = SafeTrim $row.itemDisplayName

    if (-not (IsGuid $id)) { continue }
    if (-not ($SupportedTypes -contains $typ)) { continue }

    $discovered += [PSCustomObject]@{
      sourceItemId    = $id
      itemType        = $typ
      itemDisplayName = $nm
    }
  }
}

if (-not $discovered -or $discovered.Count -eq 0) {
  Write-Host ("No items returned by stage endpoint; falling back to workspace items for {0} ..." -f $src.workspaceId) -ForegroundColor Yellow
  $itemsFrom = "workspace"

  $wsItems = (GetJson "$base/workspaces/$($src.workspaceId)/items" $headers).value
  foreach ($row in $wsItems) {
    $id  = SafeTrim $row.id
    $typ = SafeTrim $row.type
    $nm  = SafeTrim $row.displayName

    if (-not (IsGuid $id)) { continue }
    if (-not ($SupportedTypes -contains $typ)) { continue }

    $discovered += [PSCustomObject]@{
      sourceItemId    = $id
      itemType        = $typ
      itemDisplayName = $nm
    }
  }
}

if (-not $discovered -or $discovered.Count -eq 0) {
  throw "No deployable items discovered from stage OR workspace."
}

Write-Host ("Items discovered from {0}:" -f $itemsFrom)
foreach ($i in $discovered) {
  Write-Host (" - {0} [{1}] (id={2})" -f $i.itemDisplayName, $i.itemType, $i.sourceItemId)
}

# ---------------- Build Items to Deploy ----------------
$itemsToDeploy = @()
if ($ItemsJson -and $ItemsJson.Trim()) {
  Write-Host "Selective deploy based on ItemsJson..."
  $requested = $ItemsJson | ConvertFrom-Json

  foreach ($req in $requested) {
    $disp = SafeTrim $req.itemDisplayName
    $typ  = SafeTrim $req.itemType
    if (-not $disp -or -not $typ) { continue }

    $match = $discovered | Where-Object { $_.itemDisplayName -ieq $disp -and $_.itemType -ieq $typ } | Select-Object -First 1
    if ($match) { $itemsToDeploy += $match }
    else { Write-Host ("Requested item not found: '{0}' [{1}] — skipping." -f $disp, $typ) }
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
$result = PostDeploy $deployUri $headers $body

Write-Host "✅ Deployment succeeded."
$result | ConvertTo-Json -Depth 20
