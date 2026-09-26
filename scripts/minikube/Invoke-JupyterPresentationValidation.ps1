# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

[CmdletBinding()]
param(
    [string]$Profile = "data-master-jupyter-validation",

    [string]$EvidencePath,

    [ValidateRange(120, 1800)]
    [int]$TimeoutSeconds = 900
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")

function Assert-JupyterResultProperty {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Object,

        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    if ($null -eq $Object.PSObject.Properties[$Name]) {
        throw "Jupyter validation result is missing property '$Name'."
    }
}

Set-DataMasterMinikubeContext -Profile $Profile
$root = Get-DataMasterRepositoryRoot

$remaining = [math]::Max(1, $TimeoutSeconds)
Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
    "rollout", "status", "deployment/jupyter", "-n", "data-platform",
    "--timeout=${remaining}s"
)

$podJson = Invoke-DataMasterNative -FilePath "kubectl" -CaptureOutput -Arguments @(
    "get", "pods", "-n", "data-platform",
    "-l", "app.kubernetes.io/name=jupyter", "-o", "json"
)
$pods = @(
    (($podJson -join "") | ConvertFrom-Json | Select-Object -ExpandProperty items) |
        Where-Object {
            $null -eq $_.metadata.PSObject.Properties["deletionTimestamp"]
        }
)
if ($pods.Count -ne 1) {
    throw "Expected exactly one active Jupyter pod; observed $($pods.Count)."
}
$pod = $pods[0]
$readyCondition = @($pod.status.conditions | Where-Object {
    $_.type -eq "Ready" -and $_.status -eq "True"
})
if ($readyCondition.Count -ne 1) {
    throw "Jupyter pod is not Ready."
}
$containerStatus = @($pod.status.containerStatuses | Where-Object {
    $_.name -eq "jupyter"
})
if ($containerStatus.Count -ne 1 -or -not $containerStatus[0].ready) {
    throw "Jupyter container status is unavailable or not ready."
}
$imageReference = [string]$pod.spec.containers[0].image
$imageId = [string]$containerStatus[0].imageID
if ($imageReference -notmatch '^data-master-jupyter:git-[0-9a-f]{7,40}$') {
    throw "Jupyter pod must use an immutable project image: $imageReference"
}
if ($imageId -notmatch '^docker(?:-pullable)?://sha256:[0-9a-f]{64}$') {
    throw "Jupyter pod image ID is not an immutable SHA-256 reference."
}

$apiProbe = @'
import os
import urllib.request

token = os.environ["JUPYTER_TOKEN"]
request = urllib.request.Request(
    "http://127.0.0.1:8888/api",
    headers={"Authorization": "token " + token},
)
with urllib.request.urlopen(request, timeout=15) as response:
    if response.status != 200:
        raise SystemExit("Jupyter API did not return HTTP 200")
print("JUPYTER_AUTHENTICATED_API_STATUS=PASS")
'@
$apiOutput = Invoke-DataMasterNative -FilePath "kubectl" -CaptureOutput -Arguments @(
    "exec", "deployment/jupyter", "-n", "data-platform", "--",
    "python3", "-c", $apiProbe
)
if (($apiOutput -join "`n") -notmatch '(?m)^JUPYTER_AUTHENTICATED_API_STATUS=PASS$') {
    throw "Authenticated Jupyter API probe did not pass."
}

$notebookProbe = @'
import json
from pathlib import Path

path = Path("/opt/spark/work-dir/jobs/presentation/notebooks/data_master_delta_presentation.ipynb")
document = json.loads(path.read_text(encoding="utf-8"))
if document.get("nbformat") != 4 or len(document.get("cells", [])) < 7:
    raise SystemExit("Presentation notebook contract is invalid")
if any(cell.get("outputs") for cell in document["cells"] if cell.get("cell_type") == "code"):
    raise SystemExit("Presentation notebook must not contain committed outputs")
print("JUPYTER_NOTEBOOK_CONTRACT_STATUS=PASS")
'@
$notebookOutput = Invoke-DataMasterNative -FilePath "kubectl" `
    -CaptureOutput -Arguments @(
        "exec", "deployment/jupyter", "-n", "data-platform", "--",
        "python3", "-c", $notebookProbe
    )
if (($notebookOutput -join "`n") -notmatch '(?m)^JUPYTER_NOTEBOOK_CONTRACT_STATUS=PASS$') {
    throw "Jupyter presentation notebook contract did not pass."
}

$validationOutput = Invoke-DataMasterNative -FilePath "kubectl" `
    -CaptureOutput -Arguments @(
        "exec", "deployment/jupyter", "-n", "data-platform", "--",
        "python3", "-B",
        "/opt/spark/work-dir/jobs/presentation/run_jupyter_delta_validation.py"
    )
$resultMarkers = @($validationOutput | Where-Object {
    [string]$_ -match '^JUPYTER_PRESENTATION_RESULT='
})
if ($resultMarkers.Count -ne 1) {
    throw "Expected exactly one Jupyter presentation result marker."
}
$result = ([string]$resultMarkers[0] -replace '^JUPYTER_PRESENTATION_RESULT=', '') |
    ConvertFrom-Json
foreach ($property in @(
    "schema_version", "evidence_kind", "status", "image_role", "session",
    "layers", "prepared_gold_query", "snapshot_consistency", "privacy",
    "limitations"
)) {
    Assert-JupyterResultProperty -Object $result -Name $property
}
if (
    [int]$result.schema_version -ne 1 -or
    $result.evidence_kind -ne "data_master_jupyter_presentation_validation" -or
    $result.status -ne "PASS" -or
    $result.image_role -ne "PRESENTATION_READ_ONLY" -or
    $result.snapshot_consistency -ne "PASS"
) {
    throw "Jupyter presentation result header is invalid."
}
if (
    $result.session.spark_version -ne "3.3.1" -or
    $result.session.delta_version -ne "2.2.0" -or
    $result.session.delta_extension -ne "io.delta.sql.DeltaSparkSessionExtension" -or
    $result.session.delta_catalog -ne "org.apache.spark.sql.delta.catalog.DeltaCatalog" -or
    $result.session.credentials_provider -ne "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
) {
    throw "Jupyter Spark/Delta/S3A session contract is invalid."
}
$expectedViews = [ordered]@{
    bronze = "bronze_transacoes"
    raw_vault = "raw_hub_transacao"
    gold = "gold_transacoes_por_dia"
}
foreach ($layer in $expectedViews.Keys) {
    $layerResult = $result.layers.PSObject.Properties[$layer]
    if ($null -eq $layerResult) {
        throw "Jupyter result is missing layer '$layer'."
    }
    if (
        $layerResult.Value.view -ne $expectedViews[$layer] -or
        [long]$layerResult.Value.row_count -lt 1 -or
        [long]$layerResult.Value.delta_version -lt 0 -or
        $layerResult.Value.snapshot_stable -ne $true
    ) {
        throw "Jupyter layer '$layer' failed its view or snapshot contract."
    }
}
if (
    $result.prepared_gold_query.status -ne "PASS" -or
    $result.prepared_gold_query.view -ne "gold_transacoes_por_dia" -or
    [long]$result.prepared_gold_query.result_rows -lt 1
) {
    throw "Prepared Gold presentation query did not pass."
}
if (
    $result.privacy.classification -ne "technical_aggregate_only" -or
    $result.privacy.contains_pii -ne $false -or
    $result.privacy.contains_secrets -ne $false -or
    $result.privacy.contains_business_payload -ne $false
) {
    throw "Jupyter presentation evidence is not technical-only."
}

$evidence = [ordered]@{
    schema_version = 1
    evidence_kind = "data_master_jupyter_presentation_validation"
    captured_at = (Get-Date).ToUniversalTime().ToString("o")
    profile = $Profile
    status = "PASS"
    jupyter = [ordered]@{
        api = "PASS"
        notebook = "PASS"
        pod_ready = $true
        restart_count = [int]$containerStatus[0].restartCount
        image = $imageReference
        image_id = ($imageId -replace '^docker(?:-pullable)?://', '')
    }
    session = $result.session
    layers = $result.layers
    prepared_gold_query = $result.prepared_gold_query
    snapshot_consistency = $result.snapshot_consistency
    privacy = $result.privacy
    limitations = @($result.limitations)
}

if ($EvidencePath) {
    if (-not [System.IO.Path]::IsPathRooted($EvidencePath)) {
        $EvidencePath = Join-Path $root $EvidencePath
    }
    $EvidencePath = [System.IO.Path]::GetFullPath($EvidencePath)
    if ([System.IO.Path]::GetExtension($EvidencePath) -ne ".json") {
        throw "Jupyter evidence path must use the .json extension."
    }
    $parent = Split-Path -Parent $EvidencePath
    New-Item -ItemType Directory -Path $parent -Force | Out-Null
    [System.IO.File]::WriteAllText(
        $EvidencePath,
        ($evidence | ConvertTo-Json -Depth 12),
        (New-Object System.Text.UTF8Encoding($false))
    )
    Write-Output "JUPYTER_PRESENTATION_EVIDENCE_PATH=$EvidencePath"
}

Write-Output "JUPYTER_POD_READINESS_STATUS=PASS"
Write-Output "JUPYTER_AUTHENTICATED_API_STATUS=PASS"
Write-Output "JUPYTER_SPARK_SESSION_STATUS=PASS"
Write-Output "JUPYTER_DELTA_MINIO_STATUS=PASS"
Write-Output "JUPYTER_BRONZE_VIEW_STATUS=PASS"
Write-Output "JUPYTER_RAW_VAULT_VIEW_STATUS=PASS"
Write-Output "JUPYTER_GOLD_VIEW_STATUS=PASS"
Write-Output "JUPYTER_SNAPSHOT_CONSISTENCY_STATUS=PASS"
Write-Output "JUPYTER_PRESENTATION_VALIDATION_STATUS=PASS"
