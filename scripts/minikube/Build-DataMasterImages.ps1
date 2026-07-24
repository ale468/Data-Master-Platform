[CmdletBinding()]
param(
    [string]$Tag,

    [string]$AirflowRepository = "data-master-airflow",

    [string]$SparkRepository = "data-master-spark-jobs",

    [switch]$AllowDirty
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")

$root = Get-DataMasterRepositoryRoot
if (-not $Tag) {
    $Tag = Get-DataMasterImageTag
}
if ($Tag -notmatch "^git-[0-9a-f]{7,40}$") {
    throw "Image tag must be immutable and match git-<sha>: $Tag"
}
if (-not $AllowDirty) {
    $changes = & git -C $root status --porcelain
    if ($changes) {
        throw "Refusing to build a Git-tagged image from a dirty worktree. Commit first or use -AllowDirty explicitly."
    }
}

$airflowImage = "${AirflowRepository}:$Tag"
$sparkImage = "${SparkRepository}:$Tag"
Invoke-DataMasterNative -FilePath "docker" -Arguments @(
    "build", "-f", (Join-Path $root "Dockerfile.airflow"),
    "-t", $airflowImage, $root
)
Invoke-DataMasterNative -FilePath "docker" -Arguments @(
    "build", "-f", (Join-Path $root "Dockerfile.spark"),
    "-t", $sparkImage, $root
)

$airflowRole = Invoke-DataMasterNative -FilePath "docker" -CaptureOutput -Arguments @(
    "run", "--rm", $airflowImage, "bash", "-ec",
    'printf ''%s'' "$AIRFLOW_IMAGE_ROLE"'
)
if (($airflowRole -join "`n") -notmatch "ORCHESTRATION_ONLY") {
    throw "Airflow image role marker is invalid."
}
$airflowPackages = Invoke-DataMasterNative -FilePath "docker" -CaptureOutput -Arguments @(
    "run", "--rm", $airflowImage, "python", "-c",
    "import importlib.util as u; print('YES' if u.find_spec('pyspark') or u.find_spec('delta') else 'NO')"
)
if (($airflowPackages -join "`n") -notmatch "(?m)^NO$") {
    throw "Airflow image unexpectedly contains Spark processing packages."
}
$sparkRole = Invoke-DataMasterNative -FilePath "docker" -CaptureOutput -Arguments @(
    "run", "--rm", $sparkImage, "bash", "-ec",
    'test -f jobs/kubernetes/run_pipeline_stage.py && printf ''%s'' "$SPARK_IMAGE_ROLE"'
)
if (($sparkRole -join "`n") -notmatch "PROCESSING") {
    throw "Spark image role or jobs payload is invalid."
}

Write-Output "IMAGE_TAG=$Tag"
Write-Output "AIRFLOW_IMAGE=$airflowImage"
Write-Output "SPARK_IMAGE=$sparkImage"
Write-Output "AIRFLOW_IMAGE_ROLE=ORCHESTRATION_ONLY"
Write-Output "AIRFLOW_IMAGE_CONTAINS_PYSPARK=NO"
Write-Output "AIRFLOW_IMAGE_CONTAINS_DELTA_SPARK=NO"
Write-Output "SPARK_IMAGE_ROLE=PROCESSING"
Write-Output "SPARK_IMAGE_CONTAINS_JOBS=YES"
Write-Output "IMAGE_BUILD_STATUS=PASS"
