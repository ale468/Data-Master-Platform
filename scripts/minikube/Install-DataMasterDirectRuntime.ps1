[CmdletBinding()]
param(
    [string]$Profile = "data-master-repro-test",

    [string]$ImageTag,

    [string]$AirflowImageRepository = "data-master-airflow",

    [string]$SparkImageRepository = "data-master-spark-jobs",

    [ValidateRange(120, 1800)]
    [int]$TimeoutSeconds = 900
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")

Set-DataMasterMinikubeContext -Profile $Profile
if (-not $ImageTag) { $ImageTag = Get-DataMasterImageTag }
if ($ImageTag -notmatch "^git-[0-9a-f]{7,40}$") {
    throw "Image tag must be immutable and match git-<sha>: $ImageTag"
}
$root = Get-DataMasterRepositoryRoot

Invoke-DataMasterNative -FilePath "helm" -Arguments @(
    "repo", "add", "spark-operator", "https://kubeflow.github.io/spark-operator",
    "--force-update"
)
Invoke-DataMasterNative -FilePath "helm" -Arguments @("repo", "update", "spark-operator")
Invoke-DataMasterNative -FilePath "helm" -Arguments @(
    "upgrade", "--install", "spark-operator", "spark-operator/spark-operator",
    "--version", "2.5.0",
    "--namespace", "spark-operator",
    "--create-namespace",
    "--set", "webhook.enable=true",
    "--set", "spark.jobNamespaces[0]=data-platform",
    "--wait", "--timeout", "${TimeoutSeconds}s"
)
Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
    "apply", "-f", (Join-Path $root "infra\workloads\spark-apps\rbac")
)

$charts = @(
    @{ Name = "minio"; Extra = @() },
    @{ Name = "postgres-metastore"; Extra = @() },
    @{ Name = "hive-metastore"; Extra = @() },
    @{ Name = "jupyter"; Extra = @() },
    @{ Name = "airflow"; Extra = @(
        "--set-string", "image.repository=$AirflowImageRepository",
        "--set-string", "image.tag=$ImageTag",
        "--set-string", "sparkImage.repository=$SparkImageRepository",
        "--set-string", "sparkImage.tag=$ImageTag"
    ) }
)
foreach ($chart in $charts) {
    $arguments = @(
        "upgrade", "--install", $chart.Name,
        (Join-Path $root "infra\helm-charts\$($chart.Name)"),
        "--namespace", "data-platform",
        "--create-namespace",
        "--wait", "--timeout", "${TimeoutSeconds}s"
    ) + $chart.Extra
    Invoke-DataMasterNative -FilePath "helm" -Arguments $arguments
}

Write-Output "DIRECT_RUNTIME_REVISION_MODE=LOCAL_WORKTREE_FALLBACK"
Write-Output "DIRECT_RUNTIME_IMAGE_TAG=$ImageTag"
Write-Output "DIRECT_RUNTIME_INSTALL_STATUS=PASS"
