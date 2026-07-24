[CmdletBinding()]
param(
    [string]$Profile = "data-master-repro-test",

    [string]$ImageTag,

    [string]$SparkImageRepository = "data-master-spark-jobs",

    [ValidateRange(120, 10800)]
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
$batchId = (Get-Date).ToUniversalTime().ToString("yyyyMMddHHmmss")
$applicationName = "dm-spark-integration-$batchId"
$root = Get-DataMasterRepositoryRoot
$template = [System.IO.File]::ReadAllText(
    (Join-Path $root "infra\workloads\spark-apps\templates\spark-integration.yaml")
)
$rendered = $template.Replace("__BATCH_ID__", $batchId).Replace(
    "__SPARK_IMAGE__", "${SparkImageRepository}:$ImageTag"
)
$temporaryFile = New-TemporaryFile
$driverSeen = $false
$executorSeen = $false
$state = "UNKNOWN"
try {
    [System.IO.File]::WriteAllText(
        $temporaryFile.FullName,
        $rendered,
        (New-Object System.Text.UTF8Encoding($false))
    )
    Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
        "apply", "-f", $temporaryFile.FullName
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $driverPods = & kubectl get pods -n data-platform `
            -l "sparkoperator.k8s.io/app-name=$applicationName,data-master.io/spark-role=driver" `
            -o name 2>$null
        $executorPods = & kubectl get pods -n data-platform `
            -l "sparkoperator.k8s.io/app-name=$applicationName,data-master.io/spark-role=executor" `
            -o name 2>$null
        if ($driverPods) { $driverSeen = $true }
        if ($executorPods) { $executorSeen = $true }

        $stateOutput = & kubectl get sparkapplication $applicationName -n data-platform `
            -o "jsonpath={.status.applicationState.state}" 2>$null
        if ($stateOutput) {
            $state = ($stateOutput -join "").Trim()
        }
        else {
            $state = "PENDING"
        }
        if ($state -eq "COMPLETED") { break }
        if ($state -in @("FAILED", "FAILING", "SUBMISSION_FAILED", "UNKNOWN")) {
            if ($state -ne "UNKNOWN") { throw "SparkApplication entered state $state." }
        }
        Start-Sleep -Seconds 3
    }
    if ($state -ne "COMPLETED") {
        throw "SparkApplication did not complete before timeout; state=$state"
    }
    if (-not $driverSeen) { throw "No separate Spark driver pod was observed." }
    if (-not $executorSeen) { throw "No separate Spark executor pod was observed." }

    $driverPod = (& kubectl get pods -n data-platform `
        -l "sparkoperator.k8s.io/app-name=$applicationName,data-master.io/spark-role=driver" `
        -o "jsonpath={.items[0].metadata.name}").Trim()
    $driverLogs = (& kubectl logs $driverPod -n data-platform) -join [Environment]::NewLine
    if ($driverLogs -notmatch "SPARK_MINIO_CONNECTIVITY_STATUS=PASS") {
        throw "Driver logs do not prove MinIO connectivity."
    }
    if ($driverLogs -notmatch "SPARK_DELTA_WRITE_STATUS=PASS") {
        throw "Driver logs do not prove Delta write/read."
    }

    Write-Output "TEST_PATH=MINIKUBE_SPARK_INTEGRATION"
    Write-Output "SPARK_APPLICATION=$applicationName"
    Write-Output "SPARK_APPLICATION_STATUS=PASS"
    Write-Output "SPARK_DRIVER_POD_STATUS=PASS"
    Write-Output "SPARK_EXECUTOR_PODS_STATUS=PASS"
    Write-Output "SPARK_EXECUTION_MODE=DISTRIBUTED_KUBERNETES"
    Write-Output "SPARK_MINIO_CONNECTIVITY_STATUS=PASS"
    Write-Output "SPARK_DELTA_WRITE_STATUS=PASS"
    Write-Output "SPARK_INTEGRATION_STATUS=PASS"
}
catch {
    Write-Output "SPARK_INTEGRATION_STATUS=FAIL"
    $originalError = $_
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & kubectl describe sparkapplication $applicationName -n data-platform 2>$null
        & kubectl get pods -n data-platform -o wide 2>$null
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    throw $originalError
}
finally {
    Remove-Item -LiteralPath $temporaryFile.FullName -Force -ErrorAction SilentlyContinue
}
