[CmdletBinding()]
param(
    [string]$Profile = "data-master-repro-test",

    [string]$Tag,

    [string]$AirflowRepository = "data-master-airflow",

    [string]$SparkRepository = "data-master-spark-jobs",

    [switch]$PreloadRuntimeDependencies
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")

function Import-DataMasterDockerImageStream {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Image,

        [Parameter(Mandatory = $true)]
        [string]$Profile
    )

    $saveInfo = New-Object System.Diagnostics.ProcessStartInfo
    $saveInfo.FileName = "docker.exe"
    $saveInfo.Arguments = "image save $Image"
    $saveInfo.UseShellExecute = $false
    $saveInfo.RedirectStandardOutput = $true
    $saveInfo.CreateNoWindow = $true

    $loadInfo = New-Object System.Diagnostics.ProcessStartInfo
    $loadInfo.FileName = "docker.exe"
    $loadInfo.Arguments = (
        "exec -i $Profile docker image load"
    )
    $loadInfo.UseShellExecute = $false
    $loadInfo.RedirectStandardInput = $true
    $loadInfo.CreateNoWindow = $true

    $saveProcess = New-Object System.Diagnostics.Process
    $loadProcess = New-Object System.Diagnostics.Process
    $saveProcess.StartInfo = $saveInfo
    $loadProcess.StartInfo = $loadInfo
    try {
        if (-not $loadProcess.Start()) {
            throw "Unable to start Docker runtime image load for: $Image"
        }
        if (-not $saveProcess.Start()) {
            throw "Unable to start Docker save for: $Image"
        }
        $saveProcess.StandardOutput.BaseStream.CopyTo(
            $loadProcess.StandardInput.BaseStream
        )
        $loadProcess.StandardInput.Close()
        $saveProcess.WaitForExit()
        $loadProcess.WaitForExit()
        if ($saveProcess.ExitCode -ne 0 -or $loadProcess.ExitCode -ne 0) {
            throw (
                "Streaming image import failed for '$Image': " +
                "save=$($saveProcess.ExitCode), load=$($loadProcess.ExitCode)"
            )
        }
    }
    finally {
        $saveProcess.Dispose()
        $loadProcess.Dispose()
    }
}

Assert-DataMasterSafeProfile -Profile $Profile
if (-not $Tag) {
    $Tag = Get-DataMasterImageTag
}
$projectImages = @("${AirflowRepository}:$Tag", "${SparkRepository}:$Tag")
$runtimeDependencyImages = @(
    "postgres:15",
    "bde2020/hive:2.3.2-postgresql-metastore",
    "minio/minio:RELEASE.2024-01-28T22-35-53Z",
    "minio/mc:RELEASE.2024-01-13T08-44-48Z",
    "quay.io/jupyter/pyspark-notebook:2024-04-01",
    "ghcr.io/kubeflow/spark-operator/controller:2.5.0"
)

$images = @($projectImages)
if ($PreloadRuntimeDependencies) {
    foreach ($image in $runtimeDependencyImages) {
        $existingImageIds = @(& docker images --quiet $image)
        if ($LASTEXITCODE -ne 0) {
            throw "Unable to inspect local Docker images for: $image"
        }
        if ($existingImageIds.Count -eq 0) {
            Invoke-DataMasterNative -FilePath "docker" -Arguments @("pull", $image)
        }
    }
    $images += $runtimeDependencyImages
}

foreach ($image in $images) {
    & docker image inspect $image | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Local image does not exist: $image"
    }
    if ($PreloadRuntimeDependencies) {
        Import-DataMasterDockerImageStream -Image $image -Profile $Profile
    }
    else {
        Invoke-DataMasterNative -FilePath "minikube" -Arguments @(
            "image", "load", $image, "-p", $Profile
        )
    }
}

foreach ($image in $images) {
    $hostImageId = @(
        Invoke-DataMasterNative -FilePath "docker" -CaptureOutput -Arguments @(
            "image", "inspect", $image, "--format", "{{.Id}}"
        )
    )
    $nodeImageId = @(
        Invoke-DataMasterNative -FilePath "docker" -CaptureOutput -Arguments @(
            "exec", $Profile, "docker", "image", "inspect", $image,
            "--format", "{{.Id}}"
        )
    )
    if ($hostImageId.Count -ne 1 -or $nodeImageId.Count -ne 1) {
        throw "Image inspection returned an unexpected result for: $image"
    }
    if ($nodeImageId[0].Trim() -ne $hostImageId[0].Trim()) {
        throw "Image ID mismatch in Minikube profile '$Profile': $image"
    }
    Write-Output "MINIKUBE_IMAGE_${image}=PASS"
}
if ($PreloadRuntimeDependencies) {
    Write-Output "MINIKUBE_RUNTIME_DEPENDENCY_IMPORT_STATUS=PASS"
}
Write-Output "MINIKUBE_IMAGE_IMPORT_STATUS=PASS"
