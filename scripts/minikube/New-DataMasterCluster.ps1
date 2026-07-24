[CmdletBinding()]
param(
    [string]$Profile = "data-master-repro-test",

    [ValidateRange(2, 64)]
    [int]$Cpus = 4,

    [ValidateRange(4096, 262144)]
    [int]$Memory = 11264,

    [ValidatePattern("^[0-9]+(g|G|mb|MB)$")]
    [string]$DiskSize = "30g",

    [ValidateSet("docker", "hyperv", "virtualbox")]
    [string]$Driver = "docker"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")

Assert-DataMasterSafeProfile -Profile $Profile
$status = & minikube status -p $Profile --output=json 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Output "MINIKUBE_PROFILE_ACTION=REUSE_OR_START"
}
else {
    Write-Output "MINIKUBE_PROFILE_ACTION=CREATE"
}

Invoke-DataMasterNative -FilePath "minikube" -Arguments @(
    "start",
    "-p", $Profile,
    "--cpus=$Cpus",
    "--memory=$Memory",
    "--disk-size=$DiskSize",
    "--driver=$Driver"
)
Set-DataMasterMinikubeContext -Profile $Profile

$activeContext = (& kubectl config current-context).Trim()
if ($activeContext -ne $Profile) {
    throw "Unexpected kubectl context: $activeContext"
}
Write-Output "MINIKUBE_PROFILE=$Profile"
Write-Output "MINIKUBE_CLUSTER_STATUS=PASS"
