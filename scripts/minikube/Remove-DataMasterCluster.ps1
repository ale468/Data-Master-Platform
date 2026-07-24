[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Profile,

    [switch]$ConfirmDeletion
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")

Assert-DataMasterSafeProfile -Profile $Profile
if (-not $ConfirmDeletion) {
    throw "Cluster deletion requires -ConfirmDeletion and an explicit non-protected -Profile."
}
Invoke-DataMasterNative -FilePath "minikube" -Arguments @("delete", "-p", $Profile)
Write-Output "REMOVED_MINIKUBE_PROFILE=$Profile"
Write-Output "MINIKUBE_CLUSTER_REMOVAL_STATUS=PASS"
