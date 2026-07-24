[CmdletBinding()]
param(
    [string]$Profile = "data-master-repro-test",

    [string]$ChartVersion = "7.3.11",

    [ValidateRange(120, 1800)]
    [int]$TimeoutSeconds = 600
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")

Set-DataMasterMinikubeContext -Profile $Profile
Invoke-DataMasterNative -FilePath "helm" -Arguments @(
    "repo", "add", "argo", "https://argoproj.github.io/argo-helm",
    "--force-update"
)
Invoke-DataMasterNative -FilePath "helm" -Arguments @("repo", "update", "argo")
Invoke-DataMasterNative -FilePath "helm" -Arguments @(
    "upgrade", "--install", "argocd", "argo/argo-cd",
    "--version", $ChartVersion,
    "--namespace", "argocd",
    "--create-namespace",
    "--wait",
    "--timeout", "${TimeoutSeconds}s"
)

Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
    "wait", "--for=condition=Established",
    "crd/applications.argoproj.io",
    "--timeout=${TimeoutSeconds}s"
)
Write-Output "ARGOCD_CHART_VERSION=$ChartVersion"
Write-Output "ARGOCD_INSTALL_STATUS=PASS"
