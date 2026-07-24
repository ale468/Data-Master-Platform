[CmdletBinding()]
param(
    [string]$Profile = "data-master-repro-test"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")

Assert-DataMasterSafeProfile -Profile $Profile
$safeProfile = $Profile -replace "[^A-Za-z0-9_.-]", "_"
$statePath = Join-Path ([System.IO.Path]::GetTempPath()) "data-master-port-forwards-$safeProfile.json"
if (-not (Test-Path $statePath)) {
    Write-Output "PORT_FORWARDS_STATUS=ALREADY_STOPPED"
    return
}

$entries = @(Get-Content -Raw -LiteralPath $statePath | ConvertFrom-Json)
foreach ($entry in $entries) {
    $process = Get-Process -Id $entry.pid -ErrorAction SilentlyContinue
    if ($process -and ($process.ProcessName -eq "kubectl")) {
        Stop-Process -Id $entry.pid -Force
    }
}
Remove-Item -LiteralPath $statePath -Force
Write-Output "PORT_FORWARDS_STATUS=STOPPED"
