[CmdletBinding()]
param(
    [string]$Profile = "data-master-repro-test"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")

Set-DataMasterMinikubeContext -Profile $Profile
$safeProfile = $Profile -replace "[^A-Za-z0-9_.-]", "_"
$statePath = Join-Path ([System.IO.Path]::GetTempPath()) "data-master-port-forwards-$safeProfile.json"
if (Test-Path $statePath) {
    throw "Port-forward state already exists. Run Stop-DataMasterPortForwards.ps1 first: $statePath"
}

$definitions = @(
    @{ Name = "argocd"; Namespace = "argocd"; Service = "argocd-server"; Ports = "8080:443"; LocalPorts = @(8080) },
    @{ Name = "airflow"; Namespace = "data-platform"; Service = "airflow"; Ports = "8082:8080"; LocalPorts = @(8082) },
    @{ Name = "minio"; Namespace = "data-platform"; Service = "minio"; Ports = "9000:9000,9001:9001"; LocalPorts = @(9000, 9001) },
    @{ Name = "jupyter"; Namespace = "data-platform"; Service = "jupyter"; Ports = "8888:8888"; LocalPorts = @(8888) }
)

$processes = @()
try {
    foreach ($definition in $definitions) {
        Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
            "get", "service", $definition.Service, "-n", $definition.Namespace
        ) | Out-Null
        foreach ($port in $definition.LocalPorts) {
            if (Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue) {
                throw "Local port is already in use: $port"
            }
        }

        $portArguments = $definition.Ports.Split(",")
        $arguments = @(
            "--context", $Profile,
            "port-forward", "service/$($definition.Service)",
            "-n", $definition.Namespace
        ) + $portArguments
        $process = Start-Process -FilePath "kubectl" -ArgumentList $arguments `
            -PassThru -WindowStyle Hidden
        $processes += [pscustomobject]@{
            name = $definition.Name
            pid = $process.Id
            ports = $definition.Ports
        }
    }

    Start-Sleep -Seconds 3
    foreach ($item in $processes) {
        if (-not (Get-Process -Id $item.pid -ErrorAction SilentlyContinue)) {
            throw "Port-forward process stopped unexpectedly: $($item.name)"
        }
    }
    $processes | ConvertTo-Json | Set-Content -LiteralPath $statePath -Encoding UTF8
}
catch {
    foreach ($item in $processes) {
        Stop-Process -Id $item.pid -Force -ErrorAction SilentlyContinue
    }
    throw
}

Write-Output "PORT_FORWARD_STATE=$statePath"
Write-Output "ARGOCD_URL=https://localhost:8080"
Write-Output "AIRFLOW_URL=http://localhost:8082"
Write-Output "MINIO_API_URL=http://localhost:9000"
Write-Output "MINIO_CONSOLE_URL=http://localhost:9001"
Write-Output "JUPYTER_URL=http://localhost:8888"
Write-Output "PORT_FORWARDS_STATUS=PASS"
