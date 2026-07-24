[CmdletBinding()]
param(
    [ValidateRange(2, 64)]
    [int]$MinimumCpuCount = 4,

    [ValidateRange(4, 256)]
    [int]$MinimumMemoryGiB = 11
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")

$requiredCommands = @("git", "docker", "minikube", "kubectl", "helm")
foreach ($command in $requiredCommands) {
    if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
        throw "Required command not found: $command"
    }
    Write-Output "PREREQUISITE_$($command.ToUpperInvariant())=PASS"
}

if ($PSVersionTable.PSVersion.Major -lt 5) {
    throw "Windows PowerShell 5.1 or PowerShell 7+ is required."
}

$root = Get-DataMasterRepositoryRoot
Invoke-DataMasterNative -FilePath "git" -Arguments @(
    "-C", $root, "rev-parse", "--is-inside-work-tree"
) | Out-Null
Invoke-DataMasterNative -FilePath "docker" -Arguments @("info") | Out-Null
$dockerMemoryOutput = Invoke-DataMasterNative -FilePath "docker" -CaptureOutput -Arguments @(
    "info", "--format", "{{.MemTotal}}"
)
$dockerMemoryBytes = [int64](($dockerMemoryOutput | Select-Object -Last 1).ToString().Trim())
$dockerMemoryGiB = [math]::Floor($dockerMemoryBytes / 1GB)
Invoke-DataMasterNative -FilePath "minikube" -Arguments @("version") | Out-Null
Invoke-DataMasterNative -FilePath "kubectl" -Arguments @("version", "--client") | Out-Null
Invoke-DataMasterNative -FilePath "helm" -Arguments @("version", "--short") | Out-Null

$cpuCount = [Environment]::ProcessorCount
$memoryGiB = 0
if (Get-Command Get-CimInstance -ErrorAction SilentlyContinue) {
    $memoryBytes = (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
    $memoryGiB = [math]::Floor($memoryBytes / 1GB)
}
if ($cpuCount -lt $MinimumCpuCount) {
    throw "Insufficient CPU: available=$cpuCount required=$MinimumCpuCount"
}
if (($memoryGiB -gt 0) -and ($memoryGiB -lt $MinimumMemoryGiB)) {
    throw "Insufficient memory: available=${memoryGiB}GiB required=${MinimumMemoryGiB}GiB"
}
if ($dockerMemoryGiB -lt $MinimumMemoryGiB) {
    throw "Insufficient Docker memory: available=${dockerMemoryGiB}GiB required=${MinimumMemoryGiB}GiB"
}

$ports = @(8080, 8082, 8888, 9000, 9001)
foreach ($port in $ports) {
    $listener = Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue
    if ($listener) {
        Write-Output "LOCAL_PORT_${port}=IN_USE"
    }
    else {
        Write-Output "LOCAL_PORT_${port}=AVAILABLE"
    }
}

Write-Output "AVAILABLE_CPU_COUNT=$cpuCount"
Write-Output "AVAILABLE_DOCKER_MEMORY_GIB=$dockerMemoryGiB"
if ($memoryGiB -gt 0) {
    Write-Output "AVAILABLE_MEMORY_GIB=$memoryGiB"
}
Write-Output "POWERSHELL_VERSION=$($PSVersionTable.PSVersion)"
Write-Output "PREREQUISITES_STATUS=PASS"
