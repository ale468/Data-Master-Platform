# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Profile,

    [Parameter(Mandatory = $true)]
    [ValidateSet("baseline", "candidate")]
    [string]$Variant,

    [Parameter(Mandatory = $true)]
    [ValidateSet("ready-idle", "e2e-active", "post-e2e")]
    [string]$Phase,

    [Parameter(Mandatory = $true)]
    [string]$OutputPath,

    [ValidateRange(2, 120)]
    [int]$SampleCount = 6,

    [ValidateRange(1, 300)]
    [int]$SampleIntervalSeconds = 10,

    [string]$DeployedRevision,

    [ValidateRange(0, 86400)]
    [double]$BootstrapDurationSeconds,

    [ValidateRange(0, 86400)]
    [double]$E2EDurationSeconds
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")

Assert-DataMasterSafeProfile -Profile $Profile
$root = Get-DataMasterRepositoryRoot
if (-not $DeployedRevision) {
    $DeployedRevision = (& git -C $root rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0) { throw "Unable to resolve the deployed Git revision." }
}
$arguments = @(
    (Join-Path $PSScriptRoot "runtime_footprint.py"),
    "capture",
    "--profile", $Profile,
    "--variant", $Variant,
    "--phase", $Phase,
    "--output", $OutputPath,
    "--samples", [string]$SampleCount,
    "--interval-seconds", [string]$SampleIntervalSeconds,
    "--deployed-revision", $DeployedRevision
)
if ($PSBoundParameters.ContainsKey("BootstrapDurationSeconds")) {
    $arguments += @("--bootstrap-duration-seconds", [string]$BootstrapDurationSeconds)
}
if ($PSBoundParameters.ContainsKey("E2EDurationSeconds")) {
    $arguments += @("--e2e-duration-seconds", [string]$E2EDurationSeconds)
}
& python @arguments
if ($LASTEXITCODE -ne 0) { throw "Runtime footprint capture failed." }