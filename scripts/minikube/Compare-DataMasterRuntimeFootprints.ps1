# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$BaselinePath,

    [Parameter(Mandatory = $true)]
    [string]$CandidatePath,

    [Parameter(Mandatory = $true)]
    [string]$OutputPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

& python (Join-Path $PSScriptRoot "runtime_footprint.py") compare `
    --baseline $BaselinePath `
    --candidate $CandidatePath `
    --output $OutputPath
if ($LASTEXITCODE -ne 0) { throw "Runtime footprint comparison failed." }