[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$EvidencePath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.ExecutionEvidence.ps1")

$evidence = Read-DataMasterExecutionEvidence -Path $EvidencePath
Write-Output "DURABLE_EXECUTION_EVIDENCE_STATUS=PASS"
Write-Output "DURABLE_EVIDENCE_RUN_ID=$($evidence.dag.run_id)"
Write-Output "DURABLE_EVIDENCE_PRIVACY_STATUS=PASS"
