# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

[CmdletBinding()]
param(
    [string]$Profile = "data-master-multibatch",

    [string]$ScenarioId,

    [string]$EvidencePath,

    [ValidateRange(1200, 43200)]
    [int]$TimeoutSecondsPerRun = 7200
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")
. (Join-Path $PSScriptRoot "DataMaster.ExecutionEvidence.ps1")

if (-not $ScenarioId) {
    $ScenarioId = "multibatch-" + (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmss")
}
if ($ScenarioId -notmatch '^[A-Za-z0-9._-]+$') {
    throw "ScenarioId must use only letters, digits, dot, underscore, or dash."
}

Set-DataMasterMinikubeContext -Profile $Profile
$root = Get-DataMasterRepositoryRoot
if (-not $EvidencePath) {
    $EvidencePath = Join-Path $root "evidence\runtime\$ScenarioId.json"
}
elseif (-not [System.IO.Path]::IsPathRooted($EvidencePath)) {
    $EvidencePath = Join-Path $root $EvidencePath
}
$EvidencePath = [System.IO.Path]::GetFullPath($EvidencePath)

$steps = @(
    [ordered]@{ Name = "batch_1"; SourceBatch = "batch-1"; BatchId = "$ScenarioId-b1"; RunId = "$ScenarioId-b1" },
    [ordered]@{ Name = "batch_2"; SourceBatch = "batch-2"; BatchId = "$ScenarioId-b2"; RunId = "$ScenarioId-b2" },
    [ordered]@{ Name = "batch_2_replay"; SourceBatch = "batch-2"; BatchId = "$ScenarioId-b2"; RunId = "$ScenarioId-b2-replay" },
    [ordered]@{ Name = "batch_3"; SourceBatch = "batch-3"; BatchId = "$ScenarioId-b3"; RunId = "$ScenarioId-b3" }
)

$runEvidence = [ordered]@{}
foreach ($step in $steps) {
    $config = [ordered]@{
        scenario_id = $ScenarioId
        batch_id = $step.BatchId
        source_batch = $step.SourceBatch
    }
    $configJson = $config | ConvertTo-Json -Compress
    $stepEvidencePath = "$EvidencePath.$($step.Name).json"
    & (Join-Path $PSScriptRoot "Invoke-AirflowEndToEndTest.ps1") `
        -Profile $Profile `
        -RunId $step.RunId `
        -EvidencePath $stepEvidencePath `
        -DagRunConfigJson $configJson `
        -TimeoutSeconds $TimeoutSecondsPerRun
    $runEvidence[$step.Name] = Get-Content -LiteralPath $stepEvidencePath -Raw |
        ConvertFrom-Json
}

foreach ($step in $steps) {
    $evidence = $runEvidence[$step.Name]
    if (-not ($evidence.PSObject.Properties.Name -contains "multibatch")) {
        throw "Missing multibatch evidence for $($step.Name)."
    }
    if ($evidence.multibatch.scenario_id -ne $ScenarioId -or
        $evidence.multibatch.batch_id -ne $step.BatchId -or
        $evidence.multibatch.run_id -ne $step.RunId -or
        $evidence.multibatch.source_batch -ne $step.SourceBatch) {
        throw "Multibatch identifiers do not match for $($step.Name)."
    }
}

$b1 = $runEvidence.batch_1
$b2 = $runEvidence.batch_2
$replay = $runEvidence.batch_2_replay
$b3 = $runEvidence.batch_3

$replayLayers = @("bronze", "raw_vault_hubs", "raw_vault_links", "raw_vault_satellites")
$replayDeltas = [ordered]@{}
foreach ($layer in $replayLayers) {
    $delta = [long]$replay.aggregate_counts.$layer - [long]$b2.aggregate_counts.$layer
    $replayDeltas[$layer] = $delta
    if ($delta -ne 0) {
        throw "Replay changed $layer by $delta rows."
    }
}

if ([long]$b1.multibatch.history.changed_customer_versions -ne 1 -or
    [long]$b2.multibatch.history.changed_customer_versions -ne 2 -or
    [long]$replay.multibatch.history.changed_customer_versions -ne 2 -or
    [long]$b3.multibatch.history.changed_customer_versions -ne 2) {
    throw "Changed customer Satellite history is inconsistent."
}
foreach ($evidence in @($b1, $b2, $replay, $b3)) {
    if ([long]$evidence.multibatch.history.unchanged_customer_versions -ne 1) {
        throw "Unchanged customer gained a spurious Satellite version."
    }
}
if (-not $b2.multibatch.history.new_customer_present -or
    -not $b2.multibatch.history.new_customer_account_present -or
    [long]$b2.multibatch.history.new_customer_relationship_rows -ne 1 -or
    [long]$b2.multibatch.history.existing_customer_new_relationship_rows -ne 1) {
    throw "Batch 2 entity or relationship evidence is incomplete."
}
if (-not $b3.multibatch.late_arrival.present) {
    throw "Batch 3 late-arriving transaction is missing."
}

$lateEvent = [DateTimeOffset]::Parse([string]$b3.multibatch.late_arrival.event_timestamp)
$lateLoad = [DateTimeOffset]::Parse([string]$b3.multibatch.late_arrival.load_timestamp)
$batch2Generated = [DateTimeOffset]::Parse(
    [string]$b2.multibatch.source_manifest.generated_at
)
if (-not ($lateEvent -lt $batch2Generated -and $batch2Generated -lt $lateLoad)) {
    throw "Late-arrival event time and load time are not ordered as expected."
}

$summary = [ordered]@{
    schema_version = 1
    evidence_kind = "data_master_deterministic_multibatch_validation"
    scenario_id = $ScenarioId
    profile = $Profile
    captured_at = [DateTimeOffset]::UtcNow.ToString("o")
    status = "PASS"
    sequence = @("batch-1", "batch-2", "batch-2-replay", "batch-3")
    commits = $b3.commits
    images = $b3.images
    runs = @(
        foreach ($step in $steps) {
            $item = $runEvidence[$step.Name]
            [ordered]@{
                name = $step.Name
                source_batch = $step.SourceBatch
                batch_id = $step.BatchId
                run_id = $step.RunId
                manifest_sha256 = [string]$item.multibatch.source_manifest.manifest_sha256
                generated_at = [string]$item.multibatch.source_manifest.generated_at
                aggregate_counts = $item.aggregate_counts
                batch_counts = $item.multibatch.batch_counts
                history = $item.multibatch.history
                late_arrival = $item.multibatch.late_arrival
            }
        }
    )
    assertions = [ordered]@{
        deterministic_replay_manifest = (
            [string]$b2.multibatch.source_manifest.manifest_sha256 -eq
            [string]$replay.multibatch.source_manifest.manifest_sha256
        )
        replay_layer_deltas = $replayDeltas
        changed_customer_versions_after_batch_2 = 2
        unchanged_customer_versions = 1
        new_entity_and_relationships = "PASS"
        late_arrival_event_before_batch_2_generation = $true
        late_arrival_loaded_after_batch_2_generation = $true
        gold_rebuilt_after_each_run = $true
    }
    privacy = [ordered]@{
        classification = "technical_aggregate_only"
        contains_pii = $false
        contains_secrets = $false
        contains_business_payload = $false
    }
    limitations = @(
        "synthetic-local-only",
        "not-streaming",
        "not-production-sla",
        "not-high-volume-benchmark",
        "late-arrival-is-an-immutable-new-transaction"
    )
}

if (-not $summary.assertions.deterministic_replay_manifest) {
    throw "Batch 2 replay manifest is not deterministic."
}
Write-DataMasterExecutionEvidence -Evidence $summary -Path $EvidencePath
Write-Output "MULTIBATCH_SCENARIO_ID=$ScenarioId"
Write-Output "MULTIBATCH_EVIDENCE_PATH=$EvidencePath"
Write-Output "MULTIBATCH_VALIDATION_STATUS=PASS"
