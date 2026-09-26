Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\.."))
. (Join-Path $root "scripts\minikube\DataMaster.ExecutionEvidence.ps1")

function Copy-TestEvidence {
    param([Parameter(Mandatory = $true)][object]$Value)
    $json = $Value | ConvertTo-Json -Depth 20
    return ConvertFrom-DataMasterJson -Json $json
}

function Assert-TestThrows {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Action,
        [Parameter(Mandatory = $true)][string]$ExpectedPattern
    )
    try {
        & $Action
    }
    catch {
        if ($_.Exception.Message -notmatch $ExpectedPattern) {
            throw "Unexpected validation error: $($_.Exception.Message)"
        }
        return
    }
    throw "Expected validation failure matching '$ExpectedPattern'."
}

$stages = @(
    "bronze", "hubs", "links", "satellites", "gold",
    "data-vault-gate", "masking-gate", "evidence"
)
$applications = @()
$stageStatuses = @()
foreach ($stage in $stages) {
    $name = "run-$stage-12345678"
    $applications += [ordered]@{
        name = $name
        stage = $stage
        image = "data-master-spark-jobs:git-1234567"
        status = "SUCCESS"
        task_started_at = "2026-07-14T01:00:00+00:00"
        task_finished_at = "2026-07-14T01:05:00+00:00"
        task_exit_code = 0
        evidence_source = "synthetic_test_fixture"
    }
    $stageStatuses += [ordered]@{
        stage = $stage
        task_id = "run_" + $stage.Replace("-", "_")
        application_name = $name
        status = "SUCCESS"
        marker = "SPARK_STAGE_RESULT.status=SUCCESS"
    }
}
$valid = [ordered]@{
    schema_version = 1
    evidence_kind = "data_master_minikube_airflow_e2e"
    captured_at = "2026-07-14T02:00:00+00:00"
    source = [ordered]@{
        mode = "synthetic_contract_test"
        records = @("airflow_task_metadata", "airflow_task_logs")
    }
    dag = [ordered]@{
        dag_id = "banking_data_vault_pipeline"
        run_id = "minikube-e2e-test"
        state = "SUCCESS"
        started_at = "2026-07-14T01:00:00+00:00"
        finished_at = "2026-07-14T02:00:00+00:00"
    }
    commits = @([ordered]@{ sha = "1234567"; purpose = "test fixture" })
    images = @(
        [ordered]@{
            role = "airflow"
            reference = "data-master-airflow:git-1234567"
            image_id = "sha256:" + ("a" * 64)
        },
        [ordered]@{
            role = "spark_jobs"
            reference = "data-master-spark-jobs:git-1234567"
            image_id = "sha256:" + ("b" * 64)
        }
    )
    spark_applications = $applications
    stages = $stageStatuses
    quality_gates = [ordered]@{
        data_vault = [ordered]@{
            status = "PASS"
            markers = @(
                "DATA_VAULT_LINEAGE_STATUS=PASS",
                "DATA_VAULT_GOLD_LINEAGE_STATUS=PASS",
                "DATA_VAULT_QUALITY_GATE_STATUS=PASS"
            )
            validated_at = "2026-07-14T01:45:00+00:00"
            evidence_source = "synthetic_test_fixture"
        }
        masking = [ordered]@{
            status = "PASS"
            markers = @(
                "MASKING_STATUS=PASS", "GOLD_PII_EXPOSURE_STATUS=PASS"
            )
            validated_at = "2026-07-14T01:50:00+00:00"
            evidence_source = "synthetic_test_fixture"
        }
        reproducibility = [ordered]@{
            status = "PENDING_VALIDATION"
            markers = @("REPRODUCIBILITY_GATE_STATUS=PENDING_VALIDATION")
            validated_at = $null
            evidence_source = "synthetic_test_fixture"
        }
    }
    technical_lineage = [ordered]@{
        path = "bronze->raw_vault->business_vault_latest->gold"
        status = "PASS"
    }
    aggregate_counts = [ordered]@{
        bronze = 1
        raw_vault_hubs = 1
        raw_vault_links = 1
        raw_vault_satellites = 1
        gold = 1
    }
    privacy = [ordered]@{
        classification = "technical_aggregate_only"
        contains_pii = $false
        contains_secrets = $false
        contains_business_payload = $false
    }
    operational_risks = @()
}

Assert-DataMasterSensitiveContent -Evidence $valid

$orderedSecretField = [ordered]@{
    privacy = [ordered]@{
        token = "not-a-real-token"
    }
}
Assert-TestThrows -ExpectedPattern "forbidden field" -Action {
    Assert-DataMasterSensitiveContent -Evidence $orderedSecretField
}

Assert-DataMasterExecutionEvidence -Evidence $valid | Out-Null

$withStorage = Copy-TestEvidence -Value $valid
$withStorage | Add-Member -NotePropertyName storage -NotePropertyValue ([ordered]@{
    business_vault_path = "s3a://lakehouse/business_vault"
    gold_path = "s3a://lakehouse/gold"
    gold_tables = [ordered]@{
        gold_transacoes_por_dia = "s3a://lakehouse/gold/gold_transacoes_por_dia"
        gold_transacoes_por_cliente = "s3a://lakehouse/gold/gold_transacoes_por_cliente"
        gold_volume_por_produto = "s3a://lakehouse/gold/gold_volume_por_produto"
        gold_eventos_digitais_por_canal = "s3a://lakehouse/gold/gold_eventos_digitais_por_canal"
        gold_contas_por_agencia = "s3a://lakehouse/gold/gold_contas_por_agencia"
        gold_risco_transacional_simplificado = "s3a://lakehouse/gold/gold_risco_transacional_simplificado"
        gold_clientes_protegidos = "s3a://lakehouse/gold/gold_clientes_protegidos"
    }
})
Assert-DataMasterExecutionEvidence -Evidence $withStorage | Out-Null

$withMultibatch = Copy-TestEvidence -Value $withStorage
$withMultibatch | Add-Member -NotePropertyName multibatch -NotePropertyValue ([ordered]@{
    schema_version = 1
    scenario_id = "test-scenario"
    source_batch = "batch-1"
    batch_id = "test-scenario-b1"
    run_id = "minikube-e2e-test"
    batch_counts = [ordered]@{
        bronze = 399
        raw_vault_hubs = 240
        raw_vault_links = 490
        raw_vault_satellites = 399
    }
    source_manifest = [ordered]@{
        generator_version = "deterministic-multibatch-v1"
        generated_at = "2026-07-14T01:00:00+00:00"
        manifest_sha256 = "a" * 64
        source_counts = [ordered]@{
            clientes = 10
            contas = 10
            transacoes = 100
            cartoes = 10
            eventos_digitais = 100
            agencias = 2
            produtos = 3
        }
    }
    history = [ordered]@{
        changed_customer_versions = 1
        unchanged_customer_versions = 1
        new_customer_present = $false
        new_customer_account_present = $false
        new_customer_relationship_rows = 0
        existing_customer_new_relationship_rows = 0
    }
    late_arrival = [ordered]@{
        transaction_id = "TRX_LATE_000001"
        present = $false
    }
    gold_rows = 7
})
Assert-DataMasterExecutionEvidence -Evidence $withMultibatch | Out-Null

$mismatchedMultibatchRun = Copy-TestEvidence -Value $withMultibatch
$mismatchedMultibatchRun.multibatch.run_id = "different-run"
Assert-TestThrows -ExpectedPattern "run_id does not match" -Action {
    Assert-DataMasterExecutionEvidence -Evidence $mismatchedMultibatchRun | Out-Null
}

$invalidManifestHash = Copy-TestEvidence -Value $withMultibatch
$invalidManifestHash.multibatch.source_manifest.manifest_sha256 = "not-a-sha"
Assert-TestThrows -ExpectedPattern "manifest_sha256" -Action {
    Assert-DataMasterExecutionEvidence -Evidence $invalidManifestHash | Out-Null
}

$summaryRuns = @()
$runDefinitions = @(
    [ordered]@{ name = "batch_1"; source = "batch-1"; batch = "scenario-b1"; run = "scenario-b1"; generated = "2026-01-01T00:00:00+00:00"; manifest = "a" * 64; changed = 1; aggregate = 100 },
    [ordered]@{ name = "batch_2"; source = "batch-2"; batch = "scenario-b2"; run = "scenario-b2"; generated = "2026-01-03T00:00:00+00:00"; manifest = "b" * 64; changed = 2; aggregate = 120 },
    [ordered]@{ name = "batch_2_replay"; source = "batch-2"; batch = "scenario-b2"; run = "scenario-b2-replay"; generated = "2026-01-03T00:00:00+00:00"; manifest = "b" * 64; changed = 2; aggregate = 120 },
    [ordered]@{ name = "batch_3"; source = "batch-3"; batch = "scenario-b3"; run = "scenario-b3"; generated = "2026-01-04T00:00:00+00:00"; manifest = "c" * 64; changed = 2; aggregate = 130 }
)
foreach ($definition in $runDefinitions) {
    $lateArrival = if ($definition.name -eq "batch_3") {
        [ordered]@{
            transaction_id = "TXN_MB_LATE_000000001"
            present = $true
            event_timestamp = "2026-01-02T00:00:00+00:00"
            load_timestamp = "2026-01-05T00:00:00+00:00"
            batch_id = $definition.batch
            run_id = $definition.run
        }
    }
    else {
        [ordered]@{
            transaction_id = "TXN_MB_LATE_000000001"
            present = $false
        }
    }
    $summaryRuns += [ordered]@{
        name = $definition.name
        source_batch = $definition.source
        batch_id = $definition.batch
        run_id = $definition.run
        manifest_sha256 = $definition.manifest
        generated_at = $definition.generated
        aggregate_counts = [ordered]@{
            bronze = $definition.aggregate
            raw_vault_hubs = $definition.aggregate
            raw_vault_links = $definition.aggregate
            raw_vault_satellites = $definition.aggregate
            gold = $definition.aggregate
        }
        batch_counts = [ordered]@{
            bronze = 10
            raw_vault_hubs = 5
            raw_vault_links = 5
            raw_vault_satellites = 10
        }
        history = [ordered]@{
            changed_customer_versions = $definition.changed
            unchanged_customer_versions = 1
            new_customer_present = ($definition.name -ne "batch_1")
            new_customer_account_present = ($definition.name -ne "batch_1")
            new_customer_relationship_rows = if ($definition.name -eq "batch_1") { 0 } else { 1 }
            existing_customer_new_relationship_rows = if ($definition.name -eq "batch_1") { 0 } else { 1 }
        }
        late_arrival = $lateArrival
    }
}
$validSummary = [ordered]@{
    schema_version = 1
    evidence_kind = "data_master_deterministic_multibatch_validation"
    scenario_id = "scenario"
    profile = "data-master-multibatch-test"
    captured_at = "2026-01-05T01:00:00+00:00"
    status = "PASS"
    sequence = @("batch-1", "batch-2", "batch-2-replay", "batch-3")
    commits = $valid.commits
    images = $valid.images
    runs = $summaryRuns
    assertions = [ordered]@{
        deterministic_replay_manifest = $true
        replay_layer_deltas = [ordered]@{
            bronze = 0
            raw_vault_hubs = 0
            raw_vault_links = 0
            raw_vault_satellites = 0
        }
        changed_customer_versions_after_batch_2 = 2
        unchanged_customer_versions = 1
        new_entity_and_relationships = "PASS"
        late_arrival_event_before_batch_2_generation = $true
        late_arrival_loaded_after_batch_2_generation = $true
        gold_rebuilt_after_each_run = $true
    }
    privacy = $valid.privacy
    limitations = @("synthetic-local-only", "not-streaming")
}
Assert-DataMasterExecutionEvidence -Evidence $validSummary | Out-Null

$invalidReplaySummary = Copy-TestEvidence -Value $validSummary
$invalidReplaySummary.runs[2].aggregate_counts.bronze = 121
Assert-TestThrows -ExpectedPattern "replay changed.*bronze" -Action {
    Assert-DataMasterExecutionEvidence -Evidence $invalidReplaySummary | Out-Null
}

$sameStorageRoot = Copy-TestEvidence -Value $withStorage
$sameStorageRoot.storage.business_vault_path = "s3a://lakehouse/gold"
Assert-TestThrows -ExpectedPattern "paths must be distinct" -Action {
    Assert-DataMasterExecutionEvidence -Evidence $sameStorageRoot | Out-Null
}

$invalidGoldTablePath = Copy-TestEvidence -Value $withStorage
$invalidGoldTablePath.storage.gold_tables.gold_clientes_protegidos =
    "s3a://lakehouse/business_vault/gold_clientes_protegidos"
Assert-TestThrows -ExpectedPattern "path is invalid.*gold_clientes_protegidos" -Action {
    Assert-DataMasterExecutionEvidence -Evidence $invalidGoldTablePath | Out-Null
}

$roundTripPath = Join-Path ([System.IO.Path]::GetTempPath()) (
    "data-master-evidence-test-" + [guid]::NewGuid().ToString("N") + ".json"
)
try {
    Write-DataMasterExecutionEvidence -Evidence $valid -Path $roundTripPath
    $roundTrip = Read-DataMasterExecutionEvidence -Path $roundTripPath
    if ($roundTrip.dag.run_id -ne "minikube-e2e-test") {
        throw "Durable evidence round-trip changed the DAG run id."
    }
}
finally {
    Remove-Item -LiteralPath $roundTripPath -Force -ErrorAction SilentlyContinue
}

$validObject = Copy-TestEvidence -Value $valid

$checkpointPath = Join-Path ([System.IO.Path]::GetTempPath()) (
    "data-master-spark-observation-" + [guid]::NewGuid().ToString("N") + ".json"
)
try {
    foreach ($stage in $stages) {
        Save-DataMasterSparkApplicationObservation -Path $checkpointPath `
            -DagId "banking_data_vault_pipeline" -RunId "minikube-e2e-test" `
            -Stage $stage -Name "run-$stage-12345678" `
            -Image "data-master-spark-jobs:git-1234567" `
            -CreationTimestamp "2026-07-14T01:00:00+00:00" | Out-Null
    }
    $checkpoint = Read-DataMasterSparkApplicationObservationCheckpoint `
        -Path $checkpointPath -RequireComplete
    if (@($checkpoint.observations).Count -ne $stages.Count) {
        throw "SparkApplication checkpoint did not persist every expected stage."
    }
    Assert-TestThrows -ExpectedPattern "conflicting observation.*bronze" -Action {
        Save-DataMasterSparkApplicationObservation -Path $checkpointPath `
            -DagId "banking_data_vault_pipeline" -RunId "minikube-e2e-test" `
            -Stage "bronze" -Name "run-bronze-12345678" `
            -Image "data-master-spark-jobs:git-deadbeef" `
            -CreationTimestamp "2026-07-14T01:00:00+00:00" | Out-Null
    }
    Save-DataMasterSparkApplicationObservation -Path $checkpointPath `
        -DagId "banking_data_vault_pipeline" -RunId "minikube-e2e-test" `
        -Stage "bronze" -Name "run-bronze-retry-12345678" `
        -Image "data-master-spark-jobs:git-1234567" `
        -CreationTimestamp "2026-07-14T01:01:00+00:00" | Out-Null
    $retriedCheckpoint = Read-DataMasterSparkApplicationObservationCheckpoint `
        -Path $checkpointPath -RequireComplete
    $bronzeObservation = @($retriedCheckpoint.observations | Where-Object {
        $_.stage -eq "bronze"
    })
    if ($bronzeObservation.Count -ne 1 -or
        $bronzeObservation[0].name -ne "run-bronze-retry-12345678") {
        throw "SparkApplication retry did not replace the older stage observation."
    }

    $missingCheckpointStage = Copy-TestEvidence -Value $checkpoint
    $missingCheckpointStage.observations = @(
        $missingCheckpointStage.observations | Where-Object { $_.stage -ne "gold" }
    )
    Assert-TestThrows -ExpectedPattern "checkpoint is missing stage.*gold" -Action {
        Assert-DataMasterSparkApplicationObservationCheckpoint `
            -Checkpoint $missingCheckpointStage -RequireComplete | Out-Null
    }

    $mutableCheckpointImage = Copy-TestEvidence -Value $checkpoint
    $mutableCheckpointImage.observations[0].image = "data-master-spark-jobs:latest"
    Assert-TestThrows -ExpectedPattern "checkpoint image must use immutable git tag" -Action {
        Assert-DataMasterSparkApplicationObservationCheckpoint `
            -Checkpoint $mutableCheckpointImage -RequireComplete | Out-Null
    }
}
finally {
    Remove-Item -LiteralPath $checkpointPath -Force -ErrorAction SilentlyContinue
}

$missingCount = Copy-TestEvidence -Value $valid
$missingCount.aggregate_counts.PSObject.Properties.Remove("gold")
Assert-TestThrows -ExpectedPattern "aggregate_counts.gold" -Action {
    Assert-DataMasterExecutionEvidence -Evidence $missingCount | Out-Null
}

$missingStage = Copy-TestEvidence -Value $valid
$missingStage.spark_applications = @(
    $missingStage.spark_applications | Where-Object { $_.stage -ne "gold" }
)
Assert-TestThrows -ExpectedPattern "missing SparkApplication.*gold" -Action {
    Assert-DataMasterExecutionEvidence -Evidence $missingStage | Out-Null
}

$mutableImage = Copy-TestEvidence -Value $valid
$mutableImage.images[1].reference = "data-master-spark-jobs:latest"
Assert-TestThrows -ExpectedPattern "immutable git tag" -Action {
    Assert-DataMasterExecutionEvidence -Evidence $mutableImage | Out-Null
}

$piiField = Copy-TestEvidence -Value $valid
$piiField.spark_applications[0] | Add-Member -NotePropertyName "cpf" `
    -NotePropertyValue "000.000.000-00"
Assert-TestThrows -ExpectedPattern "unsupported field|forbidden field" -Action {
    Assert-DataMasterExecutionEvidence -Evidence $piiField | Out-Null
}

$secretField = Copy-TestEvidence -Value $valid
$secretField.dag | Add-Member -NotePropertyName "token" `
    -NotePropertyValue "not-a-real-token"
Assert-TestThrows -ExpectedPattern "unsupported field|forbidden field" -Action {
    Assert-DataMasterExecutionEvidence -Evidence $secretField | Out-Null
}

Write-Output "DURABLE_EVIDENCE_UNIT_TEST_STATUS=PASS"
