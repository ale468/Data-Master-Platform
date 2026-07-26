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
