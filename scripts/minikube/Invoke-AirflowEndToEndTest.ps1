[CmdletBinding()]
param(
    [string]$Profile = "data-master-repro-test",

    [string]$DagId = "banking_data_vault_pipeline",

    [string]$RunId,

    [string]$EvidencePath,

    [switch]$ResumeExistingRun,

    [switch]$MaterializeExistingEvidence,

    [ValidateRange(300, 10800)]
    [int]$TimeoutSeconds = 3600
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")
. (Join-Path $PSScriptRoot "DataMaster.ExecutionEvidence.ps1")

function Get-AirflowDagRun {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetRunId
    )
    $output = Invoke-DataMasterNative -FilePath "kubectl" `
        -CaptureOutput -Arguments @(
            "exec", "deployment/airflow", "-n", "data-platform", "--",
            "airflow", "dags", "list-runs", "--dag-id", $DagId,
            "--output", "json"
        )
    $runs = (($output -join [Environment]::NewLine) | ConvertFrom-Json)
    $run = @($runs | Where-Object { $_.run_id -eq $TargetRunId })
    if ($run.Count -ne 1) {
        throw "Unable to resolve exactly one Airflow run with run_id '$TargetRunId'."
    }
    return $run[0]
}

function Get-AirflowDagRunState {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetRunId
    )
    return (Get-AirflowDagRun -TargetRunId $TargetRunId).state
}

function Get-AirflowTaskStates {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetRunId
    )
    $output = Invoke-DataMasterNative -FilePath "kubectl" `
        -CaptureOutput -Arguments @(
            "exec", "deployment/airflow", "-n", "data-platform", "--",
            "airflow", "tasks", "states-for-dag-run", $DagId,
            $TargetRunId, "--output", "json"
        )
    return (($output -join [Environment]::NewLine) | ConvertFrom-Json)
}

function Get-AirflowTechnicalTaskLog {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetRunId,

        [Parameter(Mandatory = $true)]
        [string]$TaskId
    )
    if ($TargetRunId -notmatch '^[A-Za-z0-9._-]+$' -or
        $TaskId -notmatch '^[A-Za-z0-9._-]+$') {
        throw "Unsafe Airflow run or task identifier for durable evidence."
    }
    $baseOutput = Invoke-DataMasterNative -FilePath "kubectl" `
        -CaptureOutput -Arguments @(
            "exec", "deployment/airflow", "-n", "data-platform", "--",
            "airflow", "config", "get-value", "logging", "base_log_folder"
        )
    $base = ($baseOutput -join "").Trim().TrimEnd("/")
    $taskFolder = "$base/dag_id=$DagId/run_id=$TargetRunId/task_id=$TaskId"
    $fileOutput = Invoke-DataMasterNative -FilePath "kubectl" `
        -CaptureOutput -Arguments @(
            "exec", "deployment/airflow", "-n", "data-platform", "--",
            "find", $taskFolder, "-maxdepth", "1", "-type", "f"
        )
    $attempts = @()
    foreach ($line in $fileOutput) {
        if ([string]$line -match 'attempt=(\d+)\.log$') {
            $attempts += [pscustomobject]@{
                Path = [string]$line
                Attempt = [int]$Matches[1]
            }
        }
    }
    if ($attempts.Count -eq 0) {
        throw "No persisted Airflow task log found for '$TaskId'."
    }
    $path = ($attempts | Sort-Object Attempt -Descending | Select-Object -First 1).Path
    $pattern = (
        "job_id:|SPARK_STAGE_RESULT=|PRESENTATION_EVIDENCE=|" +
        "PRESENTATION_EVIDENCE_STATUS=|DATA_VAULT_[A-Z_]+=PASS|" +
        "MASKING_STATUS=PASS|GOLD_PII_EXPOSURE_STATUS=PASS|" +
        "Task exited with return code|exit code 137"
    )
    $technical = Invoke-DataMasterNative -FilePath "kubectl" `
        -CaptureOutput -Arguments @(
            "exec", "deployment/airflow", "-n", "data-platform", "--",
            "grep", "-E", $pattern, $path
        )
    return ($technical -join [Environment]::NewLine)
}

function Get-DataMasterLocalImageId {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Reference
    )
    $output = Invoke-DataMasterNative -FilePath "docker" `
        -CaptureOutput -Arguments @(
            "image", "inspect", "--format", "{{.Id}}", $Reference
        )
    return ($output -join "").Trim()
}

function Get-DataMasterLabelValue {
    param(
        [object]$Labels,

        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    if ($null -eq $Labels) {
        return $null
    }
    $property = $Labels.PSObject.Properties[$Name]
    if ($null -eq $property) {
        return $null
    }
    return [string]$property.Value
}

function Write-AirflowDurableEvidenceFailureRecord {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetRunId,

        [Parameter(Mandatory = $true)]
        [string]$TargetPath,

        [Parameter(Mandatory = $true)]
        [System.Management.Automation.ErrorRecord]$ErrorRecord
    )

    $phaseValue = Get-Variable -Name "DataMasterDurableEvidencePhase" `
        -Scope Script -ValueOnly -ErrorAction SilentlyContinue
    $phase = if ([string]::IsNullOrWhiteSpace($phaseValue)) {
        "unclassified_fail_closed"
    }
    else {
        $phaseValue
    }
    $failure = [ordered]@{
        schema_version = 1
        evidence_kind = "data_master_minikube_airflow_e2e_failure"
        captured_at = [DateTimeOffset]::UtcNow.ToString("o")
        dag = [ordered]@{
            dag_id = $DagId
            run_id = $TargetRunId
        }
        status = "FAIL"
        phase = $phase
        exception_type = [string]$ErrorRecord.Exception.GetType().Name
        privacy = [ordered]@{
            classification = "technical_aggregate_only"
            contains_pii = $false
            contains_secrets = $false
            contains_business_payload = $false
        }
    }
    Assert-DataMasterSensitiveContent -Evidence $failure
    $failurePath = "$TargetPath.failure.json"
    $resolved = [System.IO.Path]::GetFullPath($failurePath)
    $directory = [System.IO.Path]::GetDirectoryName($resolved)
    [System.IO.Directory]::CreateDirectory($directory) | Out-Null
    [System.IO.File]::WriteAllText(
        $resolved,
        ($failure | ConvertTo-Json -Depth 10) + [Environment]::NewLine,
        (New-Object System.Text.UTF8Encoding($false))
    )
    return $resolved
}

function Set-AirflowDurableEvidencePhase {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetRunId,

        [Parameter(Mandatory = $true)]
        [string]$TargetPath,

        [Parameter(Mandatory = $true)]
        [string]$Phase
    )

    $script:DataMasterDurableEvidencePhase = $Phase
    $progress = [ordered]@{
        schema_version = 1
        evidence_kind = "data_master_minikube_airflow_e2e_materialization_progress"
        recorded_at = [DateTimeOffset]::UtcNow.ToString("o")
        dag = [ordered]@{
            dag_id = $DagId
            run_id = $TargetRunId
        }
        phase = $Phase
        status = "IN_PROGRESS"
        privacy = [ordered]@{
            classification = "technical_aggregate_only"
            contains_pii = $false
            contains_secrets = $false
            contains_business_payload = $false
        }
    }
    Assert-DataMasterSensitiveContent -Evidence $progress
    $progressPath = "$TargetPath.progress.json"
    $resolved = [System.IO.Path]::GetFullPath($progressPath)
    $directory = [System.IO.Path]::GetDirectoryName($resolved)
    [System.IO.Directory]::CreateDirectory($directory) | Out-Null
    [System.IO.File]::WriteAllText(
        $resolved,
        ($progress | ConvertTo-Json -Depth 10) + [Environment]::NewLine,
        (New-Object System.Text.UTF8Encoding($false))
    )
    return $resolved
}

function Save-AirflowDurableEvidence {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetRunId,

        [Parameter(Mandatory = $true)]
        [string]$ObservationCheckpointPath,

        [Parameter(Mandatory = $true)]
        [string]$TargetPath
    )

    $expectedStages = @(
        "bronze", "hubs", "links", "satellites", "gold",
        "data-vault-gate", "masking-gate", "evidence"
    )
    Set-AirflowDurableEvidencePhase -TargetRunId $TargetRunId `
        -TargetPath $TargetPath -Phase "airflow_dag_run_metadata" | Out-Null
    $run = Get-AirflowDagRun -TargetRunId $TargetRunId
    if ($run.state -ne "success" -or -not $run.start_date -or -not $run.end_date) {
        throw "Airflow DAG run metadata is incomplete for durable evidence."
    }
    Set-AirflowDurableEvidencePhase -TargetRunId $TargetRunId `
        -TargetPath $TargetPath -Phase "airflow_task_states" | Out-Null
    $taskStates = @(Get-AirflowTaskStates -TargetRunId $TargetRunId)
    $taskById = @{}
    foreach ($task in $taskStates) {
        $taskById[[string]$task.task_id] = $task
    }
    Set-AirflowDurableEvidencePhase -TargetRunId $TargetRunId `
        -TargetPath $TargetPath -Phase "sparkapplication_checkpoint" | Out-Null
    $checkpoint = Read-DataMasterSparkApplicationObservationCheckpoint `
        -Path $ObservationCheckpointPath -RequireComplete
    if ($checkpoint.dag.dag_id -ne $DagId -or
        $checkpoint.dag.run_id -ne $TargetRunId) {
        throw "SparkApplication checkpoint does not belong to durable evidence run '$TargetRunId'."
    }
    $observationsByStage = @{}
    foreach ($observation in @($checkpoint.observations)) {
        $observationsByStage[[string]$observation.stage] = $observation
    }

    $applications = @()
    $stageStatuses = @()
    $technicalLogs = @{}
    foreach ($stage in $expectedStages) {
        $taskId = "run_" + $stage.Replace("-", "_")
        Set-AirflowDurableEvidencePhase -TargetRunId $TargetRunId `
            -TargetPath $TargetPath -Phase "airflow_task_metadata:$stage" | Out-Null
        if (-not $taskById.ContainsKey($taskId)) {
            throw "Airflow durable evidence is missing task '$taskId'."
        }
        $task = $taskById[$taskId]
        if ($task.state -ne "success" -or -not $task.start_date -or
            -not $task.end_date) {
            throw "Airflow task '$taskId' is incomplete for durable evidence."
        }
        if (-not $observationsByStage.ContainsKey($stage)) {
            throw "SparkApplication checkpoint is missing stage '$stage' and will not be inferred."
        }
        Set-AirflowDurableEvidencePhase -TargetRunId $TargetRunId `
            -TargetPath $TargetPath -Phase "airflow_task_log:$stage" | Out-Null
        $technicalLog = Get-AirflowTechnicalTaskLog `
            -TargetRunId $TargetRunId -TaskId $taskId
        $technicalLogs[$stage] = $technicalLog
        $jobMatches = [regex]::Matches(
            $technicalLog, 'job_id:\s*([a-z0-9-]+)'
        )
        $exitMatches = [regex]::Matches(
            $technicalLog, 'Task exited with return code\s+(\d+)'
        )
        $resultMatches = [regex]::Matches(
            $technicalLog, 'SPARK_STAGE_RESULT=(\{[^\r\n]+\})'
        )
        if ($jobMatches.Count -eq 0 -or $exitMatches.Count -eq 0 -or
            $resultMatches.Count -eq 0) {
            throw "Technical task log for '$taskId' is incomplete."
        }
        $applicationName = $jobMatches[$jobMatches.Count - 1].Groups[1].Value
        $taskExitCode = [int]$exitMatches[$exitMatches.Count - 1].Groups[1].Value
        $stageResult = $resultMatches[$resultMatches.Count - 1].Groups[1].Value |
            ConvertFrom-Json
        if ($stageResult.stage -ne $stage -or $stageResult.status -ne "SUCCESS") {
            throw "Technical stage result for '$stage' is inconsistent."
        }
        Set-AirflowDurableEvidencePhase -TargetRunId $TargetRunId `
            -TargetPath $TargetPath -Phase "checkpoint_match:$stage" | Out-Null
        $observed = $observationsByStage[$stage]
        if ($observed.name -ne $applicationName) {
            throw "SparkApplication checkpoint for '$stage' does not match persisted job_id."
        }
        $applications += [ordered]@{
            name = $applicationName
            stage = $stage
            image = $observed.image
            status = "SUCCESS"
            task_started_at = [string]$task.start_date
            task_finished_at = [string]$task.end_date
            task_exit_code = $taskExitCode
            evidence_source = "airflow_task_instance_and_log"
        }
        $stageStatuses += [ordered]@{
            stage = $stage
            task_id = $taskId
            application_name = $applicationName
            status = "SUCCESS"
            marker = "SPARK_STAGE_RESULT.stage=$stage,status=SUCCESS"
        }
    }

    Set-AirflowDurableEvidencePhase -TargetRunId $TargetRunId `
        -TargetPath $TargetPath -Phase "presentation_evidence_payload" | Out-Null
    $evidenceMatches = [regex]::Matches(
        $technicalLogs["evidence"], 'PRESENTATION_EVIDENCE=(\{[^\r\n]+\})'
    )
    if ($evidenceMatches.Count -eq 0 -or
        $technicalLogs["evidence"] -notmatch
        [regex]::Escape("PRESENTATION_EVIDENCE_STATUS=PASS")) {
        throw "Presentation evidence payload is missing from the durable source."
    }
    $presentation = $evidenceMatches[$evidenceMatches.Count - 1].Groups[1].Value |
        ConvertFrom-Json
    if ($presentation.status -ne "SUCCESS") {
        throw "Presentation evidence status is not SUCCESS."
    }

    $hasStorageEvidence = (
        $presentation.PSObject.Properties.Name -contains "storage"
    )

    $dataVaultMarkers = @(
        "DATA_VAULT_LINEAGE_STATUS=PASS",
        "DATA_VAULT_GOLD_LINEAGE_STATUS=PASS",
        "DATA_VAULT_QUALITY_GATE_STATUS=PASS"
    )
    if ($hasStorageEvidence) {
        $dataVaultMarkers += @(
            "GOLD_STORAGE_PATH_STATUS=PASS",
            "BUSINESS_VAULT_GOLD_PATH_SEPARATION_STATUS=PASS"
        )
    }
    $maskingMarkers = @(
        "MASKING_STATUS=PASS", "GOLD_PII_EXPOSURE_STATUS=PASS"
    )
    Set-AirflowDurableEvidencePhase -TargetRunId $TargetRunId `
        -TargetPath $TargetPath -Phase "quality_gate_markers" | Out-Null
    foreach ($marker in $dataVaultMarkers) {
        if ($technicalLogs["data-vault-gate"] -notmatch
            [regex]::Escape($marker)) {
            throw "Durable Data Vault marker is missing: $marker"
        }
    }
    foreach ($marker in $maskingMarkers) {
        if ($technicalLogs["masking-gate"] -notmatch
            [regex]::Escape($marker)) {
            throw "Durable masking marker is missing: $marker"
        }
    }

    Set-AirflowDurableEvidencePhase -TargetRunId $TargetRunId `
        -TargetPath $TargetPath -Phase "airflow_runtime_image" | Out-Null
    $airflowImageOutput = Invoke-DataMasterNative -FilePath "kubectl" `
        -CaptureOutput -Arguments @(
            "get", "deployment", "airflow", "-n", "data-platform",
            "-o", "jsonpath={.spec.template.spec.containers[0].image}"
        )
    $airflowImage = ($airflowImageOutput -join "").Trim()
    $imageReferences = @($airflowImage) + @(
        $applications | ForEach-Object { $_.image }
    )
    $imageReferences = @($imageReferences | Sort-Object -Unique)
    $commits = @()
    $images = @()
    Set-AirflowDurableEvidencePhase -TargetRunId $TargetRunId `
        -TargetPath $TargetPath -Phase "runtime_image_inventory" | Out-Null
    foreach ($reference in $imageReferences) {
        if ($reference -notmatch ':git-([0-9a-f]{7,40})$') {
            throw "Runtime image is not immutable: $reference"
        }
        $sha = $Matches[1]
        if (-not ($commits | Where-Object { $_.sha -eq $sha })) {
            Invoke-DataMasterNative -FilePath "git" -Arguments @(
                "-C", $root, "cat-file", "-e", "$sha^{commit}"
            ) | Out-Null
            $commits += [ordered]@{
                sha = $sha
                purpose = "runtime image captured by E2E"
            }
        }
        $role = if ($reference -like "data-master-airflow:*") {
            "airflow_orchestrator"
        }
        else {
            "spark_jobs_$sha"
        }
        $images += [ordered]@{
            role = $role
            reference = $reference
            image_id = Get-DataMasterLocalImageId -Reference $reference
        }
    }

    $risks = @()
    foreach ($stage in $expectedStages) {
        if ($technicalLogs[$stage] -match 'exit code 137') {
            $risks += [ordered]@{
                code = $stage.ToUpperInvariant().Replace("-", "_") +
                    "_EXECUTOR_OOMKILLED"
                component = "spark_executor"
                status = "OBSERVED_RECOVERED"
                observed_at = [string]$taskById[
                    "run_" + $stage.Replace("-", "_")
                ].end_date
                blocking = $false
                evidence_source = "airflow_task_log"
                observed_exit_code = 137
            }
        }
    }
    Set-AirflowDurableEvidencePhase -TargetRunId $TargetRunId `
        -TargetPath $TargetPath -Phase "hive_runtime_status" | Out-Null
    $hiveOutput = Invoke-DataMasterNative -FilePath "kubectl" `
        -CaptureOutput -Arguments @(
            "get", "pods", "-n", "data-platform",
            "-l", "app.kubernetes.io/name=hive-metastore", "-o", "json"
        )
    $hivePods = (($hiveOutput -join "") | ConvertFrom-Json).items
    if (@($hivePods).Count -eq 1) {
        $containerStatus = $hivePods[0].status.containerStatuses[0]
        if ([int]$containerStatus.restartCount -gt 0) {
            $lastTerminated = $containerStatus.lastState.terminated
            $risks += [ordered]@{
                code = "HIVE_METASTORE_RESTARTS"
                component = "hive_metastore"
                status = "OPEN_LOCAL_RUNTIME_LIMITATION"
                observed_at = if ($lastTerminated.finishedAt) {
                    [string]$lastTerminated.finishedAt
                }
                else {
                    [DateTimeOffset]::UtcNow.ToString("o")
                }
                blocking = $false
                evidence_source = "kubernetes_container_status"
                observed_exit_code = if ($null -ne $lastTerminated.exitCode) {
                    [int]$lastTerminated.exitCode
                }
                else { 0 }
                observed_count = [int]$containerStatus.restartCount
            }
        }
    }

    $evidence = [ordered]@{
        schema_version = 1
        evidence_kind = "data_master_minikube_airflow_e2e"
        captured_at = [DateTimeOffset]::UtcNow.ToString("o")
        source = [ordered]@{
            mode = "airflow_persisted_records_with_sparkapplication_checkpoint"
            records = @(
                "airflow_dag_run_json", "airflow_task_states_json",
                "airflow_technical_task_logs",
                "sparkapplication_observation_checkpoint",
                "docker_image_inventory"
            )
        }
        dag = [ordered]@{
            dag_id = $DagId
            run_id = $TargetRunId
            state = "SUCCESS"
            started_at = [string]$run.start_date
            finished_at = [string]$run.end_date
        }
        commits = $commits
        images = $images
        spark_applications = $applications
        stages = $stageStatuses
        quality_gates = [ordered]@{
            data_vault = [ordered]@{
                status = "PASS"
                markers = $dataVaultMarkers
                validated_at = [string]$taskById["run_data_vault_gate"].end_date
                evidence_source = "airflow_task_log"
            }
            masking = [ordered]@{
                status = "PASS"
                markers = $maskingMarkers
                validated_at = [string]$taskById["run_masking_gate"].end_date
                evidence_source = "airflow_task_log"
            }
            reproducibility = [ordered]@{
                status = "PENDING_VALIDATION"
                markers = @("REPRODUCIBILITY_GATE_STATUS=PENDING_VALIDATION")
                validated_at = $null
                evidence_source = "durable_evidence_gate_pending"
            }
        }
        technical_lineage = [ordered]@{
            path = [string]$presentation.lineage
            status = "PASS"
        }
        aggregate_counts = [ordered]@{
            bronze = [long]$presentation.counts.bronze
            raw_vault_hubs = [long]$presentation.counts.raw_vault_hubs
            raw_vault_links = [long]$presentation.counts.raw_vault_links
            raw_vault_satellites = [long]$presentation.counts.raw_vault_satellites
            gold = [long]$presentation.counts.gold
        }
        privacy = [ordered]@{
            classification = "technical_aggregate_only"
            contains_pii = $false
            contains_secrets = $false
            contains_business_payload = $false
        }
        operational_risks = $risks
    }
    if ($hasStorageEvidence) {
        $evidence["storage"] = [ordered]@{
            business_vault_path = [string]$presentation.storage.business_vault_path
            gold_path = [string]$presentation.storage.gold_path
            gold_tables = $presentation.storage.gold_tables
        }
    }
    Set-AirflowDurableEvidencePhase -TargetRunId $TargetRunId `
        -TargetPath $TargetPath -Phase "write_execution_evidence" | Out-Null
    Write-DataMasterExecutionEvidence -Evidence $evidence -Path $TargetPath
    Set-AirflowDurableEvidencePhase -TargetRunId $TargetRunId `
        -TargetPath $TargetPath -Phase "complete" | Out-Null
    return [System.IO.Path]::GetFullPath($TargetPath)
}

Set-DataMasterMinikubeContext -Profile $Profile
Write-Output "AIRFLOW_E2E_MATERIALIZE_EXISTING_EVIDENCE=$MaterializeExistingEvidence"
$root = Get-DataMasterRepositoryRoot
$dagSource = [System.IO.File]::ReadAllText(
    (Join-Path $root "dags\banking_data_vault_pipeline_dag.py")
)
foreach ($forbidden in @("local[*]", "SparkSubmitOperator", "SparkSession")) {
    if ($dagSource.Contains($forbidden)) {
        throw "Official Airflow DAG contains forbidden local processing token: $forbidden"
    }
}

$runIdWasProvided = [bool]$RunId
if (-not $RunId) {
    $RunId = "minikube-e2e-" + (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmss")
}
if ($ResumeExistingRun -and -not $runIdWasProvided) {
    throw "-ResumeExistingRun requires an explicit -RunId."
}
if ($MaterializeExistingEvidence -and -not $runIdWasProvided) {
    throw "-MaterializeExistingEvidence requires an explicit -RunId."
}
if (-not $EvidencePath) {
    $EvidencePath = Join-Path $root "evidence\runtime\$RunId.json"
}
elseif (-not [System.IO.Path]::IsPathRooted($EvidencePath)) {
    $EvidencePath = Join-Path $root $EvidencePath
}
$EvidencePath = [System.IO.Path]::GetFullPath($EvidencePath)
$observationCheckpointPath = "$EvidencePath.sparkapplications.checkpoint.json"
if ($MaterializeExistingEvidence) {
    try {
        Set-AirflowDurableEvidencePhase -TargetRunId $RunId `
            -TargetPath $EvidencePath -Phase "materialization_start" | Out-Null
        $resolvedEvidencePath = Save-AirflowDurableEvidence `
            -TargetRunId $RunId `
            -ObservationCheckpointPath $observationCheckpointPath `
            -TargetPath $EvidencePath
        Write-Output "AIRFLOW_DURABLE_EVIDENCE_MATERIALIZATION_STATUS=PASS"
        Write-Output "AIRFLOW_DAG_RUN_ID=$RunId"
        Write-Output "AIRFLOW_DAG_RUN_STATUS=PASS"
        Write-Output "AIRFLOW_DURABLE_EVIDENCE_STATUS=PASS"
        Write-Output "AIRFLOW_DURABLE_EVIDENCE_PATH=$resolvedEvidencePath"
        return
    }
    catch {
        $originalError = $_
        $failurePath = $null
        try {
            $failurePath = Write-AirflowDurableEvidenceFailureRecord `
                -TargetRunId $RunId -TargetPath $EvidencePath `
                -ErrorRecord $originalError
        }
        catch {
            $failurePath = $null
        }
        if ($failurePath) {
            Write-Output "AIRFLOW_DURABLE_EVIDENCE_FAILURE_RECORD=$failurePath"
        }
        Write-Output "AIRFLOW_DURABLE_EVIDENCE_MATERIALIZATION_STATUS=FAIL"
        Write-Output "AIRFLOW_DURABLE_EVIDENCE_STATUS=FAIL"
        throw $originalError
    }
}
$started = Get-Date
try {
    Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
        "exec", "deployment/airflow", "-n", "data-platform", "--",
        "airflow", "dags", "unpause", $DagId
    ) | Out-Null
    if (-not $ResumeExistingRun) {
        Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
            "exec", "deployment/airflow", "-n", "data-platform", "--",
            "airflow", "dags", "trigger", $DagId, "--run-id", $RunId
        )
    }
    else {
        Get-AirflowDagRunState -TargetRunId $RunId | Out-Null
    }
    $dagRun = Get-AirflowDagRun -TargetRunId $RunId
    $runStartText = [string]$dagRun.start_date
    if (-not $runStartText) {
        $runStartText = [string]$dagRun.execution_date
    }
    $observationStart = if ($runStartText) {
        [DateTimeOffset]::Parse($runStartText).UtcDateTime
    }
    else {
        $started.ToUniversalTime()
    }
    if ($ResumeExistingRun -and $runStartText) {
        $started = $observationStart
    }
    Write-Output "AIRFLOW_DAG_TRIGGER_STATUS=PASS"
    if ($ResumeExistingRun) {
        Write-Output "AIRFLOW_DAG_TRIGGER_MODE=RESUME_EXISTING_RUN"
    }
    else {
        Write-Output "AIRFLOW_DAG_TRIGGER_MODE=NEW_RUN"
    }

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $state = "queued"
    while ((Get-Date) -lt $deadline) {
        $state = Get-AirflowDagRunState -TargetRunId $RunId
        $applicationOutput = Invoke-DataMasterNative -FilePath "kubectl" `
            -CaptureOutput -Arguments @(
                "get", "sparkapplications", "-n", "data-platform", "-o", "json"
            )
        $applications = (($applicationOutput -join "") | ConvertFrom-Json).items
        $matchingApplications = @($applications | Where-Object {
            $created = [DateTimeOffset]::Parse($_.metadata.creationTimestamp)
            (Get-DataMasterLabelValue `
                -Labels $_.spec.driver.labels `
                -Name "data-master.io/runtime-profile") -eq "presentation-demo" -and
                $created.UtcDateTime -ge $observationStart
        } | Sort-Object { $_.metadata.creationTimestamp })
        foreach ($application in $matchingApplications) {
            $stage = Get-DataMasterLabelValue `
                -Labels $application.spec.driver.labels `
                -Name "data-master.io/stage"
            if ($stage) {
                Save-DataMasterSparkApplicationObservation `
                    -Path $observationCheckpointPath -DagId $DagId `
                    -RunId $RunId -Stage $stage `
                    -Name ([string]$application.metadata.name) `
                    -Image ([string]$application.spec.image) `
                    -CreationTimestamp ([string]$application.metadata.creationTimestamp) |
                    Out-Null
            }
        }
        if ($state -eq "success") { break }
        if ($state -eq "failed") { throw "Airflow DAG run failed: $runId" }
        Start-Sleep -Seconds 10
    }
    if ($state -ne "success") {
        throw "Airflow DAG did not complete before timeout; state=$state"
    }
    $resolvedEvidencePath = Save-AirflowDurableEvidence `
        -TargetRunId $RunId `
        -ObservationCheckpointPath $observationCheckpointPath `
        -TargetPath $EvidencePath

    $duration = [int]((Get-Date) - $started).TotalSeconds
    Write-Output "TEST_PATH=MINIKUBE_AIRFLOW_E2E"
    Write-Output "AIRFLOW_DAG_RUN_ID=$RunId"
    Write-Output "AIRFLOW_DAG_DURATION_SECONDS=$duration"
    Write-Output "AIRFLOW_DAG_RUN_STATUS=PASS"
    Write-Output "AIRFLOW_DURABLE_EVIDENCE_STATUS=PASS"
    Write-Output "AIRFLOW_DURABLE_EVIDENCE_PATH=$resolvedEvidencePath"
    Write-Output "AIRFLOW_SPARK_SUBMISSION_STATUS=PASS"
    Write-Output "AIRFLOW_SPARK_MONITORING_STATUS=PASS"
    Write-Output "AIRFLOW_LOCAL_SPARK_EXECUTION_DETECTED=NO"
    Write-Output "AIRFLOW_E2E_STATUS=PASS"
}
catch {
    $originalError = $_
    $failurePath = $null
    try {
        $failurePath = Write-AirflowDurableEvidenceFailureRecord `
            -TargetRunId $RunId -TargetPath $EvidencePath `
            -ErrorRecord $originalError
    }
    catch {
        $failurePath = $null
    }
    if ($failurePath) {
        Write-Output "AIRFLOW_DURABLE_EVIDENCE_FAILURE_RECORD=$failurePath"
    }
    Write-Output "AIRFLOW_DURABLE_EVIDENCE_STATUS=FAIL"
    Write-Output "AIRFLOW_E2E_STATUS=FAIL"
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & kubectl exec deployment/airflow -n data-platform -- `
            airflow tasks states-for-dag-run $DagId $RunId 2>$null
        & kubectl get sparkapplications,pods -n data-platform -o wide 2>$null
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    throw $originalError
}
