Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:DataMasterExpectedStages = @(
    "bronze",
    "hubs",
    "links",
    "satellites",
    "gold",
    "data-vault-gate",
    "masking-gate",
    "evidence"
)

$script:DataMasterExpectedGoldTables = @(
    "gold_transacoes_por_dia",
    "gold_transacoes_por_cliente",
    "gold_volume_por_produto",
    "gold_eventos_digitais_por_canal",
    "gold_contas_por_agencia",
    "gold_risco_transacional_simplificado",
    "gold_clientes_protegidos"
)

$script:DataMasterSparkApplicationCheckpointKind =
    "data_master_spark_application_observation_checkpoint"

function ConvertFrom-DataMasterJson {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Json
    )

    $convertFromJson = Get-Command ConvertFrom-Json
    if ($convertFromJson.Parameters.ContainsKey("DateKind")) {
        return ConvertFrom-Json -InputObject $Json -DateKind String
    }
    return ConvertFrom-Json -InputObject $Json
}

function Assert-DataMasterObjectShape {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Value,

        [Parameter(Mandatory = $true)]
        [string]$Context,

        [Parameter(Mandatory = $true)]
        [string[]]$Required,

        [string[]]$Optional = @()
    )

    if ($null -eq $Value) {
        throw "Durable evidence is missing object '$Context'."
    }
    $properties = if ($Value -is [System.Collections.IDictionary]) {
        @($Value.Keys | ForEach-Object { [string]$_ })
    }
    else {
        @($Value.PSObject.Properties | ForEach-Object { $_.Name })
    }
    foreach ($name in $Required) {
        if ($name -notin $properties) {
            throw "Durable evidence is missing required field '$Context.$name'."
        }
    }
    $allowed = @($Required) + @($Optional)
    foreach ($name in $properties) {
        if ($name -notin $allowed) {
            throw "Durable evidence contains unsupported field '$Context.$name'."
        }
    }
}

function Assert-DataMasterNonEmptyString {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$Value,

        [Parameter(Mandatory = $true)]
        [string]$Context
    )

    if ($Value -isnot [string] -or [string]::IsNullOrWhiteSpace($Value)) {
        throw "Durable evidence field '$Context' must be a non-empty string."
    }
}

function Assert-DataMasterInteger {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$Value,

        [Parameter(Mandatory = $true)]
        [string]$Context,

        [long]$Minimum = 0
    )

    if ($Value -isnot [byte] -and $Value -isnot [int16] -and
        $Value -isnot [int32] -and $Value -isnot [int64]) {
        throw "Durable evidence field '$Context' must be an integer."
    }
    if ([long]$Value -lt $Minimum) {
        throw "Durable evidence field '$Context' must be >= $Minimum."
    }
}

function ConvertTo-DataMasterTimestamp {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$Value,

        [Parameter(Mandatory = $true)]
        [string]$Context
    )

    Assert-DataMasterNonEmptyString -Value $Value -Context $Context
    $parsed = [DateTimeOffset]::MinValue
    $valid = [DateTimeOffset]::TryParse(
        [string]$Value,
        [System.Globalization.CultureInfo]::InvariantCulture,
        [System.Globalization.DateTimeStyles]::RoundtripKind,
        [ref]$parsed
    )
    if (-not $valid) {
        throw "Durable evidence field '$Context' must be an ISO-8601 timestamp."
    }
    return $parsed
}

function Assert-DataMasterForbiddenProperties {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$Value,

        [string]$Context = "root"
    )

    $forbidden = @(
        "cpf", "nome", "email", "telefone", "endereco",
        "data_nascimento", "numero_cartao", "payload", "cliente",
        "customer", "password", "passwd", "token", "credential",
        "secret", "secret_key", "access_key"
    )
    if ($null -eq $Value -or $Value -is [string] -or
        $Value -is [ValueType]) {
        return
    }
    if ($Value -is [System.Collections.IDictionary]) {
        foreach ($key in @($Value.Keys)) {
            $keyName = [string]$key
            if ($keyName.ToLowerInvariant() -in $forbidden) {
                throw "Durable evidence contains forbidden field '$Context.$keyName'."
            }
            Assert-DataMasterForbiddenProperties `
                -Value $Value[$key] -Context "$Context.$keyName"
        }
        return
    }
    if ($Value -is [System.Collections.IEnumerable] -and
        $Value -isnot [System.Management.Automation.PSCustomObject]) {
        $index = 0
        foreach ($item in $Value) {
            Assert-DataMasterForbiddenProperties `
                -Value $item -Context "$Context[$index]"
            $index++
        }
        return
    }

    foreach ($property in $Value.PSObject.Properties) {
        if ($property.Name.ToLowerInvariant() -in $forbidden) {
            throw "Durable evidence contains forbidden field '$Context.$($property.Name)'."
        }
        Assert-DataMasterForbiddenProperties `
            -Value $property.Value -Context "$Context.$($property.Name)"
    }
}

function Assert-DataMasterSensitiveContent {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Evidence
    )

    Assert-DataMasterForbiddenProperties -Value $Evidence
    $json = $Evidence | ConvertTo-Json -Depth 20 -Compress
    foreach ($pattern in @(
        '(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b',
        '\b\d{3}\.\d{3}\.\d{3}-\d{2}\b',
        '\b\d{4}[ -]\d{4}[ -]\d{4}[ -]\d{4}\b',
        '(?i)"(?:password|passwd|token|credential|secret_key|access_key)"\s*:\s*"[^\"]+"',
        '(?i)\bBearer\s+[A-Za-z0-9._-]+'
    )) {
        if ($json -match $pattern) {
            throw "Durable evidence contains a forbidden personal-data or secret pattern."
        }
    }
}

function Assert-DataMasterTechnicalOnlyPrivacy {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Privacy,

        [Parameter(Mandatory = $true)]
        [string]$Context
    )

    Assert-DataMasterObjectShape -Value $Privacy -Context $Context `
        -Required @(
            "classification", "contains_pii", "contains_secrets",
            "contains_business_payload"
        )
    if ($Privacy.classification -ne "technical_aggregate_only") {
        throw "Durable evidence privacy classification is invalid."
    }
    foreach ($flag in @(
        "contains_pii", "contains_secrets", "contains_business_payload"
    )) {
        if ($Privacy.$flag -isnot [bool] -or $Privacy.$flag) {
            throw "Durable evidence privacy flag '$flag' must be boolean false."
        }
    }
}

function New-DataMasterSparkApplicationObservationCheckpoint {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$DagId,

        [Parameter(Mandatory = $true)]
        [string]$RunId
    )

    return [pscustomobject][ordered]@{
        schema_version = 1
        evidence_kind = $script:DataMasterSparkApplicationCheckpointKind
        captured_at = [DateTimeOffset]::UtcNow.ToString("o")
        source = "kubernetes_sparkapplication_observation"
        dag = [pscustomobject][ordered]@{
            dag_id = $DagId
            run_id = $RunId
        }
        observations = @()
        privacy = [pscustomobject][ordered]@{
            classification = "technical_aggregate_only"
            contains_pii = $false
            contains_secrets = $false
            contains_business_payload = $false
        }
    }
}

function Assert-DataMasterSparkApplicationObservationCheckpoint {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Checkpoint,

        [switch]$RequireComplete
    )

    Assert-DataMasterObjectShape -Value $Checkpoint -Context "checkpoint" `
        -Required @(
            "schema_version", "evidence_kind", "captured_at", "source",
            "dag", "observations", "privacy"
        )
    Assert-DataMasterInteger -Value $Checkpoint.schema_version `
        -Context "checkpoint.schema_version" -Minimum 1
    if ([int]$Checkpoint.schema_version -ne 1) {
        throw "Unsupported SparkApplication checkpoint schema_version '$($Checkpoint.schema_version)'."
    }
    if ($Checkpoint.evidence_kind -ne $script:DataMasterSparkApplicationCheckpointKind) {
        throw "Unsupported SparkApplication checkpoint kind '$($Checkpoint.evidence_kind)'."
    }
    ConvertTo-DataMasterTimestamp -Value $Checkpoint.captured_at `
        -Context "checkpoint.captured_at" | Out-Null
    if ($Checkpoint.source -ne "kubernetes_sparkapplication_observation") {
        throw "SparkApplication checkpoint has an unsupported source."
    }
    Assert-DataMasterObjectShape -Value $Checkpoint.dag -Context "checkpoint.dag" `
        -Required @("dag_id", "run_id")
    foreach ($field in @("dag_id", "run_id")) {
        Assert-DataMasterNonEmptyString -Value $Checkpoint.dag.$field `
            -Context "checkpoint.dag.$field"
    }

    $observations = @($Checkpoint.observations)
    if ($observations.Count -eq 0) {
        throw "SparkApplication checkpoint has no observations."
    }
    $stages = @()
    $names = @()
    foreach ($observation in $observations) {
        Assert-DataMasterObjectShape -Value $observation `
            -Context "checkpoint.observations[]" -Required @(
                "stage", "name", "image", "creation_timestamp", "observed_at"
            )
        foreach ($field in @("stage", "name", "image")) {
            Assert-DataMasterNonEmptyString -Value $observation.$field `
                -Context "checkpoint.observations[].$field"
        }
        if ($observation.stage -notin $script:DataMasterExpectedStages) {
            throw "SparkApplication checkpoint contains unsupported stage '$($observation.stage)'."
        }
        if ($observation.name -notmatch '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$') {
            throw "SparkApplication checkpoint contains invalid name '$($observation.name)'."
        }
        if ($observation.image -notmatch '^[a-z0-9./_-]+:git-[0-9a-f]{7,40}$') {
            throw "SparkApplication checkpoint image must use immutable git tag: '$($observation.image)'."
        }
        ConvertTo-DataMasterTimestamp -Value $observation.creation_timestamp `
            -Context "checkpoint.observations[].creation_timestamp" | Out-Null
        ConvertTo-DataMasterTimestamp -Value $observation.observed_at `
            -Context "checkpoint.observations[].observed_at" | Out-Null
        if ($observation.stage -in $stages) {
            throw "SparkApplication checkpoint contains duplicate stage '$($observation.stage)'."
        }
        if ($observation.name -in $names) {
            throw "SparkApplication checkpoint contains duplicate name '$($observation.name)'."
        }
        $stages += [string]$observation.stage
        $names += [string]$observation.name
    }
    if ($RequireComplete) {
        foreach ($stage in $script:DataMasterExpectedStages) {
            if ($stage -notin $stages) {
                throw "SparkApplication checkpoint is missing stage '$stage'."
            }
        }
    }
    Assert-DataMasterTechnicalOnlyPrivacy -Privacy $Checkpoint.privacy `
        -Context "checkpoint.privacy"
    Assert-DataMasterSensitiveContent -Evidence $Checkpoint
    return $Checkpoint
}

function Read-DataMasterSparkApplicationObservationCheckpoint {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [switch]$RequireComplete
    )

    $resolved = [System.IO.Path]::GetFullPath($Path)
    if (-not [System.IO.File]::Exists($resolved)) {
        throw "SparkApplication observation checkpoint does not exist: $resolved"
    }
    try {
        $checkpoint = ConvertFrom-DataMasterJson -Json (
            [System.IO.File]::ReadAllText($resolved)
        )
    }
    catch {
        throw "SparkApplication observation checkpoint is not valid JSON: $resolved"
    }
    if ($RequireComplete) {
        return Assert-DataMasterSparkApplicationObservationCheckpoint `
            -Checkpoint $checkpoint -RequireComplete
    }
    return Assert-DataMasterSparkApplicationObservationCheckpoint `
        -Checkpoint $checkpoint
}

function Write-DataMasterSparkApplicationObservationCheckpoint {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Checkpoint,

        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    Assert-DataMasterSparkApplicationObservationCheckpoint `
        -Checkpoint $Checkpoint | Out-Null
    $resolved = [System.IO.Path]::GetFullPath($Path)
    $directory = [System.IO.Path]::GetDirectoryName($resolved)
    [System.IO.Directory]::CreateDirectory($directory) | Out-Null
    $temporaryPath = "$resolved.$([guid]::NewGuid().ToString('N')).tmp"
    try {
        $json = $Checkpoint | ConvertTo-Json -Depth 20
        [System.IO.File]::WriteAllText(
            $temporaryPath,
            $json + [Environment]::NewLine,
            (New-Object System.Text.UTF8Encoding($false))
        )
        if ([System.IO.File]::Exists($resolved)) {
            Move-Item -LiteralPath $temporaryPath -Destination $resolved -Force
        }
        else {
            [System.IO.File]::Move($temporaryPath, $resolved)
        }
    }
    finally {
        if ([System.IO.File]::Exists($temporaryPath)) {
            Remove-Item -LiteralPath $temporaryPath -Force -ErrorAction SilentlyContinue
        }
    }
}

function Save-DataMasterSparkApplicationObservation {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [string]$DagId,

        [Parameter(Mandatory = $true)]
        [string]$RunId,

        [Parameter(Mandatory = $true)]
        [string]$Stage,

        [Parameter(Mandatory = $true)]
        [string]$Name,

        [Parameter(Mandatory = $true)]
        [string]$Image,

        [Parameter(Mandatory = $true)]
        [string]$CreationTimestamp
    )

    $checkpoint = if ([System.IO.File]::Exists(
        [System.IO.Path]::GetFullPath($Path)
    )) {
        Read-DataMasterSparkApplicationObservationCheckpoint -Path $Path
    }
    else {
        New-DataMasterSparkApplicationObservationCheckpoint `
            -DagId $DagId -RunId $RunId
    }
    if ($checkpoint.dag.dag_id -ne $DagId -or
        $checkpoint.dag.run_id -ne $RunId) {
        throw "SparkApplication checkpoint does not belong to Airflow run '$RunId'."
    }
    $existing = @($checkpoint.observations | Where-Object {
        $_.stage -eq $Stage
    })
    if ($existing.Count -eq 1) {
        if ($existing[0].name -ne $Name -or
            $existing[0].image -ne $Image -or
            $existing[0].creation_timestamp -ne $CreationTimestamp) {
            throw "SparkApplication checkpoint contains conflicting observation for stage '$Stage'."
        }
        return $checkpoint
    }
    if ($existing.Count -gt 1) {
        throw "SparkApplication checkpoint contains duplicate stage '$Stage'."
    }
    $checkpoint.observations = @($checkpoint.observations) + @(
        [pscustomobject][ordered]@{
            stage = $Stage
            name = $Name
            image = $Image
            creation_timestamp = $CreationTimestamp
            observed_at = [DateTimeOffset]::UtcNow.ToString("o")
        }
    )
    $checkpoint.captured_at = [DateTimeOffset]::UtcNow.ToString("o")
    Write-DataMasterSparkApplicationObservationCheckpoint `
        -Checkpoint $checkpoint -Path $Path
    return $checkpoint
}

function Assert-DataMasterMarkerSet {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$Markers,

        [Parameter(Mandatory = $true)]
        [string[]]$Required,

        [Parameter(Mandatory = $true)]
        [string]$Context
    )

    $resolved = @($Markers | ForEach-Object { [string]$_ })
    foreach ($marker in $Required) {
        if ($marker -notin $resolved) {
            throw "Durable evidence is missing required marker '$marker' in '$Context'."
        }
    }
}

function Assert-DataMasterExecutionEvidence {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Evidence
    )

    Assert-DataMasterObjectShape -Value $Evidence -Context "root" -Required @(
        "schema_version", "evidence_kind", "captured_at", "source", "dag",
        "commits", "images", "spark_applications", "stages",
        "quality_gates", "technical_lineage", "aggregate_counts",
        "privacy", "operational_risks"
    ) -Optional @("storage")
    Assert-DataMasterInteger -Value $Evidence.schema_version `
        -Context "schema_version" -Minimum 1
    if ([int]$Evidence.schema_version -ne 1) {
        throw "Unsupported durable evidence schema_version '$($Evidence.schema_version)'."
    }
    if ($Evidence.evidence_kind -ne "data_master_minikube_airflow_e2e") {
        throw "Unsupported durable evidence kind '$($Evidence.evidence_kind)'."
    }
    ConvertTo-DataMasterTimestamp -Value $Evidence.captured_at `
        -Context "captured_at" | Out-Null

    Assert-DataMasterObjectShape -Value $Evidence.source -Context "source" `
        -Required @("mode", "records")
    Assert-DataMasterNonEmptyString -Value $Evidence.source.mode `
        -Context "source.mode"
    $sourceRecords = @($Evidence.source.records)
    if ($sourceRecords.Count -eq 0) {
        throw "Durable evidence must identify at least one captured source record."
    }
    foreach ($record in $sourceRecords) {
        Assert-DataMasterNonEmptyString -Value $record -Context "source.records"
    }

    Assert-DataMasterObjectShape -Value $Evidence.dag -Context "dag" -Required @(
        "dag_id", "run_id", "state", "started_at", "finished_at"
    )
    foreach ($field in @("dag_id", "run_id", "state")) {
        Assert-DataMasterNonEmptyString -Value $Evidence.dag.$field `
            -Context "dag.$field"
    }
    if ($Evidence.dag.state -ne "SUCCESS") {
        throw "Durable evidence DAG state must be SUCCESS."
    }
    $dagStart = ConvertTo-DataMasterTimestamp -Value $Evidence.dag.started_at `
        -Context "dag.started_at"
    $dagEnd = ConvertTo-DataMasterTimestamp -Value $Evidence.dag.finished_at `
        -Context "dag.finished_at"
    if ($dagEnd -lt $dagStart) {
        throw "Durable evidence DAG timestamps are out of order."
    }
    $commits = @($Evidence.commits)
    if ($commits.Count -eq 0) {
        throw "Durable evidence must contain at least one commit."
    }
    $commitShas = @()
    foreach ($commit in $commits) {
        Assert-DataMasterObjectShape -Value $commit -Context "commits[]" `
            -Required @("sha", "purpose")
        if ($commit.sha -notmatch '^[0-9a-f]{7,40}$') {
            throw "Durable evidence contains invalid commit SHA '$($commit.sha)'."
        }
        Assert-DataMasterNonEmptyString -Value $commit.purpose `
            -Context "commits[].purpose"
        $commitShas += [string]$commit.sha
    }

    $images = @($Evidence.images)
    if ($images.Count -lt 2) {
        throw "Durable evidence must contain immutable Airflow and Spark images."
    }
    $imageReferences = @()
    foreach ($image in $images) {
        Assert-DataMasterObjectShape -Value $image -Context "images[]" `
            -Required @("role", "reference", "image_id")
        Assert-DataMasterNonEmptyString -Value $image.role -Context "images[].role"
        if ($image.reference -notmatch '^[a-z0-9./_-]+:git-([0-9a-f]{7,40})$') {
            throw "Durable evidence image must use immutable git tag: '$($image.reference)'."
        }
        $imageCommit = $Matches[1]
        if ($imageCommit -notin $commitShas) {
            throw "Durable evidence image tag '$($image.reference)' has no matching commit."
        }
        if ($image.image_id -notmatch '^sha256:[0-9a-f]{64}$') {
            throw "Durable evidence contains invalid image ID for '$($image.reference)'."
        }
        if ($image.reference -in $imageReferences) {
            throw "Durable evidence contains duplicate image '$($image.reference)'."
        }
        $imageReferences += [string]$image.reference
    }

    $applications = @($Evidence.spark_applications)
    $applicationStages = @()
    $applicationNames = @()
    foreach ($application in $applications) {
        Assert-DataMasterObjectShape -Value $application `
            -Context "spark_applications[]" -Required @(
                "name", "stage", "image", "status", "task_started_at",
                "task_finished_at", "task_exit_code", "evidence_source"
            )
        foreach ($field in @("name", "stage", "image", "status", "evidence_source")) {
            Assert-DataMasterNonEmptyString -Value $application.$field `
                -Context "spark_applications[].$field"
        }
        if ($application.name -notmatch '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$') {
            throw "Durable evidence contains invalid SparkApplication name '$($application.name)'."
        }
        if ($application.stage -notin $script:DataMasterExpectedStages) {
            throw "Durable evidence contains unsupported stage '$($application.stage)'."
        }
        if ($application.image -notin $imageReferences) {
            throw "SparkApplication '$($application.name)' references unregistered image '$($application.image)'."
        }
        if ($application.status -ne "SUCCESS") {
            throw "SparkApplication '$($application.name)' is not recorded as SUCCESS."
        }
        $start = ConvertTo-DataMasterTimestamp -Value $application.task_started_at `
            -Context "spark_applications[].task_started_at"
        $end = ConvertTo-DataMasterTimestamp -Value $application.task_finished_at `
            -Context "spark_applications[].task_finished_at"
        if ($end -lt $start) {
            throw "SparkApplication '$($application.name)' timestamps are out of order."
        }
        Assert-DataMasterInteger -Value $application.task_exit_code `
            -Context "spark_applications[].task_exit_code"
        if ([long]$application.task_exit_code -ne 0) {
            throw "SparkApplication '$($application.name)' task exit code must be 0."
        }
        if ($application.stage -in $applicationStages) {
            throw "Durable evidence contains duplicate SparkApplication stage '$($application.stage)'."
        }
        if ($application.name -in $applicationNames) {
            throw "Durable evidence contains duplicate SparkApplication name '$($application.name)'."
        }
        $applicationStages += [string]$application.stage
        $applicationNames += [string]$application.name
    }
    foreach ($stage in $script:DataMasterExpectedStages) {
        if ($stage -notin $applicationStages) {
            throw "Durable evidence is missing SparkApplication for stage '$stage'."
        }
    }

    $stages = @($Evidence.stages)
    $stageNames = @()
    foreach ($stageRecord in $stages) {
        Assert-DataMasterObjectShape -Value $stageRecord -Context "stages[]" `
            -Required @("stage", "task_id", "application_name", "status", "marker")
        foreach ($field in @("stage", "task_id", "application_name", "status", "marker")) {
            Assert-DataMasterNonEmptyString -Value $stageRecord.$field `
                -Context "stages[].$field"
        }
        if ($stageRecord.stage -notin $script:DataMasterExpectedStages) {
            throw "Durable evidence contains unsupported stage status '$($stageRecord.stage)'."
        }
        if ($stageRecord.status -ne "SUCCESS") {
            throw "Durable evidence stage '$($stageRecord.stage)' is not SUCCESS."
        }
        $expectedTask = "run_" + $stageRecord.stage.Replace("-", "_")
        if ($stageRecord.task_id -ne $expectedTask) {
            throw "Durable evidence stage '$($stageRecord.stage)' has unexpected task_id."
        }
        $applicationIndex = [array]::IndexOf($applicationStages, [string]$stageRecord.stage)
        if ($applicationIndex -lt 0 -or
            $applicationNames[$applicationIndex] -ne $stageRecord.application_name) {
            throw "Durable evidence stage '$($stageRecord.stage)' does not map to its SparkApplication."
        }
        if ($stageRecord.stage -in $stageNames) {
            throw "Durable evidence contains duplicate stage status '$($stageRecord.stage)'."
        }
        $stageNames += [string]$stageRecord.stage
    }
    foreach ($stage in $script:DataMasterExpectedStages) {
        if ($stage -notin $stageNames) {
            throw "Durable evidence is missing status for stage '$stage'."
        }
    }

    Assert-DataMasterObjectShape -Value $Evidence.quality_gates `
        -Context "quality_gates" -Required @(
            "data_vault", "masking", "reproducibility"
        )
    foreach ($gateName in @("data_vault", "masking", "reproducibility")) {
        $gate = $Evidence.quality_gates.$gateName
        Assert-DataMasterObjectShape -Value $gate -Context "quality_gates.$gateName" `
            -Required @("status", "markers", "validated_at", "evidence_source")
        Assert-DataMasterNonEmptyString -Value $gate.status `
            -Context "quality_gates.$gateName.status"
        Assert-DataMasterNonEmptyString -Value $gate.evidence_source `
            -Context "quality_gates.$gateName.evidence_source"
        if (@($gate.markers).Count -eq 0) {
            throw "Durable evidence gate '$gateName' has no markers."
        }
    }
    if ($Evidence.quality_gates.data_vault.status -ne "PASS") {
        throw "Durable evidence Data Vault gate status must be PASS."
    }
    if ($Evidence.quality_gates.masking.status -ne "PASS") {
        throw "Durable evidence masking gate status must be PASS."
    }
    if ($Evidence.quality_gates.reproducibility.status -notin @(
        "PENDING_VALIDATION", "PASS"
    )) {
        throw "Durable evidence reproducibility status is invalid."
    }
    foreach ($gateName in @("data_vault", "masking")) {
        ConvertTo-DataMasterTimestamp `
            -Value $Evidence.quality_gates.$gateName.validated_at `
            -Context "quality_gates.$gateName.validated_at" | Out-Null
    }
    if ($Evidence.quality_gates.reproducibility.status -eq "PASS") {
        ConvertTo-DataMasterTimestamp `
            -Value $Evidence.quality_gates.reproducibility.validated_at `
            -Context "quality_gates.reproducibility.validated_at" | Out-Null
    }
    Assert-DataMasterMarkerSet -Markers $Evidence.quality_gates.data_vault.markers `
        -Context "quality_gates.data_vault" -Required @(
            "DATA_VAULT_LINEAGE_STATUS=PASS",
            "DATA_VAULT_GOLD_LINEAGE_STATUS=PASS",
            "DATA_VAULT_QUALITY_GATE_STATUS=PASS"
        )
    Assert-DataMasterMarkerSet -Markers $Evidence.quality_gates.masking.markers `
        -Context "quality_gates.masking" -Required @(
            "MASKING_STATUS=PASS", "GOLD_PII_EXPOSURE_STATUS=PASS"
        )

    Assert-DataMasterObjectShape -Value $Evidence.technical_lineage `
        -Context "technical_lineage" -Required @("path", "status")
    if ($Evidence.technical_lineage.status -ne "PASS" -or
        $Evidence.technical_lineage.path -ne
        "bronze->raw_vault->business_vault_latest->gold") {
        throw "Durable evidence technical lineage is missing or invalid."
    }

    $requiredCounts = @(
        "bronze", "raw_vault_hubs", "raw_vault_links",
        "raw_vault_satellites", "gold"
    )
    Assert-DataMasterObjectShape -Value $Evidence.aggregate_counts `
        -Context "aggregate_counts" -Required $requiredCounts
    foreach ($countName in $requiredCounts) {
        Assert-DataMasterInteger -Value $Evidence.aggregate_counts.$countName `
            -Context "aggregate_counts.$countName" -Minimum 1
    }

    $rootProperties = if ($Evidence -is [System.Collections.IDictionary]) {
        @($Evidence.Keys | ForEach-Object { [string]$_ })
    }
    else {
        @($Evidence.PSObject.Properties | ForEach-Object { $_.Name })
    }
    if ("storage" -in $rootProperties) {
        Assert-DataMasterObjectShape -Value $Evidence.storage -Context "storage" `
            -Required @("business_vault_path", "gold_path", "gold_tables")
        Assert-DataMasterNonEmptyString -Value $Evidence.storage.business_vault_path `
            -Context "storage.business_vault_path"
        Assert-DataMasterNonEmptyString -Value $Evidence.storage.gold_path `
            -Context "storage.gold_path"

        $businessVaultPath = $Evidence.storage.business_vault_path.TrimEnd("/")
        $goldPath = $Evidence.storage.gold_path.TrimEnd("/")
        if ($businessVaultPath -eq $goldPath) {
            throw "Durable evidence Business Vault and Gold paths must be distinct."
        }

        Assert-DataMasterObjectShape -Value $Evidence.storage.gold_tables `
            -Context "storage.gold_tables" `
            -Required $script:DataMasterExpectedGoldTables
        foreach ($tableName in $script:DataMasterExpectedGoldTables) {
            $tablePath = if (
                $Evidence.storage.gold_tables -is [System.Collections.IDictionary]
            ) {
                $Evidence.storage.gold_tables[$tableName]
            }
            else {
                $Evidence.storage.gold_tables.$tableName
            }
            Assert-DataMasterNonEmptyString -Value $tablePath `
                -Context "storage.gold_tables.$tableName"
            if ($tablePath -ne "$goldPath/$tableName") {
                throw "Durable evidence Gold table path is invalid for '$tableName'."
            }
        }
    }

    Assert-DataMasterObjectShape -Value $Evidence.privacy -Context "privacy" `
        -Required @(
            "classification", "contains_pii", "contains_secrets",
            "contains_business_payload"
        )
    if ($Evidence.privacy.classification -ne "technical_aggregate_only") {
        throw "Durable evidence privacy classification is invalid."
    }
    foreach ($flag in @(
        "contains_pii", "contains_secrets", "contains_business_payload"
    )) {
        if ($Evidence.privacy.$flag -isnot [bool] -or $Evidence.privacy.$flag) {
            throw "Durable evidence privacy flag '$flag' must be boolean false."
        }
    }

    foreach ($risk in @($Evidence.operational_risks)) {
        Assert-DataMasterObjectShape -Value $risk -Context "operational_risks[]" `
            -Required @(
                "code", "component", "status", "observed_at", "blocking",
                "evidence_source"
            ) -Optional @("observed_exit_code", "observed_count")
        foreach ($field in @("code", "component", "status", "evidence_source")) {
            Assert-DataMasterNonEmptyString -Value $risk.$field `
                -Context "operational_risks[].$field"
        }
        ConvertTo-DataMasterTimestamp -Value $risk.observed_at `
            -Context "operational_risks[].observed_at" | Out-Null
        if ($risk.blocking -isnot [bool]) {
            throw "Durable evidence operational risk blocking flag must be boolean."
        }
        if ($risk.PSObject.Properties.Name -contains "observed_exit_code") {
            Assert-DataMasterInteger -Value $risk.observed_exit_code `
                -Context "operational_risks[].observed_exit_code"
        }
        if ($risk.PSObject.Properties.Name -contains "observed_count") {
            Assert-DataMasterInteger -Value $risk.observed_count `
                -Context "operational_risks[].observed_count"
        }
    }

    Assert-DataMasterSensitiveContent -Evidence $Evidence
    return $Evidence
}

function Read-DataMasterExecutionEvidence {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $resolved = [System.IO.Path]::GetFullPath($Path)
    if (-not [System.IO.File]::Exists($resolved)) {
        throw "Durable execution evidence file does not exist: $resolved"
    }
    try {
        $evidence = ConvertFrom-DataMasterJson -Json (
            [System.IO.File]::ReadAllText($resolved)
        )
    }
    catch {
        throw "Durable execution evidence is not valid JSON: $resolved"
    }
    return Assert-DataMasterExecutionEvidence -Evidence $evidence
}

function Write-DataMasterExecutionEvidence {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Evidence,

        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    Assert-DataMasterExecutionEvidence -Evidence $Evidence | Out-Null
    $resolved = [System.IO.Path]::GetFullPath($Path)
    $directory = [System.IO.Path]::GetDirectoryName($resolved)
    [System.IO.Directory]::CreateDirectory($directory) | Out-Null
    $json = $Evidence | ConvertTo-Json -Depth 20
    [System.IO.File]::WriteAllText(
        $resolved,
        $json + [Environment]::NewLine,
        (New-Object System.Text.UTF8Encoding($false))
    )
}
