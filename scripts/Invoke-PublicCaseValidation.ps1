[CmdletBinding()]
param(
    [ValidateSet("local-small")]
    [string]$RuntimeProfile = "local-small",

    [string]$ResultPath,

    [ValidateRange(1, 64)]
    [int]$MinimumCpuCount = 2,

    [ValidateRange(1, 256)]
    [int]$MinimumDockerMemoryGiB = 4,

    [ValidateRange(1, 1000)]
    [int]$MinimumMonitoringEvents = 5
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$validationStartedAt = (Get-Date).ToUniversalTime()
$repositoryRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$buildRoot = [System.IO.Path]::GetFullPath(
    (Join-Path $repositoryRoot "build\public-case-validation")
)
$defaultResultPath = Join-Path $buildRoot "case-validation.json"
$effectiveResultPath = $defaultResultPath
$sourceRevision = "UNKNOWN"
$finalExitCode = 1

function Test-PathWithin {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$CandidatePath,

        [Parameter(Mandatory = $true)]
        [string]$ParentPath
    )

    $candidate = [System.IO.Path]::GetFullPath($CandidatePath)
    $parent = [System.IO.Path]::GetFullPath($ParentPath)
    $pathComparison = [System.StringComparison]::Ordinal
    if ([System.IO.Path]::DirectorySeparatorChar -eq "\") {
        $pathComparison = [System.StringComparison]::OrdinalIgnoreCase
    }
    $separator = [System.IO.Path]::DirectorySeparatorChar
    $parentPrefix = $parent.TrimEnd(
        [char[]]@(
            [System.IO.Path]::DirectorySeparatorChar,
            [System.IO.Path]::AltDirectorySeparatorChar
        )
    ) + $separator

    return $candidate.Equals(
        $parent,
        $pathComparison
    ) -or $candidate.StartsWith(
        $parentPrefix,
        $pathComparison
    )
}

function Resolve-PublicCaseResultPath {
    [CmdletBinding()]
    param(
        [string]$RequestedPath,

        [Parameter(Mandatory = $true)]
        [string]$RootPath,

        [Parameter(Mandatory = $true)]
        [string]$AllowedBuildRoot
    )

    if ([string]::IsNullOrWhiteSpace($RequestedPath)) {
        $resolved = Join-Path $AllowedBuildRoot "case-validation.json"
    }
    elseif ([System.IO.Path]::IsPathRooted($RequestedPath)) {
        $resolved = [System.IO.Path]::GetFullPath($RequestedPath)
    }
    else {
        $resolved = [System.IO.Path]::GetFullPath(
            (Join-Path $RootPath $RequestedPath)
        )
    }

    if ([System.IO.Path]::GetExtension($resolved) -ne ".json") {
        throw "INVALID_RESULT_EXTENSION"
    }

    $privateEvidenceRoot = Join-Path $RootPath "evidence\runtime"
    $privateGovernanceRoot = Join-Path $RootPath "spdd"
    if (
        (Test-PathWithin -CandidatePath $resolved -ParentPath $privateEvidenceRoot) -or
        (Test-PathWithin -CandidatePath $resolved -ParentPath $privateGovernanceRoot)
    ) {
        throw "FORBIDDEN_RESULT_PATH"
    }

    if (
        (Test-PathWithin -CandidatePath $resolved -ParentPath $RootPath) -and
        -not (Test-PathWithin -CandidatePath $resolved -ParentPath $AllowedBuildRoot)
    ) {
        throw "TRACKED_RESULT_PATH"
    }

    return $resolved
}

function Assert-CommandAvailable {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "MISSING_COMMAND_$($Name.ToUpperInvariant())"
    }
}

function Assert-RequiredFiles {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$RootPath
    )

    $requiredFiles = @(
        "Dockerfile.spark",
        "requirements-spark.txt",
        "jobs\demo\run_presentation_demo.py",
        "jobs\observability\run_observability_smoke.py",
        "jobs\raw_vault\data_vault_quality_gate.py",
        "jobs\business_vault\run_gold_masking_smoke.py",
        "jobs\common\runtime_profiles.py",
        "config\privacy\data-classification.yml"
    )
    foreach ($relativePath in $requiredFiles) {
        if (-not (Test-Path -LiteralPath (Join-Path $RootPath $relativePath) -PathType Leaf)) {
            throw "MISSING_REQUIRED_FILE"
        }
    }
}

function Assert-CleanPublicWorktree {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$RootPath
    )

    $statusOutput = @(
        & git -C $RootPath status --porcelain --untracked-files=all 2>&1
    )
    if ($LASTEXITCODE -ne 0) {
        throw "GIT_STATUS_UNAVAILABLE"
    }
    if (@($statusOutput).Count -gt 0) {
        throw "PUBLIC_WORKTREE_NOT_CLEAN"
    }
}

function Assert-ResultPathWritable {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetPath
    )

    $directory = Split-Path -Parent $TargetPath
    [System.IO.Directory]::CreateDirectory($directory) | Out-Null
    $probePath = Join-Path $directory (
        ".case-validation-write-probe-" + [Guid]::NewGuid().ToString("N")
    )
    try {
        [System.IO.File]::WriteAllText(
            $probePath,
            "probe",
            (New-Object System.Text.UTF8Encoding($false))
        )
    }
    catch {
        throw "RESULT_PATH_NOT_WRITABLE"
    }
    finally {
        if (Test-Path -LiteralPath $probePath) {
            Remove-Item -LiteralPath $probePath -Force -ErrorAction SilentlyContinue
        }
    }
}

function Get-DockerCapacity {
    [CmdletBinding()]
    param()

    $output = @(
        & docker info --format "{{.OSType}}|{{.NCPU}}|{{.MemTotal}}" 2>&1
    )
    if ($LASTEXITCODE -ne 0) {
        throw "DOCKER_DAEMON_UNAVAILABLE"
    }

    $capacityLine = $null
    foreach ($line in $output) {
        $candidate = $line.ToString().Trim()
        if ($candidate -match "^[^|]+\|[0-9]+\|[0-9]+$") {
            $capacityLine = $candidate
        }
    }
    if ($null -eq $capacityLine) {
        throw "DOCKER_CAPACITY_UNAVAILABLE"
    }

    $parts = $capacityLine.Split("|")
    return [ordered]@{
        os_type = $parts[0].ToLowerInvariant()
        cpu_count = [int]$parts[1]
        memory_bytes = [int64]$parts[2]
    }
}

function Get-RequiredProperty {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$InputObject,

        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $property = $InputObject.PSObject.Properties[$Name]
    if ($null -eq $property) {
        throw "INVALID_PRESENTATION_PAYLOAD"
    }
    return $property.Value
}

function Get-FailureValueCount {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$Value
    )

    if ($null -eq $Value) {
        return 0
    }
    if ($Value -is [string]) {
        return [int](-not [string]::IsNullOrWhiteSpace($Value))
    }
    if ($Value -is [bool]) {
        return [int]$Value
    }
    if ($Value -is [System.Management.Automation.PSCustomObject]) {
        $count = 0
        foreach ($property in $Value.PSObject.Properties) {
            $count += Get-FailureValueCount $property.Value
        }
        return $count
    }
    if ($Value -is [System.Collections.IDictionary]) {
        $count = 0
        foreach ($key in $Value.Keys) {
            $count += Get-FailureValueCount $Value[$key]
        }
        return $count
    }
    if ($Value -is [System.Collections.IEnumerable]) {
        $count = 0
        foreach ($item in $Value) {
            $count += Get-FailureValueCount $item
        }
        return $count
    }
    if ($Value -is [ValueType]) {
        return [int]([double]$Value -ne 0)
    }
    return 1
}

function New-InitialCaseResult {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Profile,

        [Parameter(Mandatory = $true)]
        [string]$Revision
    )

    return [ordered]@{
        schema_version = 1
        status = "FAILURE"
        runtime_profile = $Profile
        expected_runtime_profile = $Profile
        execution_scope = "local_direct_validation"
        source_revision = $Revision
        batch_id = "UNAVAILABLE"
        started_at = $validationStartedAt.ToString("o")
        finished_at = $validationStartedAt.ToString("o")
        duration_seconds = 0
        checks = [ordered]@{
            pipeline = [ordered]@{
                status = "NOT_RUN"
                runner_status = "UNKNOWN"
                runner_exit_code = -1
                stages = [ordered]@{}
                layer_counts = [ordered]@{}
            }
            data_vault = [ordered]@{
                status = "NOT_RUN"
                gate_status = "UNKNOWN"
            }
            masking = [ordered]@{
                status = "NOT_RUN"
                failure_count = 0
            }
            observability = [ordered]@{
                status = "NOT_RUN"
                event_count = 0
                minimum_event_count = $MinimumMonitoringEvents
            }
            secret_scan = [ordered]@{
                status = "NOT_RUN"
                finding_count = 0
            }
            overall = [ordered]@{
                status = "FAIL"
            }
        }
        failed_checks = @("wrapper")
    }
}

function New-SanitizedCaseResult {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$PresentationPayload,

        [Parameter(Mandatory = $true)]
        [string]$RequestedProfile,

        [Parameter(Mandatory = $true)]
        [string]$Revision,

        [Parameter(Mandatory = $true)]
        [int]$RunnerExitCode,

        [Parameter(Mandatory = $true)]
        [int]$RequiredMonitoringEvents
    )

    $runnerStatus = [string](Get-RequiredProperty $PresentationPayload "status")
    $actualProfile = [string](Get-RequiredProperty $PresentationPayload "runtime_profile")
    $expectedProfile = [string](
        Get-RequiredProperty $PresentationPayload "expected_runtime_profile"
    )
    $executionScope = [string](
        Get-RequiredProperty $PresentationPayload "execution_scope"
    )
    $batchId = [string](Get-RequiredProperty $PresentationPayload "batch_id")
    $stageResults = Get-RequiredProperty $PresentationPayload "stage_results"
    $layerCountsPayload = Get-RequiredProperty $PresentationPayload "layer_counts"

    $stageNames = @(
        "bronze",
        "raw_hubs",
        "raw_links",
        "raw_satellites",
        "gold"
    )
    $stages = [ordered]@{}
    foreach ($stageName in $stageNames) {
        $stage = Get-RequiredProperty $stageResults $stageName
        $stages[$stageName] = [string](Get-RequiredProperty $stage "status")
    }

    $layerNames = @(
        "bronze",
        "raw_vault_hubs",
        "raw_vault_links",
        "raw_vault_satellites",
        "gold"
    )
    $layerCounts = [ordered]@{}
    foreach ($layerName in $layerNames) {
        $layerCounts[$layerName] = [long](
            Get-RequiredProperty $layerCountsPayload $layerName
        )
    }

    $failedChecks = New-Object System.Collections.Generic.List[string]
    $profilePassed = (
        $actualProfile -eq $RequestedProfile -and
        $expectedProfile -eq $RequestedProfile -and
        $executionScope -eq "local_direct_validation"
    )
    if (-not $profilePassed) {
        $failedChecks.Add("runtime_profile")
    }
    if ($RunnerExitCode -ne 0) {
        $failedChecks.Add("runner_exit_code")
    }

    $pipelinePassed = $runnerStatus -eq "SUCCESS"
    foreach ($stageName in $stageNames) {
        if ($stages[$stageName] -ne "SUCCESS") {
            $pipelinePassed = $false
        }
    }
    foreach ($layerName in $layerNames) {
        if ($layerCounts[$layerName] -le 0) {
            $pipelinePassed = $false
        }
    }
    if (-not $pipelinePassed) {
        $failedChecks.Add("pipeline")
    }

    $dataVaultStage = Get-RequiredProperty $stageResults "data_vault_quality_gate"
    $dataVaultStageStatus = [string](
        Get-RequiredProperty $dataVaultStage "status"
    )
    $dataVaultPayload = Get-RequiredProperty (
        $PresentationPayload
    ) "data_vault_quality_gate"
    $dataVaultGateStatus = [string](
        Get-RequiredProperty $dataVaultPayload "status"
    )
    $dataVaultPassed = (
        $dataVaultStageStatus -eq "SUCCESS" -and
        $dataVaultGateStatus -eq "PASS"
    )
    if (-not $dataVaultPassed) {
        $failedChecks.Add("data_vault")
    }

    $validationFailures = Get-RequiredProperty (
        $PresentationPayload
    ) "validation_failures"
    $maskingFailures = Get-RequiredProperty (
        $validationFailures
    ) "masking_failures"
    $maskingFailureFields = @(
        "masking_sample_failures",
        "forbidden_columns",
        "raw_pattern_hits",
        "protected_check_failures",
        "cliente_check_failures",
        "risco_check_failures"
    )
    $maskingFailureCount = 0
    foreach ($fieldName in $maskingFailureFields) {
        $maskingFailureCount += Get-FailureValueCount (
            Get-RequiredProperty $maskingFailures $fieldName
        )
    }
    $maskingPassed = $maskingFailureCount -eq 0
    if (-not $maskingPassed) {
        $failedChecks.Add("masking")
    }

    $monitoring = Get-RequiredProperty $PresentationPayload "monitoring"
    $monitoringEventCount = [long](Get-RequiredProperty $monitoring "rows")
    $observabilityPassed = $monitoringEventCount -ge $RequiredMonitoringEvents
    if (-not $observabilityPassed) {
        $failedChecks.Add("observability")
    }

    $maskingValidation = Get-RequiredProperty (
        $PresentationPayload
    ) "masking_validation"
    $secretFindings = Get-RequiredProperty $maskingValidation "secret_findings"
    $secretFindingCount = Get-FailureValueCount $secretFindings
    $maskingSecretFindings = Get-RequiredProperty (
        $maskingFailures
    ) "secret_findings"
    $secretFindingCount += Get-FailureValueCount $maskingSecretFindings
    $secretScanPassed = $secretFindingCount -eq 0
    if (-not $secretScanPassed) {
        $failedChecks.Add("secret_scan")
    }

    $overallPassed = (
        $profilePassed -and
        $RunnerExitCode -eq 0 -and
        $pipelinePassed -and
        $dataVaultPassed -and
        $maskingPassed -and
        $observabilityPassed -and
        $secretScanPassed
    )
    $finishedAt = (Get-Date).ToUniversalTime()

    return [ordered]@{
        schema_version = 1
        status = $(if ($overallPassed) { "SUCCESS" } else { "FAILURE" })
        runtime_profile = $actualProfile
        expected_runtime_profile = $expectedProfile
        execution_scope = $executionScope
        source_revision = $Revision
        batch_id = $batchId
        started_at = $validationStartedAt.ToString("o")
        finished_at = $finishedAt.ToString("o")
        duration_seconds = [math]::Round(
            ($finishedAt - $validationStartedAt).TotalSeconds,
            3
        )
        checks = [ordered]@{
            pipeline = [ordered]@{
                status = $(if ($pipelinePassed) { "PASS" } else { "FAIL" })
                runner_status = $runnerStatus
                runner_exit_code = $RunnerExitCode
                stages = $stages
                layer_counts = $layerCounts
            }
            data_vault = [ordered]@{
                status = $(if ($dataVaultPassed) { "PASS" } else { "FAIL" })
                gate_status = $dataVaultGateStatus
            }
            masking = [ordered]@{
                status = $(if ($maskingPassed) { "PASS" } else { "FAIL" })
                failure_count = $maskingFailureCount
            }
            observability = [ordered]@{
                status = $(if ($observabilityPassed) { "PASS" } else { "FAIL" })
                event_count = $monitoringEventCount
                minimum_event_count = $RequiredMonitoringEvents
            }
            secret_scan = [ordered]@{
                status = $(if ($secretScanPassed) { "PASS" } else { "FAIL" })
                finding_count = $secretFindingCount
            }
            overall = [ordered]@{
                status = $(if ($overallPassed) { "PASS" } else { "FAIL" })
            }
        }
        failed_checks = @($failedChecks)
    }
}

function Write-SanitizedCaseResult {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [System.Collections.IDictionary]$CaseResult,

        [Parameter(Mandatory = $true)]
        [string]$TargetPath
    )

    $directory = Split-Path -Parent $TargetPath
    [System.IO.Directory]::CreateDirectory($directory) | Out-Null
    $json = $CaseResult | ConvertTo-Json -Depth 20
    [System.IO.File]::WriteAllText(
        $TargetPath,
        $json + [Environment]::NewLine,
        (New-Object System.Text.UTF8Encoding($false))
    )
}

$normalizedResult = New-InitialCaseResult `
    -Profile $RuntimeProfile `
    -Revision $sourceRevision

try {
    if ($PSVersionTable.PSVersion.Major -lt 5) {
        throw "UNSUPPORTED_POWERSHELL_VERSION"
    }

    $effectiveResultPath = Resolve-PublicCaseResultPath `
        -RequestedPath $ResultPath `
        -RootPath $repositoryRoot `
        -AllowedBuildRoot $buildRoot
    Assert-ResultPathWritable -TargetPath $effectiveResultPath
    Assert-CommandAvailable -Name "git"
    Assert-CommandAvailable -Name "docker"
    Assert-RequiredFiles -RootPath $repositoryRoot

    $revisionOutput = @(
        & git -C $repositoryRoot rev-parse HEAD 2>&1
    )
    if ($LASTEXITCODE -ne 0 -or $revisionOutput.Count -ne 1) {
        throw "GIT_REVISION_UNAVAILABLE"
    }
    $sourceRevision = $revisionOutput[0].ToString().Trim()
    if ($sourceRevision -notmatch "^[0-9a-fA-F]{40}$") {
        throw "GIT_REVISION_INVALID"
    }
    $normalizedResult["source_revision"] = $sourceRevision
    Assert-CleanPublicWorktree -RootPath $repositoryRoot

    $dockerCapacity = Get-DockerCapacity
    if ($dockerCapacity.os_type -ne "linux") {
        throw "DOCKER_LINUX_ENGINE_REQUIRED"
    }
    if ($dockerCapacity.cpu_count -lt $MinimumCpuCount) {
        throw "INSUFFICIENT_DOCKER_CPU"
    }
    $dockerMemoryGiB = [math]::Floor($dockerCapacity.memory_bytes / 1GB)
    if ($dockerMemoryGiB -lt $MinimumDockerMemoryGiB) {
        throw "INSUFFICIENT_DOCKER_MEMORY"
    }

    $imageTag = "public-validation-" + $sourceRevision.Substring(0, 12).ToLowerInvariant()
    $sparkImage = "data-master-spark-jobs:$imageTag"
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $buildOutput = @(
            & docker build `
                --file (Join-Path $repositoryRoot "Dockerfile.spark") `
                --tag $sparkImage `
                $repositoryRoot 2>&1
        )
        $buildExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    foreach ($line in $buildOutput) {
        Write-Output (
            $line.ToString().Replace($repositoryRoot, "<repository>")
        )
    }
    if ($buildExitCode -ne 0) {
        throw "SPARK_IMAGE_BUILD_FAILED"
    }
    Write-Output "PUBLIC_CASE_SPARK_BUILD_STATUS=SUCCESS"

    $batchId = "public_case_" + (
        (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmss")
    )
    $mountArgument = "type=bind,source=$repositoryRoot,target=/repo,readonly"
    $dockerArguments = @(
        "run",
        "--rm",
        "--user", "65534:65534",
        "--entrypoint", "python3",
        "--env", "PYTHONDONTWRITEBYTECODE=1",
        "--env", "SPARK_LOCAL_IP=127.0.0.1",
        "--env", "SPARK_USER=nobody",
        "--env", "JAVA_TOOL_OPTIONS=-XX:-UseContainerSupport -Duser.home=/tmp",
        "--env", "SPARK_IVY_DIR=/tmp/.ivy2",
        "--env", "SPARK_JARS_PACKAGES=",
        "--mount", $mountArgument,
        "--workdir", "/tmp",
        $sparkImage,
        "-B",
        "/repo/jobs/demo/run_presentation_demo.py",
        "--runtime-profile", $RuntimeProfile,
        "--expected-runtime-profile", $RuntimeProfile,
        "--batch-id", $batchId
    )

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $demoOutput = @(& docker @dockerArguments 2>&1)
        $demoExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    Write-Output "PUBLIC_CASE_RUNNER_EXIT_CODE=$demoExitCode"

    $markerPrefix = "PRESENTATION_DEMO_RESULT="
    $markerLines = @(
        $demoOutput |
            ForEach-Object { $_.ToString() } |
            Where-Object { $_.StartsWith($markerPrefix) }
    )
    if ($markerLines.Count -ne 1) {
        throw "PRESENTATION_MARKER_COUNT_INVALID"
    }

    $presentationJson = $markerLines[0].Substring($markerPrefix.Length)
    try {
        $presentationPayload = $presentationJson | ConvertFrom-Json
    }
    catch {
        throw "PRESENTATION_PAYLOAD_INVALID_JSON"
    }

    $normalizedResult = New-SanitizedCaseResult `
        -PresentationPayload $presentationPayload `
        -RequestedProfile $RuntimeProfile `
        -Revision $sourceRevision `
        -RunnerExitCode $demoExitCode `
        -RequiredMonitoringEvents $MinimumMonitoringEvents

    if ($normalizedResult["status"] -ne "SUCCESS") {
        throw "PUBLIC_CASE_GATE_FAILED"
    }
    $finalExitCode = 0
}
catch {
    $failureCode = $_.Exception.Message
    if ($failureCode -notmatch "^[A-Z0-9_]+$") {
        $failureCode = "PUBLIC_CASE_VALIDATION_FAILED"
    }
    $normalizedResult["status"] = "FAILURE"
    $normalizedResult["checks"]["overall"]["status"] = "FAIL"
    $existingFailures = @($normalizedResult["failed_checks"])
    $normalizedResult["failed_checks"] = @(
        $existingFailures + $failureCode.ToLowerInvariant() |
            Select-Object -Unique
    )
    $normalizedResult["finished_at"] = (Get-Date).ToUniversalTime().ToString("o")
    $normalizedResult["duration_seconds"] = [math]::Round(
        ((Get-Date).ToUniversalTime() - $validationStartedAt).TotalSeconds,
        3
    )
    Write-Warning "Public case validation failed with code $failureCode."
    $finalExitCode = 1
}
finally {
    try {
        Write-SanitizedCaseResult `
            -CaseResult $normalizedResult `
            -TargetPath $effectiveResultPath
    }
    catch {
        $normalizedResult["status"] = "FAILURE"
        $finalExitCode = 1
        Write-Warning "Public case validation could not write its sanitized result."
    }

    Write-Output "CASE_VALIDATION_STATUS=$($normalizedResult["status"])"
}

exit $finalExitCode
