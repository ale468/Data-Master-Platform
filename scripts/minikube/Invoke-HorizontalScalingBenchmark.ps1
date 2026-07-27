[CmdletBinding()]
param(
    [string]$Profile,

    [ValidateSet(
        "single-node-application-scale-out",
        "multi-node-scale-out"
    )]
    [string]$Topology = "single-node-application-scale-out",

    [string]$SparkImageRepository = "data-master-spark-jobs",

    [ValidateRange(600, 10800)]
    [int]$ApplicationTimeoutSeconds = 2700,

    [string]$OutputPath,

    [switch]$KeepRunResources
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")

$script:HorizontalExitPass = 0
$script:HorizontalExitInconclusive = 2
$script:HorizontalExitFail = 3
$script:HorizontalExitHarnessError = 4
$script:HorizontalExitBlocked = 5
$script:ProfileCreatedByRun = $false
$script:FinalExitCode = $script:HorizontalExitHarnessError
$script:SparkImageForPython = ""
$script:HorizontalTemporaryRoot = ""
$script:LastHorizontalPythonExitCode = 0
$script:RegistryContainer = ""

function Throw-HorizontalBlocked {
    param([Parameter(Mandatory = $true)][string]$Message)
    throw "[BLOCKED] $Message"
}

function Throw-HorizontalFail {
    param([Parameter(Mandatory = $true)][string]$Message)
    throw "[FAIL] $Message"
}

function Save-HorizontalTerminalResult {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Result,

        [Parameter(Mandatory = $true)]
        [string]$Reason,

        [Parameter(Mandatory = $true)]
        [string]$Destination,

        [Parameter(Mandatory = $true)]
        [string]$BenchmarkId,

        [string]$GitSha,

        [string]$ImageDigest
    )

    $safeReason = ($Reason -replace "[\r\n]+", " ").Trim()
    if ($safeReason.Length -gt 300) {
        $safeReason = $safeReason.Substring(0, 300)
    }
    $payload = [ordered]@{
        schema_version = 1
        benchmark_kind = "static-horizontal-spark-scale-out"
        change_id = "DM-RUN-004"
        benchmark_id = $BenchmarkId
        result = $Result
        reason = $safeReason
        git_sha = $GitSha
        image_digest = $ImageDigest
        topology = $Topology
        generated_at = (Get-Date).ToUniversalTime().ToString("o")
        limitations = @(
            "Minikube is a local environment.",
            "No production, SLA, cost, sizing, cloud, or autoscaling claim is made."
        )
    }
    $parent = Split-Path -Parent $Destination
    if ($parent) {
        [System.IO.Directory]::CreateDirectory($parent) | Out-Null
    }
    [System.IO.File]::WriteAllText(
        $Destination,
        ($payload | ConvertTo-Json -Depth 20) + [Environment]::NewLine,
        (New-Object System.Text.UTF8Encoding($false))
    )
}

function Invoke-HorizontalPython {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,

        [int[]]$AllowedExitCodes = @(0)
    )

    if (-not $script:SparkImageForPython) {
        throw "Horizontal Python image is not initialized."
    }
    $root = Get-DataMasterRepositoryRoot
    $dockerArguments = @(
        "run", "--rm",
        "--entrypoint", "python3",
        "--mount", "type=bind,source=$root,target=/repo,readonly",
        "--mount", (
            "type=bind,source=$script:HorizontalTemporaryRoot,target=/work"
        ),
        "--workdir", "/repo",
        $script:SparkImageForPython,
        "-B"
    ) + $Arguments
    & docker @dockerArguments
    $script:LastHorizontalPythonExitCode = $LASTEXITCODE
    if ($AllowedExitCodes -notcontains $script:LastHorizontalPythonExitCode) {
        throw (
            "Horizontal Python container failed with exit code " +
            "$script:LastHorizontalPythonExitCode."
        )
    }
}

function Get-HorizontalPodSnapshot {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RunId
    )

    $json = @(
        Invoke-DataMasterNative -FilePath "kubectl" -CaptureOutput -Arguments @(
            "get", "pods",
            "--namespace", "data-platform",
            "--selector", "data-master.io/run-id=$RunId",
            "--output", "json"
        )
    ) -join [Environment]::NewLine
    $document = $json | ConvertFrom-Json
    $snapshot = @()
    foreach ($pod in @($document.items)) {
        $node = ""
        if ($null -ne $pod.spec.PSObject.Properties["nodeName"]) {
            $node = [string]$pod.spec.nodeName
        }
        $podIp = ""
        if ($null -ne $pod.status.PSObject.Properties["podIP"]) {
            $podIp = [string]$pod.status.podIP
        }
        $snapshot += [pscustomobject]@{
            name = [string]$pod.metadata.name
            role = [string]$pod.metadata.labels."data-master.io/spark-role"
            status = [string]$pod.status.phase
            node = $node
            pod_ip = $podIp
        }
    }
    return $snapshot
}

function Update-HorizontalPodInventory {
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Inventory,

        [Parameter(Mandatory = $true)]
        [string]$RunId
    )

    foreach ($pod in @(Get-HorizontalPodSnapshot -RunId $RunId)) {
        $Inventory[$pod.name] = $pod
    }
}

function Get-HorizontalMinioObservation {
    $json = @(
        Invoke-DataMasterNative -FilePath "kubectl" -CaptureOutput -Arguments @(
            "get", "pods",
            "--namespace", "data-platform",
            "--selector", "app.kubernetes.io/instance=minio",
            "--output", "json"
        )
    ) -join [Environment]::NewLine
    $document = $json | ConvertFrom-Json
    $pods = @($document.items)
    if ($pods.Count -ne 1) {
        Throw-HorizontalFail (
            "Expected exactly one MinIO pod; observed $($pods.Count)."
        )
    }
    $pod = $pods[0]
    $statuses = @(
        $pod.status.containerStatuses |
            Where-Object { $_.name -eq "minio" }
    )
    if ($statuses.Count -ne 1) {
        Throw-HorizontalFail "MinIO container status is unavailable."
    }
    $container = $statuses[0]
    $ready = (
        [string]$pod.status.phase -eq "Running" -and
        [bool]$container.ready
    )
    $restartCount = [int]$container.restartCount
    if (-not $ready -or $restartCount -ne 0) {
        Throw-HorizontalFail (
            "MinIO must remain ready with zero restarts; " +
            "ready=$ready restarts=$restartCount."
        )
    }
    return [pscustomobject]@{
        status = "PASS"
        pod_count = 1
        restart_count = $restartCount
    }
}

function Invoke-HorizontalApplication {
    param(
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Run,

        [Parameter(Mandatory = $true)]
        [string]$BenchmarkId,

        [Parameter(Mandatory = $true)]
        [string]$GitSha,

        [Parameter(Mandatory = $true)]
        [string]$ImageReference,

        [Parameter(Mandatory = $true)]
        [string]$ImageDigest,

        [Parameter(Mandatory = $true)]
        [string]$WorkingDirectory
    )

    $root = Get-DataMasterRepositoryRoot
    $manifestPath = Join-Path $WorkingDirectory "$($Run.run_id).yaml"
    $containerManifestPath = "/work/$($Run.run_id).yaml"
    $workloadPath = Join-Path $WorkingDirectory "$($Run.run_id).workload.json"
    $observationPath = Join-Path $WorkingDirectory (
        "$($Run.run_id).observation.json"
    )
    Get-HorizontalMinioObservation | Out-Null
    $renderArguments = @(
        "/repo/jobs/scalability/run_horizontal_scalability_benchmark.py",
        "--render-application",
        "--profile", [string]$Run.profile_id,
        "--benchmark-id", $BenchmarkId,
        "--run-id", [string]$Run.run_id,
        "--batch-id", [string]$Run.batch_id,
        "--git-sha", $GitSha,
        "--image", $ImageReference,
        "--image-digest", $ImageDigest,
        "--topology", $Topology,
        "--measurement-kind", [string]$Run.measurement_kind,
        "--repetition", [string]$Run.repetition,
        "--output", $containerManifestPath
    )
    Invoke-HorizontalPython -Arguments $renderArguments
    Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
        "apply", "--filename", $manifestPath
    )

    $applicationName = [string]$Run.application_name
    $inventory = @{}
    $state = "SUBMITTED"
    $terminalFailureStates = @(
        "FAILED",
        "FAILING",
        "SUBMISSION_FAILED",
        "INVALIDATING"
    )
    $deadline = (Get-Date).AddSeconds($ApplicationTimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        Update-HorizontalPodInventory -Inventory $inventory -RunId $Run.run_id
        $stateOutput = & kubectl get sparkapplication $applicationName `
            --namespace data-platform `
            --output "jsonpath={.status.applicationState.state}" 2>$null
        if ($LASTEXITCODE -eq 0 -and $stateOutput) {
            $state = ($stateOutput -join "").Trim()
        }
        if ($state -eq "COMPLETED") {
            break
        }
        if ($state -in $terminalFailureStates) {
            break
        }
        Start-Sleep -Seconds 3
    }
    Update-HorizontalPodInventory -Inventory $inventory -RunId $Run.run_id
    if (
        $state -ne "COMPLETED" -and
        $state -notin $terminalFailureStates
    ) {
        Throw-HorizontalFail (
            "SparkApplication $applicationName timed out in state $state."
        )
    }
    $sharedStorage = Get-HorizontalMinioObservation

    $driverPods = @(
        $inventory.Values | Where-Object { $_.role -eq "driver" }
    )
    $executorPods = @(
        $inventory.Values | Where-Object { $_.role -eq "executor" }
    )
    $requested = [int](
        & kubectl get sparkapplication $applicationName `
            --namespace data-platform `
            --output "jsonpath={.spec.executor.instances}"
    )
    if ($driverPods.Count -ne 1) {
        Throw-HorizontalFail (
            "Expected one driver pod; observed $($driverPods.Count)."
        )
    }
    if ($executorPods.Count -ne $requested) {
        Throw-HorizontalFail (
            "Expected $requested executor pods; observed $($executorPods.Count)."
        )
    }

    $driverName = [string]$driverPods[0].name
    $driverLogs = @(
        Invoke-DataMasterNative -FilePath "kubectl" -CaptureOutput -Arguments @(
            "logs", $driverName, "--namespace", "data-platform"
        )
    )
    $markerLines = @(
        $driverLogs |
            Where-Object {
                ([string]$_).StartsWith("HORIZONTAL_WORKLOAD_RESULT=")
            }
    )
    if ($markerLines.Count -ne 1) {
        Throw-HorizontalFail (
            "Driver did not emit exactly one horizontal workload result."
        )
    }
    $payloadText = ([string]$markerLines[0]).Substring(
        "HORIZONTAL_WORKLOAD_RESULT=".Length
    )
    $payload = $payloadText | ConvertFrom-Json
    [System.IO.File]::WriteAllText(
        $workloadPath,
        ($payload | ConvertTo-Json -Depth 100) + [Environment]::NewLine,
        (New-Object System.Text.UTF8Encoding($false))
    )

    $observation = [ordered]@{
        schema_version = 1
        benchmark_id = $BenchmarkId
        run_id = [string]$Run.run_id
        profile_id = [string]$Run.profile_id
        application_name = $applicationName
        application_status = $state
        executors_requested = $requested
        driver_pods = @($driverPods)
        executor_pods = @($executorPods)
        shared_storage = $sharedStorage
    }
    [System.IO.File]::WriteAllText(
        $observationPath,
        ($observation | ConvertTo-Json -Depth 30) + [Environment]::NewLine,
        (New-Object System.Text.UTF8Encoding($false))
    )

    if ($state -ne "COMPLETED" -or [string]$payload.status -ne "PASS") {
        $diagnostics = @()
        if (
            $null -ne $payload.PSObject.Properties["validation_failures"]
        ) {
            $diagnostics += @($payload.validation_failures)
        }
        if ($null -ne $payload.PSObject.Properties["failure"]) {
            $diagnostics += (
                "workload_failure_type=" + [string]$payload.failure.type
            )
        }
        $diagnosticText = "no_sanitized_driver_diagnostic"
        if ($diagnostics.Count -gt 0) {
            $diagnosticText = (@($diagnostics) -join ",")
        }
        Throw-HorizontalFail (
            "SparkApplication $applicationName ended in state $state; " +
            "diagnostic=$diagnosticText."
        )
    }

    Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
        "delete", "sparkapplication", $applicationName,
        "--namespace", "data-platform",
        "--wait=true",
        "--timeout=120s"
    )
    Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
        "delete", "pod",
        "--namespace", "data-platform",
        "--selector", "data-master.io/run-id=$($Run.run_id)",
        "--ignore-not-found=true",
        "--wait=true",
        "--timeout=120s"
    )

    return [pscustomobject]@{
        measurement_kind = [string]$Run.measurement_kind
        workload = $workloadPath
        observation = $observationPath
    }
}

$root = Get-DataMasterRepositoryRoot
$gitSha = ""
$imageDigest = ""
$timestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddHHmmss")
$benchmarkId = "hscale-$timestamp"
if (-not $Profile) {
    $Profile = "data-master-horizontal-$timestamp"
}
if (-not $OutputPath) {
    $OutputPath = Join-Path $root (
        "tests\evidence\horizontal-scaling\$benchmarkId.json"
    )
}
elseif (-not [System.IO.Path]::IsPathRooted($OutputPath)) {
    $OutputPath = Join-Path $root $OutputPath
}
$OutputPath = [System.IO.Path]::GetFullPath($OutputPath)
$temporaryRoot = Join-Path (
    [System.IO.Path]::GetTempPath()
) ("data-master-horizontal-" + [Guid]::NewGuid().ToString("N"))
[System.IO.Directory]::CreateDirectory($temporaryRoot) | Out-Null
$script:HorizontalTemporaryRoot = $temporaryRoot

try {
    Assert-DataMasterSafeProfile -Profile $Profile
    foreach ($command in @("git", "docker", "minikube", "kubectl", "helm")) {
        if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
            Throw-HorizontalBlocked "Required command is unavailable: $command"
        }
    }
    $changes = @(& git -C $root status --porcelain)
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to inspect Git worktree."
    }
    if ($changes.Count -ne 0) {
        Throw-HorizontalBlocked (
            "Git worktree must be clean before image construction."
        )
    }
    $gitSha = (& git -C $root rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $gitSha -notmatch "^[0-9a-f]{40}$") {
        throw "Unable to resolve exact Git SHA."
    }

    $profileInventoryText = @(
        Invoke-DataMasterNative -FilePath "minikube" -CaptureOutput -Arguments @(
            "profile", "list", "--output=json"
        )
    ) -join [Environment]::NewLine
    $profileInventory = ConvertFrom-DataMasterMinikubeProfileInventory `
        -JsonText $profileInventoryText
    if ($profileInventory -contains $Profile) {
        Throw-HorizontalBlocked (
            "Target Minikube profile already exists and will not be modified."
        )
    }

    $cpuCount = [Environment]::ProcessorCount
    if ($cpuCount -lt 4) {
        Throw-HorizontalBlocked (
            "At least four logical CPUs are required; observed $cpuCount."
        )
    }
    $memoryBytes = (
        Get-CimInstance Win32_ComputerSystem
    ).TotalPhysicalMemory
    $memoryGiB = [math]::Floor($memoryBytes / 1GB)
    if ($memoryGiB -lt 16) {
        Throw-HorizontalBlocked (
            "At least 16 GiB host memory is required; observed $memoryGiB GiB."
        )
    }
    $dockerMemory = [int64](
        (
            Invoke-DataMasterNative -FilePath "docker" -CaptureOutput -Arguments @(
                "info", "--format", "{{.MemTotal}}"
            )
        ) | Select-Object -Last 1
    )
    $dockerMemoryGiB = [math]::Floor($dockerMemory / 1GB)
    if ($dockerMemoryGiB -lt 11) {
        Throw-HorizontalBlocked (
            "At least 11 GiB Docker memory is required; observed $dockerMemoryGiB GiB."
        )
    }
    $freeDiskGiB = [math]::Floor((Get-PSDrive -Name C).Free / 1GB)
    if ($freeDiskGiB -lt 45) {
        Throw-HorizontalBlocked (
            "At least 45 GiB free disk is required; observed $freeDiskGiB GiB."
        )
    }
    if ($Topology -eq "multi-node-scale-out") {
        Throw-HorizontalBlocked (
            "Multi-node execution requires an explicitly provisioned multi-node profile."
        )
    }

    $registryListener = Get-NetTCPConnection -State Listen -LocalPort 5000 `
        -ErrorAction SilentlyContinue
    if ($registryListener) {
        Throw-HorizontalBlocked (
            "Local registry port 5000 is already in use and will not be modified."
        )
    }
    $script:RegistryContainer = "dm-horizontal-registry-$timestamp"
    Invoke-DataMasterNative -FilePath "docker" -Arguments @(
        "run", "--detach",
        "--name", $script:RegistryContainer,
        "--publish", "127.0.0.1:5000:5000",
        "registry:2.8.3"
    ) | Out-Null

    $imageTag = "git-$gitSha"
    $sparkImage = "${SparkImageRepository}:$imageTag"
    Invoke-DataMasterNative -FilePath "docker" -Arguments @(
        "build",
        "--file", (Join-Path $root "Dockerfile.spark"),
        "--tag", $sparkImage,
        "--label", "io.data-master.git-sha=$gitSha",
        $root
    )
    $script:SparkImageForPython = $sparkImage
    $imageId = (
        (
            Invoke-DataMasterNative -FilePath "docker" -CaptureOutput -Arguments @(
                "image", "inspect", $sparkImage,
                "--format", "{{.Id}}"
            )
        ) | Select-Object -Last 1
    ).Trim()
    if ($imageId -notmatch "^sha256:[0-9a-f]{64}$") {
        throw "Docker did not return an immutable sha256 image ID."
    }
    $registryTaggedImage = "localhost:5000/${SparkImageRepository}:$imageTag"
    Invoke-DataMasterNative -FilePath "docker" -Arguments @(
        "tag", $sparkImage, $registryTaggedImage
    )
    Invoke-DataMasterNative -FilePath "docker" -Arguments @(
        "push", $registryTaggedImage
    )
    $repositoryDigests = @(
        @(
            Invoke-DataMasterNative -FilePath "docker" -CaptureOutput -Arguments @(
                "image", "inspect", $registryTaggedImage,
                "--format", "{{range .RepoDigests}}{{println .}}{{end}}"
            )
        ) | Where-Object {
            ([string]$_).Trim().StartsWith(
                "localhost:5000/$SparkImageRepository@sha256:"
            )
        }
    )
    if ($repositoryDigests.Count -ne 1) {
        throw "Local registry did not return exactly one repository digest."
    }
    $imageDigest = ([string]$repositoryDigests[0]).Trim().Split("@", 2)[1]
    if ($imageDigest -notmatch "^sha256:[0-9a-f]{64}$") {
        throw "Local registry did not return an immutable manifest digest."
    }
    $imageReference = (
        "host.minikube.internal:5000/$SparkImageRepository@$imageDigest"
    )

    $planPath = Join-Path $temporaryRoot "plan.json"
    Invoke-HorizontalPython -Arguments @(
        "/repo/jobs/scalability/run_horizontal_scalability_benchmark.py",
        "--plan",
        "--benchmark-id", $benchmarkId,
        "--topology", $Topology,
        "--output", "/work/plan.json"
    )
    $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

    & (Join-Path $PSScriptRoot "New-DataMasterCluster.ps1") `
        -Profile $Profile `
        -Cpus ([int]$plan.infrastructure.minikube.cpus) `
        -Memory ([int]$plan.infrastructure.minikube.memory_mib) `
        -DiskSize ([string]$plan.infrastructure.minikube.disk_size) `
        -Driver docker `
        -InsecureRegistry "host.minikube.internal:5000"
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to create isolated Minikube profile."
    }
    $script:ProfileCreatedByRun = $true
    Set-DataMasterMinikubeContext -Profile $Profile

    $namespaceManifest = New-TemporaryFile
    try {
        $namespaceYaml = @(
            Invoke-DataMasterNative -FilePath "kubectl" -CaptureOutput -Arguments @(
                "create", "namespace", "data-platform",
                "--dry-run=client", "--output=yaml"
            )
        )
        [System.IO.File]::WriteAllLines(
            $namespaceManifest.FullName,
            [string[]]$namespaceYaml,
            (New-Object System.Text.UTF8Encoding($false))
        )
        Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
            "apply", "--filename", $namespaceManifest.FullName
        )
    }
    finally {
        Remove-Item -LiteralPath $namespaceManifest.FullName -Force `
            -ErrorAction SilentlyContinue
    }
    $minioSecretValues = @{
        MINIO_ACCESS_KEY = "dm" + (
            New-Guid
        ).Guid.Replace("-", "").Substring(0, 18)
        MINIO_SECRET_KEY = New-DataMasterLocalSecretValue
    }
    $secretApplied = $false
    for ($secretAttempt = 1; $secretAttempt -le 3; $secretAttempt++) {
        try {
            Set-DataMasterKubernetesSecret `
                -Name "data-master-minio-secret" `
                -Namespace "data-platform" `
                -Values $minioSecretValues
            $secretApplied = $true
            break
        }
        catch {
            if ($secretAttempt -eq 3) {
                throw
            }
            Start-Sleep -Seconds 5
        }
    }
    if (-not $secretApplied) {
        throw "Unable to apply the MinIO Secret after bounded retries."
    }

    Invoke-DataMasterNative -FilePath "helm" -Arguments @(
        "repo", "add", "spark-operator",
        "https://kubeflow.github.io/spark-operator",
        "--force-update"
    )
    Invoke-DataMasterNative -FilePath "helm" -Arguments @(
        "repo", "update", "spark-operator"
    )
    Invoke-DataMasterNative -FilePath "helm" -Arguments @(
        "upgrade", "--install", "spark-operator",
        "spark-operator/spark-operator",
        "--version", "2.5.0",
        "--namespace", "spark-operator",
        "--create-namespace",
        "--set", "webhook.enable=true",
        "--set", "spark.jobNamespaces[0]=data-platform",
        "--wait", "--timeout", "900s"
    )
    Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
        "apply", "--filename",
        (Join-Path $root "infra\workloads\spark-apps\rbac")
    )
    Invoke-DataMasterNative -FilePath "helm" -Arguments @(
        "upgrade", "--install", "minio",
        (Join-Path $root "infra\helm-charts\minio"),
        "--namespace", "data-platform",
        "--set-string", (
            "persistence.size=" +
            [string]$plan.infrastructure.minio.persistence_size
        ),
        "--set-string", (
            "resources.requests.cpu=" +
            [string]$plan.infrastructure.minio.resources.requests.cpu
        ),
        "--set-string", (
            "resources.requests.memory=" +
            [string]$plan.infrastructure.minio.resources.requests.memory
        ),
        "--set-string", (
            "resources.limits.cpu=" +
            [string]$plan.infrastructure.minio.resources.limits.cpu
        ),
        "--set-string", (
            "resources.limits.memory=" +
            [string]$plan.infrastructure.minio.resources.limits.memory
        ),
        "--wait", "--timeout", "900s"
    )
    Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
        "get", "customresourcedefinition",
        "sparkapplications.sparkoperator.k8s.io"
    ) | Out-Null
    Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
        "rollout", "status", "deployment/minio",
        "--namespace", "data-platform",
        "--timeout=300s"
    )
    Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
        "rollout", "status", "deployment/spark-operator-controller",
        "--namespace", "spark-operator",
        "--timeout=300s"
    )

    $warmups = @()
    $measurements = @()
    foreach ($run in @($plan.runs)) {
        $result = Invoke-HorizontalApplication `
            -Run $run `
            -BenchmarkId $benchmarkId `
            -GitSha $gitSha `
            -ImageReference $imageReference `
            -ImageDigest $imageDigest `
            -WorkingDirectory $temporaryRoot
        $entry = [ordered]@{
            workload = "/work/" + (Split-Path -Leaf $result.workload)
            observation = "/work/" + (Split-Path -Leaf $result.observation)
        }
        if ($result.measurement_kind -eq "warmup") {
            $warmups += $entry
        }
        else {
            $measurements += $entry
        }
    }

    $measurementManifestPath = Join-Path $temporaryRoot "measurements.json"
    $measurementManifest = [ordered]@{
        schema_version = 1
        benchmark_id = $benchmarkId
        topology = $Topology
        infrastructure = $plan.infrastructure
        warmups = $warmups
        measurements = $measurements
    }
    [System.IO.File]::WriteAllText(
        $measurementManifestPath,
        ($measurementManifest | ConvertTo-Json -Depth 20) + [Environment]::NewLine,
        (New-Object System.Text.UTF8Encoding($false))
    )
    $parent = Split-Path -Parent $OutputPath
    [System.IO.Directory]::CreateDirectory($parent) | Out-Null
    Invoke-HorizontalPython -Arguments @(
        "/repo/jobs/scalability/run_horizontal_scalability_benchmark.py",
        "--aggregate",
        "--measurement-manifest", "/work/measurements.json",
        "--output", "/work/final-result.json"
    ) -AllowedExitCodes @(
        $script:HorizontalExitPass,
        $script:HorizontalExitInconclusive,
        $script:HorizontalExitFail
    )
    $aggregateExit = $script:LastHorizontalPythonExitCode
    if ($aggregateExit -notin @(
        $script:HorizontalExitPass,
        $script:HorizontalExitInconclusive,
        $script:HorizontalExitFail
    )) {
        throw "Horizontal aggregation returned harness exit $aggregateExit."
    }
    Copy-Item -LiteralPath (Join-Path $temporaryRoot "final-result.json") `
        -Destination $OutputPath -Force
    $script:FinalExitCode = $aggregateExit
}
catch {
    $message = [string]$_.Exception.Message
    if ($message.StartsWith("[BLOCKED] ")) {
        $result = "BLOCKED"
        $reason = $message.Substring(10)
        $script:FinalExitCode = $script:HorizontalExitBlocked
    }
    elseif ($message.StartsWith("[FAIL] ")) {
        $result = "FAIL"
        $reason = $message.Substring(7)
        $script:FinalExitCode = $script:HorizontalExitFail
    }
    else {
        $result = "HARNESS_ERROR"
        $reason = $message
        $script:FinalExitCode = $script:HorizontalExitHarnessError
    }
    Save-HorizontalTerminalResult `
        -Result $result `
        -Reason $reason `
        -Destination $OutputPath `
        -BenchmarkId $benchmarkId `
        -GitSha $gitSha `
        -ImageDigest $imageDigest
    Write-Output "HORIZONTAL_BENCHMARK_RESULT=$result"
}
finally {
    if (
        $script:ProfileCreatedByRun -and
        -not $KeepRunResources
    ) {
        & minikube delete --profile $Profile
        if ($LASTEXITCODE -ne 0 -and $script:FinalExitCode -eq 0) {
            $script:FinalExitCode = $script:HorizontalExitHarnessError
        }
    }
    if ($script:RegistryContainer) {
        & docker container inspect $script:RegistryContainer 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) {
            & docker container rm --force $script:RegistryContainer | Out-Null
            if ($LASTEXITCODE -ne 0 -and $script:FinalExitCode -eq 0) {
                $script:FinalExitCode = $script:HorizontalExitHarnessError
            }
        }
    }
    $resolvedTemporaryRoot = [System.IO.Path]::GetFullPath($temporaryRoot)
    $resolvedSystemTemp = [System.IO.Path]::GetFullPath(
        [System.IO.Path]::GetTempPath()
    )
    if (
        $resolvedTemporaryRoot.StartsWith(
            $resolvedSystemTemp,
            [System.StringComparison]::OrdinalIgnoreCase
        ) -and
        (Split-Path -Leaf $resolvedTemporaryRoot).StartsWith(
            "data-master-horizontal-"
        )
    ) {
        Remove-Item -LiteralPath $resolvedTemporaryRoot -Recurse -Force `
            -ErrorAction SilentlyContinue
    }
}

Write-Output "HORIZONTAL_BENCHMARK_ARTIFACT=$OutputPath"
exit $script:FinalExitCode
