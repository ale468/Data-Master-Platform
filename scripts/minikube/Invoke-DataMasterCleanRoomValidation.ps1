[CmdletBinding()]
param(
    [string]$RepoUrl = "https://github.com/ale468/Data-Master-Platform.git",

    [string]$Revision,

    [string]$Profile = "data-master-repro-test",

    [ValidateRange(2, 64)]
    [int]$Cpus = 4,

    [ValidateRange(4096, 262144)]
    [int]$Memory = 11264,

    [string]$DiskSize = "30g",

    [ValidateSet("docker", "hyperv", "virtualbox")]
    [string]$Driver = "docker",

    [ValidateRange(900, 10800)]
    [int]$TimeoutSeconds = 5400
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")

function Assert-DataMasterFreshMinikubeProfile {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetProfile
    )

    $profileListOutput = & minikube profile list --output=json 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Output "CLEAN_ROOM_PROFILE_PREFLIGHT_STATUS=BLOCKED_PROFILE_INVENTORY_UNAVAILABLE"
        throw "Unable to inspect existing Minikube profiles before clean-room bootstrap."
    }

    try {
        $profileNames = @(
            ConvertFrom-DataMasterMinikubeProfileInventory `
                -JsonText ($profileListOutput -join [Environment]::NewLine)
        )
    }
    catch {
        Write-Output "CLEAN_ROOM_PROFILE_PREFLIGHT_STATUS=BLOCKED_PROFILE_INVENTORY_INVALID"
        throw "Minikube returned an invalid profile inventory; clean-room bootstrap was not started."
    }

    foreach ($profileName in $profileNames) {
        if ($profileName -eq $TargetProfile) {
            Write-Output "CLEAN_ROOM_PROFILE_PREFLIGHT_STATUS=BLOCKED_PREEXISTING_PROFILE"
            Write-Output "CLEAN_ROOM_ISOLATION_STATUS=BLOCKED_PREEXISTING_PROFILE"
            throw "Clean-room requires a new Minikube profile. Refusing to reuse preexisting profile '$TargetProfile'; it was not modified or deleted."
        }
    }

    Write-Output "CLEAN_ROOM_PROFILE_PREFLIGHT_STATUS=PASS"
}

function Assert-DataMasterCleanWorktree {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$RepositoryRoot
    )

    $changes = @(
        & git -C $RepositoryRoot status --porcelain --untracked-files=all 2>$null
    )
    if ($LASTEXITCODE -ne 0) {
        Write-Output "CLEAN_ROOM_WORKTREE_STATUS=BLOCKED_STATUS_UNAVAILABLE"
        throw "Unable to inspect the Git worktree before clean-room bootstrap."
    }
    if ($changes.Count -gt 0) {
        Write-Output "CLEAN_ROOM_WORKTREE_STATUS=BLOCKED_DIRTY"
        throw "Clean-room requires a clean Git worktree before any cluster mutation."
    }
    Write-Output "CLEAN_ROOM_WORKTREE_STATUS=PASS"
}

Assert-DataMasterSafeProfile -Profile $Profile
$root = Get-DataMasterRepositoryRoot
Assert-DataMasterCleanWorktree -RepositoryRoot $root
if (-not $Revision) {
    $Revision = (& git -C $root branch --show-current).Trim()
}
$localHead = (& git -C $root rev-parse HEAD).Trim()
$resolvedRevision = (& git -C $root rev-parse "$Revision^{commit}").Trim()
if ($LASTEXITCODE -ne 0 -or $resolvedRevision -ne $localHead) {
    throw "Clean-room revision must resolve to the exact local HEAD '$localHead'."
}

& (Join-Path $PSScriptRoot "Test-DataMasterPrerequisites.ps1") `
    -MinimumCpuCount $Cpus -MinimumMemoryGiB ([math]::Ceiling($Memory / 1024))

Assert-DataMasterFreshMinikubeProfile -TargetProfile $Profile

$remoteRevision = Resolve-DataMasterRemoteRevision `
    -RepoUrl $RepoUrl -Revision $Revision
if ([string]::IsNullOrWhiteSpace($remoteRevision)) {
    Write-Output "ARGOCD_REMOTE_REVISION_STATUS=BLOCKED_NOT_PUBLISHED"
    Write-Output "CLEAN_ROOM_GITOPS_STATUS=BLOCKED_REMOTE_REVISION_NOT_AVAILABLE"
    throw "Clean-room GitOps requires published revision '$Revision'. No push was performed."
}
if ($remoteRevision -ne $localHead) {
    Write-Output "ARGOCD_REMOTE_REVISION_STATUS=BLOCKED_SHA_MISMATCH"
    Write-Output "CLEAN_ROOM_GITOPS_STATUS=BLOCKED_REMOTE_REVISION_MISMATCH"
    throw "Remote revision '$Revision' resolves to '$remoteRevision', not local HEAD '$localHead'. No cluster mutation was performed."
}
Write-Output "ARGOCD_REMOTE_REVISION_STATUS=PASS_SHA_MATCH"

& (Join-Path $PSScriptRoot "New-DataMasterCluster.ps1") `
    -Profile $Profile -Cpus $Cpus -Memory $Memory -DiskSize $DiskSize -Driver $Driver

$preexisting = & kubectl get namespace data-platform --ignore-not-found -o name
if ($preexisting) {
    throw "Clean-room isolation failed: data-platform existed before bootstrap."
}

$imageTag = Get-DataMasterImageTag
& (Join-Path $PSScriptRoot "Build-DataMasterImages.ps1") -Tag $imageTag
& (Join-Path $PSScriptRoot "Import-DataMasterImages.ps1") `
    -Profile $Profile -Tag $imageTag -PreloadRuntimeDependencies
& (Join-Path $PSScriptRoot "Initialize-DataMasterSecrets.ps1") -Profile $Profile
& (Join-Path $PSScriptRoot "Install-DataMasterArgoCD.ps1") -Profile $Profile
& (Join-Path $PSScriptRoot "Deploy-DataMasterGitOps.ps1") `
    -RepoUrl $RepoUrl -Revision $Revision -Profile $Profile -ImageTag $imageTag
& (Join-Path $PSScriptRoot "Wait-DataMasterReady.ps1") `
    -Profile $Profile -Revision $Revision -TimeoutSeconds $TimeoutSeconds
& (Join-Path $PSScriptRoot "Invoke-SparkIntegrationTest.ps1") `
    -Profile $Profile -ImageTag $imageTag -TimeoutSeconds $TimeoutSeconds
$cleanRoomRunId = "minikube-clean-room-" +
    (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmss")
$cleanRoomEvidencePath = Join-Path $root "evidence\runtime\$cleanRoomRunId.json"
& (Join-Path $PSScriptRoot "Invoke-AirflowEndToEndTest.ps1") `
    -Profile $Profile -RunId $cleanRoomRunId `
    -EvidencePath $cleanRoomEvidencePath -TimeoutSeconds $TimeoutSeconds
& (Join-Path $PSScriptRoot "Invoke-DataMasterQualityGates.ps1") `
    -Profile $Profile -EvidencePath $cleanRoomEvidencePath

try {
    & (Join-Path $PSScriptRoot "Start-DataMasterPortForwards.ps1") -Profile $Profile
}
finally {
    & (Join-Path $PSScriptRoot "Stop-DataMasterPortForwards.ps1") -Profile $Profile
}

Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
    "rollout", "restart", "deployment/minio", "-n", "data-platform"
)
Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
    "rollout", "status", "deployment/minio", "-n", "data-platform",
    "--timeout=300s"
)

Write-Output "CLEAN_ROOM_PROFILE=$Profile"
Write-Output "CLEAN_ROOM_REMOTE_REVISION=$Revision"
Write-Output "CLEAN_ROOM_DURABLE_EVIDENCE_PATH=$cleanRoomEvidencePath"
Write-Output "CLEAN_ROOM_ISOLATION_STATUS=PASS"
Write-Output "CLEAN_ROOM_RESTART_STATUS=PASS"
Write-Output "CLEAN_ROOM_GITOPS_STATUS=PASS"
Write-Output "CLEAN_ROOM_REPRODUCIBILITY_STATUS=PASS"
