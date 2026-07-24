[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$RepoUrl,

    [Parameter(Mandatory = $true)]
    [string]$Revision,

    [string]$Profile = "data-master-repro-test",

    [string]$ImageTag,

    [string]$AirflowImageRepository = "data-master-airflow",

    [string]$SparkImageRepository = "data-master-spark-jobs"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")

Assert-DataMasterSafeProfile -Profile $Profile
if (-not (Test-DataMasterRemoteRevision -RepoUrl $RepoUrl -Revision $Revision)) {
    Write-Output "ARGOCD_REMOTE_REVISION_STATUS=BLOCKED_NOT_PUBLISHED"
    Write-Output "CLEAN_ROOM_GITOPS_STATUS=BLOCKED_REMOTE_REVISION_NOT_AVAILABLE"
    throw "Revision '$Revision' is not accessible from '$RepoUrl'. Publish it explicitly before GitOps validation."
}
if (-not $ImageTag) {
    $ImageTag = Get-DataMasterImageTag
}
if ($ImageTag -notmatch "^git-[0-9a-f]{7,40}$") {
    throw "Image tag must be immutable and match git-<sha>: $ImageTag"
}

$root = Get-DataMasterRepositoryRoot
$templatePath = Join-Path $root "infra\argocd\applications\root\app-of-apps.yaml"
$rendered = [System.IO.File]::ReadAllText($templatePath)
$replacements = [ordered]@{
    "__GIT_REPO_URL__" = $RepoUrl
    "__GIT_REVISION__" = $Revision
    "__AIRFLOW_IMAGE_REPOSITORY__" = $AirflowImageRepository
    "__SPARK_IMAGE_REPOSITORY__" = $SparkImageRepository
    "__IMAGE_TAG__" = $ImageTag
}
foreach ($token in $replacements.Keys) {
    $rendered = $rendered.Replace($token, $replacements[$token])
}
if ($rendered -match "__[A-Z0-9_]+__") {
    throw "Root Application render left unresolved tokens."
}

$temporaryFile = New-TemporaryFile
try {
    [System.IO.File]::WriteAllText(
        $temporaryFile.FullName,
        $rendered,
        (New-Object System.Text.UTF8Encoding($false))
    )
    Set-DataMasterMinikubeContext -Profile $Profile
    Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
        "apply", "-f", $temporaryFile.FullName
    )
}
finally {
    Remove-Item -LiteralPath $temporaryFile.FullName -Force -ErrorAction SilentlyContinue
}

Write-Output "ARGOCD_REPOSITORY=$RepoUrl"
Write-Output "ARGOCD_REVISION=$Revision"
Write-Output "ARGOCD_IMAGE_TAG=$ImageTag"
Write-Output "ARGOCD_REMOTE_REVISION_STATUS=PASS"
Write-Output "ARGOCD_ROOT_APPLICATION_STATUS=APPLIED"
