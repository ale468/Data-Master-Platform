Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:DataMasterRepositoryRoot = [System.IO.Path]::GetFullPath(
    (Join-Path $PSScriptRoot "..\..")
)

function Get-DataMasterRepositoryRoot {
    [CmdletBinding()]
    param()
    return $script:DataMasterRepositoryRoot
}

function Assert-DataMasterSafeProfile {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Profile
    )
    if ([string]::IsNullOrWhiteSpace($Profile)) {
        throw "Minikube profile cannot be empty."
    }
    if ($Profile -eq "data-master") {
        throw "The protected profile 'data-master' cannot be modified by reproducibility scripts."
    }
}

function Invoke-DataMasterNative {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,

        [string[]]$Arguments = @(),

        [switch]$CaptureOutput
    )

    if ($CaptureOutput) {
        $previousErrorActionPreference = $ErrorActionPreference
        try {
            $ErrorActionPreference = "Continue"
            $output = & $FilePath @Arguments 2>&1
            $exitCode = $LASTEXITCODE
        }
        finally {
            $ErrorActionPreference = $previousErrorActionPreference
        }
        if ($exitCode -ne 0) {
            throw "$FilePath failed with exit code $exitCode.`n$($output -join [Environment]::NewLine)"
        }
        return $output
    }

    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$FilePath failed with exit code $LASTEXITCODE."
    }
}

function Set-DataMasterMinikubeContext {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Profile
    )

    Assert-DataMasterSafeProfile -Profile $Profile
    Invoke-DataMasterNative -FilePath "minikube" -Arguments @(
        "update-context", "-p", $Profile
    )
    Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
        "config", "use-context", $Profile
    )
}

function New-DataMasterLocalSecretValue {
    [CmdletBinding()]
    param(
        [ValidateRange(16, 128)]
        [int]$ByteCount = 32
    )

    $bytes = New-Object byte[] $ByteCount
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $generator.GetBytes($bytes)
    }
    finally {
        $generator.Dispose()
    }
    return [Convert]::ToBase64String($bytes)
}

function Set-DataMasterKubernetesSecret {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,

        [Parameter(Mandatory = $true)]
        [string]$Namespace,

        [Parameter(Mandatory = $true)]
        [hashtable]$Values
    )

    $arguments = @(
        "create", "secret", "generic", $Name,
        "--namespace", $Namespace,
        "--dry-run=client", "-o", "yaml"
    )
    foreach ($key in ($Values.Keys | Sort-Object)) {
        $arguments += "--from-literal=$key=$($Values[$key])"
    }

    $temporaryFile = New-TemporaryFile
    try {
        $manifest = & kubectl @arguments 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "Unable to render Kubernetes Secret '$Name'."
        }
        [System.IO.File]::WriteAllLines(
            $temporaryFile.FullName,
            [string[]]$manifest,
            (New-Object System.Text.UTF8Encoding($false))
        )
        & kubectl apply -f $temporaryFile.FullName | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Unable to apply Kubernetes Secret '$Name'."
        }
    }
    finally {
        Remove-Item -LiteralPath $temporaryFile.FullName -Force -ErrorAction SilentlyContinue
    }
}

function Resolve-DataMasterRemoteRevision {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$RepoUrl,

        [Parameter(Mandatory = $true)]
        [string]$Revision
    )

    $result = @(& git ls-remote $RepoUrl 2>$null)
    if ($LASTEXITCODE -ne 0) {
        return $null
    }

    $remoteRefs = @()
    foreach ($line in $result) {
        $parts = [string]$line -split "\s+", 2
        if (
            $parts.Count -eq 2 -and
            $parts[0] -match "^[0-9a-fA-F]{40}$"
        ) {
            $remoteRefs += [pscustomobject]@{
                Sha = $parts[0].ToLowerInvariant()
                Ref = $parts[1]
            }
        }
    }

    # An annotated tag advertises both its tag object and its peeled commit.
    # Only the peeled commit can be compared with a local commit SHA.
    $effectiveRefs = @()
    foreach ($remoteRef in $remoteRefs) {
        $peeledRef = "$($remoteRef.Ref)^{}"
        $hasPeeledCommit = @(
            $remoteRefs | Where-Object { $_.Ref -eq $peeledRef }
        ).Count -gt 0
        if (
            $remoteRef.Ref.StartsWith("refs/tags/") -and
            -not $remoteRef.Ref.EndsWith("^{}") -and
            $hasPeeledCommit
        ) {
            continue
        }
        $effectiveRefs += $remoteRef
    }

    if ($Revision -match "^[0-9a-fA-F]{7,40}$") {
        $revisionPrefix = $Revision.ToLowerInvariant()
        $matches = @(
            $effectiveRefs |
                Where-Object { $_.Sha.StartsWith($revisionPrefix) }
        )
    }
    else {
        if ($Revision.StartsWith("refs/heads/")) {
            $targetRefs = @($Revision)
        }
        elseif ($Revision.StartsWith("refs/tags/")) {
            $targetRefs = @($Revision, "$Revision^{}")
        }
        elseif ($Revision -eq "HEAD") {
            $targetRefs = @("HEAD")
        }
        else {
            $targetRefs = @(
                "refs/heads/$Revision",
                "refs/tags/$Revision",
                "refs/tags/$Revision^{}"
            )
        }
        $matches = @(
            $effectiveRefs |
                Where-Object { $targetRefs -contains $_.Ref }
        )
    }

    $resolvedShas = @(
        $matches |
            Select-Object -ExpandProperty Sha -Unique
    )
    if ($resolvedShas.Count -ne 1) {
        return $null
    }
    return [string]$resolvedShas[0]
}

function Test-DataMasterRemoteRevision {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$RepoUrl,

        [Parameter(Mandatory = $true)]
        [string]$Revision
    )

    $resolved = Resolve-DataMasterRemoteRevision `
        -RepoUrl $RepoUrl -Revision $Revision
    return -not [string]::IsNullOrWhiteSpace($resolved)
}

function Get-DataMasterImageTag {
    [CmdletBinding()]
    param()
    $root = Get-DataMasterRepositoryRoot
    $sha = (& git -C $root rev-parse --short=7 HEAD).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to derive image tag from Git HEAD."
    }
    return "git-$sha"
}
