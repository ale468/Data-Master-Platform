[CmdletBinding()]
param(
    [Parameter()]
    [string]$RepositoryRoot = (Get-Location).Path
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2.0

$resolvedRoot = (Resolve-Path -LiteralPath $RepositoryRoot).Path
$trackedOutput = & git -C $resolvedRoot ls-files -- "*.ps1"
if ($LASTEXITCODE -ne 0) {
    throw "git ls-files failed; run this validator inside a Git clone."
}

$failures = New-Object System.Collections.Generic.List[string]
$validatedCount = 0

foreach ($relativePath in @($trackedOutput)) {
    if ([string]::IsNullOrWhiteSpace($relativePath)) {
        continue
    }

    $normalizedRelativePath = $relativePath.Replace("/", [IO.Path]::DirectorySeparatorChar)
    $absolutePath = Join-Path $resolvedRoot $normalizedRelativePath
    $tokens = $null
    $parseErrors = $null
    [void][System.Management.Automation.Language.Parser]::ParseFile(
        $absolutePath,
        [ref]$tokens,
        [ref]$parseErrors
    )
    $validatedCount += 1

    foreach ($parseError in @($parseErrors)) {
        $failures.Add(
            ("{0}:{1}:{2}" -f $relativePath, $parseError.Extent.StartLineNumber, $parseError.ErrorId)
        )
    }
}

foreach ($failure in $failures) {
    Write-Output "POWERSHELL_PARSE_FAILURE=$failure"
}

if ($failures.Count -gt 0) {
    Write-Output "POWERSHELL_PARSE_STATUS=FAILURE"
    exit 1
}

Write-Output "POWERSHELL_PARSE_FILES=$validatedCount"
Write-Output "POWERSHELL_PARSE_STATUS=SUCCESS"
exit 0

