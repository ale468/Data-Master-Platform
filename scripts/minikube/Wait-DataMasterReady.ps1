[CmdletBinding()]
param(
    [string]$Profile = "data-master-repro-test",

    [ValidateRange(120, 10800)]
    [int]$TimeoutSeconds = 1200,

    [string]$Revision = "main"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")

function Wait-DataMasterCondition {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$Condition,

        [Parameter(Mandatory = $true)]
        [string]$Description,

        [Parameter(Mandatory = $true)]
        [datetime]$Deadline
    )

    while ((Get-Date) -lt $Deadline) {
        if (& $Condition) {
            Write-Output "READY_CHECK=$Description"
            return
        }
        Start-Sleep -Seconds 5
    }
    throw "Timed out waiting for $Description."
}

Set-DataMasterMinikubeContext -Profile $Profile
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$root = Get-DataMasterRepositoryRoot

try {
    Wait-DataMasterCondition -Description "required namespaces" -Deadline $deadline -Condition {
        $namespaces = & kubectl get namespace -o name 2>$null
        return (
            ($namespaces -contains "namespace/argocd") -and
            ($namespaces -contains "namespace/data-platform") -and
            ($namespaces -contains "namespace/spark-operator")
        )
    }

    $childRender = & helm template data-master-applications `
        (Join-Path $root "infra\argocd\applications") `
        --set-string "git.revision=$Revision" 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to derive expected Argo CD Applications."
    }
    $expectedChildren = @($childRender | Select-String -Pattern "^kind: Application$").Count
    $expectedApplications = $expectedChildren + 1

    Wait-DataMasterCondition -Description "$expectedApplications Argo CD Applications synced and healthy" -Deadline $deadline -Condition {
        $jsonText = (& kubectl get applications.argoproj.io -n argocd -o json 2>$null) -join ""
        if (-not $jsonText) { return $false }
        $applications = ($jsonText | ConvertFrom-Json).items
        if (@($applications).Count -ne $expectedApplications) { return $false }
        $notReady = @($applications | Where-Object {
            ($_.status.sync.status -ne "Synced") -or
            ($_.status.health.status -ne "Healthy")
        })
        return $notReady.Count -eq 0
    }

    Wait-DataMasterCondition -Description "SparkApplication CRD" -Deadline $deadline -Condition {
        & kubectl get crd sparkapplications.sparkoperator.k8s.io *> $null
        return $LASTEXITCODE -eq 0
    }

    $deployments = @("minio", "postgres-metastore", "hive-metastore", "airflow")
    foreach ($deployment in $deployments) {
        $remaining = [math]::Max(1, [int]($deadline - (Get-Date)).TotalSeconds)
        Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
            "wait", "--for=condition=Available", "deployment/$deployment",
            "-n", "data-platform", "--timeout=${remaining}s"
        )
    }
    $remaining = [math]::Max(1, [int]($deadline - (Get-Date)).TotalSeconds)
    Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
        "wait", "--for=condition=Available", "deployment",
        "-l", "app.kubernetes.io/name=spark-operator",
        "-n", "spark-operator", "--timeout=${remaining}s"
    )

    Wait-DataMasterCondition -Description "bound PVCs" -Deadline $deadline -Condition {
        $jsonText = (& kubectl get pvc -n data-platform -o json 2>$null) -join ""
        if (-not $jsonText) { return $false }
        $claims = ($jsonText | ConvertFrom-Json).items
        return (@($claims).Count -gt 0) -and -not @(
            $claims | Where-Object { $_.status.phase -ne "Bound" }
        )
    }

    foreach ($service in @("minio", "postgres-metastore", "hive-metastore", "airflow")) {
        Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
            "get", "service", $service, "-n", "data-platform"
        ) | Out-Null
    }

    $statefulSetJson = (& kubectl get statefulset -A -o json 2>$null) -join ""
    if ($statefulSetJson) {
        foreach ($statefulSet in (ConvertFrom-Json $statefulSetJson).items) {
            $remaining = [math]::Max(1, [int]($deadline - (Get-Date)).TotalSeconds)
            Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
                "rollout", "status", "statefulset/$($statefulSet.metadata.name)",
                "-n", $statefulSet.metadata.namespace, "--timeout=${remaining}s"
            )
        }
    }

    Write-Output "EXPECTED_APPLICATIONS=$expectedApplications"
    Write-Output "HEALTHY_APPLICATIONS=$expectedApplications"
    Write-Output "SYNCED_APPLICATIONS=$expectedApplications"
    Write-Output "ARGOCD_APPLICATIONS_STATUS=PASS"
    Write-Output "SPARK_OPERATOR_STATUS=PASS"
    Write-Output "SPARK_CRDS_STATUS=PASS"
    Write-Output "MINIO_STATUS=PASS"
    Write-Output "POSTGRES_METASTORE_STATUS=PASS"
    Write-Output "HIVE_METASTORE_STATUS=PASS"
    Write-Output "AIRFLOW_STATUS=PASS"
    Write-Output "DATA_MASTER_READY_STATUS=PASS"
}
catch {
    Write-Output "DATA_MASTER_READY_STATUS=FAIL"
    & kubectl get applications.argoproj.io -n argocd -o wide 2>$null
    & kubectl get pods,deployments,statefulsets,services,pvc -A -o wide 2>$null
    & kubectl get events -A --sort-by=.lastTimestamp 2>$null | Select-Object -Last 40
    throw
}
