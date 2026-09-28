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
    $optionalApplicationNames = @("jupyter-app")

    Wait-DataMasterCondition -Description "$expectedApplications Argo CD Applications present and required applications synced and healthy" -Deadline $deadline -Condition {
        $jsonText = (& kubectl get applications.argoproj.io -n argocd -o json 2>$null) -join ""
        if (-not $jsonText) { return $false }
        $applications = ($jsonText | ConvertFrom-Json).items
        if (@($applications).Count -ne $expectedApplications) { return $false }
        $notReady = @($applications | Where-Object {
            $applicationName = [string]$_.metadata.name
            if ($applicationName -in $optionalApplicationNames) {
                return $false
            }
            $status = $_.PSObject.Properties["status"]
            if ($null -eq $status) { return $true }
            $sync = $status.Value.PSObject.Properties["sync"]
            $health = $status.Value.PSObject.Properties["health"]
            if (($null -eq $sync) -or ($null -eq $health)) { return $true }
            $syncStatus = $sync.Value.PSObject.Properties["status"]
            $healthStatus = $health.Value.PSObject.Properties["status"]
            return (
                ($null -eq $syncStatus) -or
                ($null -eq $healthStatus) -or
                ($syncStatus.Value -ne "Synced") -or
                ($healthStatus.Value -ne "Healthy")
            )
        })
        return $notReady.Count -eq 0
    }

    Wait-DataMasterCondition -Description "SparkApplication CRD" -Deadline $deadline -Condition {
        & kubectl get crd sparkapplications.sparkoperator.k8s.io *> $null
        return $LASTEXITCODE -eq 0
    }

    $deployments = @("minio", "airflow")
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

    foreach ($service in @("minio", "airflow")) {
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

    $applicationJson = (& kubectl get applications.argoproj.io -n argocd -o json) -join ""
    $applicationItems = @((ConvertFrom-Json $applicationJson).items)
    $healthyApplications = @($applicationItems | Where-Object {
        $itemStatus = $_.PSObject.Properties["status"]
        if ($null -eq $itemStatus) { return $false }
        $itemHealth = $itemStatus.Value.PSObject.Properties["health"]
        if ($null -eq $itemHealth) { return $false }
        $itemHealthStatus = $itemHealth.Value.PSObject.Properties["status"]
        ($null -ne $itemHealthStatus) -and ($itemHealthStatus.Value -eq "Healthy")
    }).Count
    $syncedApplications = @($applicationItems | Where-Object {
        $itemStatus = $_.PSObject.Properties["status"]
        if ($null -eq $itemStatus) { return $false }
        $itemSync = $itemStatus.Value.PSObject.Properties["sync"]
        if ($null -eq $itemSync) { return $false }
        $itemSyncStatus = $itemSync.Value.PSObject.Properties["status"]
        ($null -ne $itemSyncStatus) -and ($itemSyncStatus.Value -eq "Synced")
    }).Count
    $jupyterApplication = @($applicationItems | Where-Object {
        $_.metadata.name -eq "jupyter-app"
    })
    $jupyterApplicationStatus = "OPTIONAL_NOT_READY"
    if ($jupyterApplication.Count -eq 1) {
        $jupyterStatus = $jupyterApplication[0].PSObject.Properties["status"]
        if ($null -ne $jupyterStatus) {
            $jupyterSync = $jupyterStatus.Value.PSObject.Properties["sync"]
            $jupyterHealth = $jupyterStatus.Value.PSObject.Properties["health"]
            if ($null -ne $jupyterSync -and $null -ne $jupyterHealth) {
                $jupyterSyncStatus = $jupyterSync.Value.PSObject.Properties["status"]
                $jupyterHealthStatus = $jupyterHealth.Value.PSObject.Properties["status"]
                if (
                    $null -ne $jupyterSyncStatus -and
                    $null -ne $jupyterHealthStatus -and
                    $jupyterSyncStatus.Value -eq "Synced" -and
                    $jupyterHealthStatus.Value -eq "Healthy"
                ) {
                    $jupyterApplicationStatus = "READY"
                }
            }
        }
    }

    Write-Output "EXPECTED_APPLICATIONS=$expectedApplications"
    Write-Output "HEALTHY_APPLICATIONS=$healthyApplications"
    Write-Output "SYNCED_APPLICATIONS=$syncedApplications"
    Write-Output "JUPYTER_APPLICATION_STATUS=$jupyterApplicationStatus"
    Write-Output "ARGOCD_APPLICATIONS_STATUS=PASS"
    Write-Output "SPARK_OPERATOR_STATUS=PASS"
    Write-Output "SPARK_CRDS_STATUS=PASS"
    Write-Output "MINIO_STATUS=PASS"
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
