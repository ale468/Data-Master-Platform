[CmdletBinding()]
param(
    [string]$Profile = "data-master-repro-test",

    [string]$Namespace = "data-platform",

    [string]$MinioAccessKey = $env:DATA_MASTER_MINIO_ACCESS_KEY,

    [string]$MinioSecretKey = $env:DATA_MASTER_MINIO_SECRET_KEY,

    [string]$PostgresUser = "hive_demo",

    [string]$PostgresPassword = $env:DATA_MASTER_POSTGRES_PASSWORD,

    [string]$PostgresDatabase = "metastore",

    [string]$AirflowAdminUsername = "admin",

    [string]$AirflowAdminPassword = $env:DATA_MASTER_AIRFLOW_PASSWORD,

    [string]$JupyterToken = $env:DATA_MASTER_JUPYTER_TOKEN
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "DataMaster.Minikube.Common.ps1")

Set-DataMasterMinikubeContext -Profile $Profile
Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
    "create", "namespace", $Namespace, "--dry-run=client", "-o", "yaml"
) -CaptureOutput | kubectl apply -f - | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Unable to ensure namespace '$Namespace'."
}

if (-not $MinioAccessKey) { $MinioAccessKey = "dm" + (New-Guid).Guid.Replace("-", "").Substring(0, 18) }
if (-not $MinioSecretKey) { $MinioSecretKey = New-DataMasterLocalSecretValue }
if (-not $PostgresPassword) { $PostgresPassword = New-DataMasterLocalSecretValue }
if (-not $AirflowAdminPassword) { $AirflowAdminPassword = New-DataMasterLocalSecretValue }
if (-not $JupyterToken) { $JupyterToken = New-DataMasterLocalSecretValue }
$webserverSecret = New-DataMasterLocalSecretValue

Set-DataMasterKubernetesSecret -Name "data-master-minio-secret" -Namespace $Namespace -Values @{
    MINIO_ACCESS_KEY = $MinioAccessKey
    MINIO_SECRET_KEY = $MinioSecretKey
}
Set-DataMasterKubernetesSecret -Name "data-master-postgres-secret" -Namespace $Namespace -Values @{
    POSTGRES_USER = $PostgresUser
    POSTGRES_PASSWORD = $PostgresPassword
    POSTGRES_DB = $PostgresDatabase
}
Set-DataMasterKubernetesSecret -Name "data-master-airflow-secret" -Namespace $Namespace -Values @{
    AIRFLOW_ADMIN_USERNAME = $AirflowAdminUsername
    AIRFLOW_ADMIN_PASSWORD = $AirflowAdminPassword
    AIRFLOW_WEBSERVER_SECRET_KEY = $webserverSecret
}
Set-DataMasterKubernetesSecret -Name "data-master-jupyter-secret" -Namespace $Namespace -Values @{
    JUPYTER_TOKEN = $JupyterToken
}

$expected = @(
    "data-master-minio-secret",
    "data-master-postgres-secret",
    "data-master-airflow-secret",
    "data-master-jupyter-secret"
)
foreach ($secret in $expected) {
    Invoke-DataMasterNative -FilePath "kubectl" -Arguments @(
        "get", "secret", $secret, "-n", $Namespace
    ) | Out-Null
}
Write-Output "KUBERNETES_SECRETS_STATUS=PASS"
Write-Output "DM_SEC_002_STATUS=EVIDENCE_CAPTURED_PLANNING"
