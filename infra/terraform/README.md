# Terraform para AKS/EKS

Terraform não é obrigatório para avaliar o pipeline quando já existe um cluster Kubernetes disponível. Nesse caso, o ArgoCD aplica os manifests e Helm charts do diretório `infra/`.

Para executar o case do zero em cloud, Terraform é recomendado. Ele deve provisionar a base que o GitOps consome:

- cluster Kubernetes gerenciado: AKS ou EKS;
- node pools com CPU/memoria suficientes para Spark;
- registry de containers: ACR no Azure ou ECR na AWS;
- bucket/object storage: ADLS/S3, ou MinIO quando a demo precisa ser autocontida;
- IAM/RBAC para Spark, ArgoCD e acesso ao storage;
- ArgoCD instalado ou bootstrapado;
- apontamento da app raiz `infra/argocd/applications/root/app-of-apps.yaml`.

## Fluxo recomendado

1. Construir e publicar a imagem Spark do projeto.

```bash
docker build -f Dockerfile.spark -t <registry>/data-master-spark:0.1.0 .
docker push <registry>/data-master-spark:0.1.0
```

2. Criar a infraestrutura cloud com Terraform.

```bash
terraform init
terraform plan
terraform apply
```

3. Instalar ou apontar o ArgoCD para este repositório.

```bash
kubectl apply -f infra/argocd/applications/root/app-of-apps.yaml
```

4. Ajustar os manifests dos SparkApplications para usar a imagem publicada.

```yaml
spec:
  image: <registry>/data-master-spark:0.1.0
  mainApplicationFile: local:///opt/data-master/jobs/bronze/load_bronze.py
```

## O que ainda fica fora deste repositório

Este diretório documenta a decisão é o fluxo cloud, mas não fixa um provedor. Isso evita prender o case a AKS ou EKS antes da escolha final. A implementação Terraform deve ser criada em subpastas separadas, por exemplo:

- `infra/terraform/azure/`
- `infra/terraform/aws/`

Essa separação permite manter variaveis, IAM, networking e storage especificos de cada cloud sem misturar responsabilidades.
