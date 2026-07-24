# Data Master Platform

Referência técnica de uma plataforma de dados bancários sintéticos para
execução local. O projeto reúne geração de amostras, ingestão batch e
microbatch, camadas Bronze/Raw Vault/Gold, orquestração Airflow, processamento
Spark/Delta Lake e uma trilha GitOps para Kubernetes.

Este repositório contém somente código, testes, infraestrutura e documentação
pública. Ele não contém material privado de desafio, memória pedagógica,
prompts internos nem evidências históricas privadas.

## Arquitetura

```text
fontes sintéticas
      |
      v
Bronze Delta
      |
      v
Raw Vault (Hubs, Links e Satellites)
      |
      v
Business Vault lógica
      |
      v
Gold mascarada
```

Airflow atua como orquestrador. As transformações permanecem nos jobs
Spark/Python. Os manifests de Kubernetes, Helm e ArgoCD oferecem um caminho
local reproduzível, sem representar uma operação cloud ou produtiva validada.

## Escopo demonstrável

| Área | O que existe | Limite explícito |
|---|---|---|
| Dados | Gerador determinístico e amostras bancárias sintéticas | As combinações são aleatórias e não representam clientes reais |
| Batch/Bronze | Contratos de fonte, ingestão e leitura Delta | Baseline local; não comprova escala produtiva |
| Streaming | Microbatch local com file source e checkpoint | Não inclui broker Kafka/Kinesis/Event Hubs |
| CDC e conectores | Semântica CDC e contrato técnico executáveis localmente | Não inclui Debezium, Airbyte ou log capture produtivo |
| Data Vault | Hubs, Links, Satellites, lineage e helpers latest | Business Vault é lógica; não há PIT/Bridge física |
| Gold e privacidade | Marts derivados da Raw Vault com masking | Não constitui certificação ou compliance LGPD formal |
| Orquestração | DAG Airflow que submete jobs Spark | Não representa Airflow produtivo, HA ou multi-tenant |
| GitOps | Helm, ArgoCD, Spark Operator e scripts Minikube | Não comprova deploy cloud, autoscaling ou sizing produtivo |

## Pré-requisitos

- Git;
- PowerShell;
- Docker Desktop com engine Linux ativa;
- internet no primeiro build.

Minikube, Helm e `kubectl` são necessários apenas para o caminho Kubernetes.

## Quick start local

```powershell
git clone https://github.com/ale468/Data-Master-Platform.git
Set-Location Data-Master-Platform

docker build --file Dockerfile.spark --tag data-master-spark-jobs:local-fast .

docker run --rm --user 65534:65534 --entrypoint python3 `
  -e PYTHONDONTWRITEBYTECODE=1 `
  -e SPARK_LOCAL_IP=127.0.0.1 `
  -e SPARK_USER=nobody `
  -e "JAVA_TOOL_OPTIONS=-XX:-UseContainerSupport -Duser.home=/tmp" `
  -e SPARK_IVY_DIR=/tmp/.ivy2 `
  -e "SPARK_JARS_PACKAGES=" `
  -v "${PWD}:/repo" `
  -w /repo `
  data-master-spark-jobs:local-fast `
  -B jobs/business_vault/run_gold_masking_smoke.py `
  --runtime-profile local-small `
  --batch-id local_fast
```

O resultado esperado inclui uma linha `GOLD_MASKING_SMOKE_RESULT` cujo JSON
possui `"status": "SUCCESS"`. O comando usa dados e storage temporários; a
imagem permanece apenas no cache local.

## Testes rápidos

Com Python 3.10 disponível:

```powershell
python -m compileall -q dags jobs tests
python -m unittest discover -s tests\runtime -p "test_*.py"
```

Os builds de CI nunca publicam imagens:

```powershell
docker build --file Dockerfile.airflow --tag data-master-airflow:validation .
docker build --file Dockerfile.spark --tag data-master-spark:validation .
```

## Caminho Kubernetes local

Os scripts em `scripts/minikube/` implementam preflight, criação de cluster,
build/import de imagens, instalação ArgoCD, execução e teardown. Comece por:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\minikube\Test-DataMasterPrerequisites.ps1
```

O clean-room completo consome mais recursos e não é executado automaticamente.

## Estrutura

```text
config/                     Profiles e políticas públicas de runtime
dags/                       DAG Airflow
data/sample/                Amostras sintéticas
infra/                      Helm, ArgoCD, workloads e Terraform de referência
jobs/                       Geração, ingestão, Data Vault, Gold e smokes
scripts/minikube/           Automação do ambiente Kubernetes local
tests/                      Testes de contratos e runtime
```

## Segurança e dados

- Não use dados reais de cliente.
- Não versione tokens, chaves, senhas ou arquivos `.env` com credenciais.
- Os Secrets dos exemplos são inicializados localmente pelos scripts e não
  representam secret management produtivo.
- Gold preserva masking; contribuições não podem removê-lo.
- Todos os arquivos de `data/sample/` são sintéticos e destinados a teste.

Consulte [SECURITY.md](SECURITY.md) para relatar vulnerabilidades.

## Contribuição e licença

Leia [CONTRIBUTING.md](CONTRIBUTING.md), [GOVERNANCE.md](GOVERNANCE.md) e
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

O código deste repositório é licenciado sob
[`AGPL-3.0-only`](LICENSE). Dependências, imagens base, marcas e materiais de
terceiros permanecem sob suas licenças e direitos próprios.
