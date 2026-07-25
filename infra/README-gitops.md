# GitOps local com Argo CD e Minikube

Este guia descreve o caminho Kubernetes avançado do Data Master Platform. O
objetivo é reproduzir, em um cluster Minikube isolado, a orquestração Airflow
de `SparkApplication` pelo Spark Operator e a implantação declarativa dos
serviços de apoio. Ele não é necessário para a validação pública rápida em
`local-small`.

O estado do cluster nunca deve ser inferido deste documento. Considere o
ambiente pronto somente quando os scripts emitirem seus marcadores `PASS` na
execução atual.

## Pré-requisitos

- Git;
- Docker com engine Linux em execução;
- Windows PowerShell 5.1 ou PowerShell 7+;
- Minikube;
- `kubectl`;
- Helm 3;
- pelo menos 4 CPUs e 11 GiB disponíveis para Docker no profile padrão.

Valide antes de criar o cluster:

```powershell
.\scripts\minikube\Test-DataMasterPrerequisites.ps1
```

O script verifica comandos, engine Docker, CPU, memória e portas locais. Uma
falha de pré-requisito deve ser corrigida antes de continuar.

## Caminho recomendado: clean room

O comando abaixo exige uma revisão já publicada e acessível pelo Argo CD. Ele
não faz push e recusa uma revisão remota ausente.

```powershell
.\scripts\minikube\Invoke-DataMasterCleanRoomValidation.ps1 `
  -RepoUrl "https://github.com/ale468/Data-Master-Platform.git" `
  -Revision "<branch-ou-commit-publicado>"
```

O fluxo:

1. confirma que a revisão resolve para o `HEAD` local;
2. valida os pré-requisitos;
3. cria um profile Minikube isolado;
4. constrói imagens Airflow e Spark com tag imutável derivada do commit;
5. importa as imagens e dependências no runtime Minikube;
6. gera secrets locais ou consome valores fornecidos pelo operador;
7. instala a versão fixada do chart Argo CD;
8. renderiza e aplica o app-of-apps para a revisão informada;
9. aguarda Applications, deployments, serviços e PVCs;
10. executa integração Spark, end-to-end Airflow e quality gates;
11. verifica os port-forwards e o restart controlado do MinIO.

O aceite é a presença simultânea de:

```text
CLEAN_ROOM_ISOLATION_STATUS=PASS
CLEAN_ROOM_RESTART_STATUS=PASS
CLEAN_ROOM_GITOPS_STATUS=PASS
CLEAN_ROOM_REPRODUCIBILITY_STATUS=PASS
```

Se a revisão ainda não estiver publicada, o resultado correto é bloqueio; não
substitua a revisão por `main` apenas para contornar o gate.

## Sequência manual para diagnóstico

Use esta sequência somente quando precisar isolar uma etapa do clean room.
Mantenha o mesmo profile, revisão e tag de imagem em todos os comandos.

```powershell
$profile = "data-master-repro-test"
$revision = "<branch-ou-commit-publicado>"
$repoUrl = "https://github.com/ale468/Data-Master-Platform.git"
$tag = "git-" + (git rev-parse --short=12 HEAD)

.\scripts\minikube\New-DataMasterCluster.ps1 -Profile $profile
.\scripts\minikube\Build-DataMasterImages.ps1 -Tag $tag
.\scripts\minikube\Import-DataMasterImages.ps1 `
  -Profile $profile `
  -Tag $tag `
  -PreloadRuntimeDependencies
.\scripts\minikube\Initialize-DataMasterSecrets.ps1 -Profile $profile
.\scripts\minikube\Install-DataMasterArgoCD.ps1 -Profile $profile
.\scripts\minikube\Deploy-DataMasterGitOps.ps1 `
  -Profile $profile `
  -RepoUrl $repoUrl `
  -Revision $revision `
  -ImageTag $tag
.\scripts\minikube\Wait-DataMasterReady.ps1 `
  -Profile $profile `
  -Revision $revision
.\scripts\minikube\Invoke-SparkIntegrationTest.ps1 `
  -Profile $profile `
  -ImageTag $tag
```

`Build-DataMasterImages.ps1` recusa uma árvore Git suja por padrão. Isso impede
que uma tag baseada no commit descreva conteúdo diferente. Use `-AllowDirty`
somente em diagnóstico local, nunca como evidência reproduzível.

## Secrets e acesso local

`Initialize-DataMasterSecrets.ps1` não recomenda credenciais fixas. Quando as
variáveis abaixo não existem, ele gera valores aleatórios para o cluster
efêmero e os grava como Kubernetes Secrets:

- `DATA_MASTER_MINIO_ACCESS_KEY`;
- `DATA_MASTER_MINIO_SECRET_KEY`;
- `DATA_MASTER_POSTGRES_PASSWORD`;
- `DATA_MASTER_AIRFLOW_PASSWORD`;
- `DATA_MASTER_JUPYTER_TOKEN`.

Se precisar fornecer valores controlados, defina-os no processo antes da
execução e não os grave no Git, em logs, issues ou artifacts. Os manifests
consomem os Secrets por referência; eles não devem conter credenciais em texto.

Inicie os acessos locais somente depois do readiness:

```powershell
.\scripts\minikube\Start-DataMasterPortForwards.ps1 -Profile $profile
```

Endpoints:

| Serviço | URL local |
|---|---|
| Argo CD | `https://localhost:8080` |
| Airflow | `http://localhost:8082` |
| MinIO API | `http://localhost:9000` |
| MinIO Console | `http://localhost:9001` |
| Jupyter | `http://localhost:8888` |

Pare os processos de port-forward quando terminar:

```powershell
.\scripts\minikube\Stop-DataMasterPortForwards.ps1 -Profile $profile
```

## Ordem declarativa atual

O chart `infra/argocd/applications` renderiza sete Applications filhas. A
Application raiz é aplicada separadamente pelo script de deploy.

| Sync wave | Componentes |
|---:|---|
| 0 | Spark Operator |
| 1 | PostgreSQL Metastore, Hive Metastore e MinIO |
| 2 | Jupyter |
| 3 | Airflow e RBAC dos Spark jobs |

As cargas usam os namespaces `argocd`, `spark-operator` e `data-platform`.
Não use namespaces históricos como `minio`, `jupyter` ou `spark-jobs` nos
comandos de inspeção.

O app `spark-jobs` sincroniza o RBAC necessário. Os objetos
`SparkApplication` são submetidos dinamicamente pelos testes de integração ou
pelas tasks do Airflow; não ficam instalados permanentemente pelo app-of-apps.

## Verificação e troubleshooting

Comece sempre por observações read-only:

```powershell
minikube status -p $profile
kubectl --context $profile get nodes
kubectl --context $profile get pods -A
kubectl --context $profile get applications.argoproj.io -n argocd
kubectl --context $profile get sparkapplications -n data-platform
kubectl --context $profile get pvc -n data-platform
```

Para um componente não pronto:

```powershell
kubectl --context $profile describe pod <pod> -n <namespace>
kubectl --context $profile logs <pod> -n <namespace>
kubectl --context $profile describe application <application> -n argocd
```

Problemas frequentes:

- Docker indisponível: inicie a engine Linux e repita o prerequisite check.
- CPU ou memória insuficiente: ajuste os recursos do Docker ou informe valores
  menores somente se estiver aceitando uma execução fora do profile padrão.
- `ErrImagePull`: confirme que a tag imutável foi construída e importada para
  o mesmo profile Minikube.
- Revisão remota bloqueada: publique explicitamente a branch/commit autorizado
  antes do deploy; o script não realiza esse push.
- Porta ocupada: pare o processo correspondente ou execute
  `Stop-DataMasterPortForwards.ps1` para o profile correto.
- Application fora de sync: inspecione a condição e a revisão renderizada antes
  de qualquer mutação.

Não use limpeza global de Docker como procedimento de troubleshooting. Ela
atinge imagens, caches e volumes fora deste projeto.

## Remoção explícita do cluster isolado

A remoção é destrutiva e exige profile nomeado e confirmação:

```powershell
.\scripts\minikube\Remove-DataMasterCluster.ps1 `
  -Profile $profile `
  -ConfirmDeletion
```

O script rejeita profiles protegidos. Não use comandos de remoção sem confirmar
que o alvo é o profile isolado criado para esta validação.

## Limites

- O clean room é uma validação local avançada, não operação produtiva.
- O Spark Operator demonstra separação entre orquestração e processamento; não
  prova autoscaling, alta disponibilidade ou escala cloud.
- Secrets Kubernetes gerados localmente não equivalem a secret manager
  produtivo.
- O estado live precisa ser revalidado em cada execução.
- A arquitetura alvo pode evoluir para infraestrutura distribuída, mas nenhuma
  evolução é considerada implementada sem código, gate e evidência próprios.
