# Data Master Platform

Plataforma de dados bancários sintéticos para demonstrar, em ambiente local e
reproduzível, ingestão, armazenamento Delta Lake, modelagem Data Vault 2.0,
marts Gold protegidos, orquestração, observabilidade e GitOps.

[Validação completa do case](https://github.com/ale468/Data-Master-Platform/actions/workflows/case-validation.yml)
·
[Quality gates do case](https://github.com/ale468/Data-Master-Platform/actions/workflows/ci.yml)
·
[Política de segurança](SECURITY.md)

## 1. Objetivo do Case

O case implementa um fluxo de Engenharia de Dados auditável para um domínio
bancário exclusivamente sintético. O objetivo é mostrar como contratos de
fonte, processamento Spark, armazenamento Delta, Data Vault, Gold, masking e
monitoramento podem ser integrados e verificados sem depender de edição manual
da máquina do avaliador.

Os critérios técnicos centrais são:

- reproduzir o caminho principal a partir de um clone limpo com um comando;
- rastrear cada estágio por status, duração, contagens e lote;
- interromper a validação quando dados, lineage, masking ou contratos divergem;
- medir dois volumes locais sem confundir a medição com escala produtiva;
- manter código, configuração, testes e automação verificáveis e adequados ao
  compartilhamento com a banca.

Todos os dados e evidências compartilhados são sintéticos.

O baseline aprovado é local. Kubernetes, Spark Operator, Airflow e Argo CD
formam também um caminho local avançado, executado manualmente. `cloud-ready`
é somente um contrato de evolução; não representa cloud implantada ou
validada.

## 2. Arquitetura de Solução e Arquitetura Técnica

### Arquitetura da solução

```mermaid
flowchart LR
    Sources["CSV e JSON sintéticos"] --> Contracts["Contratos de fonte"]
    Contracts --> Bronze["Bronze Delta"]
    Bronze --> Raw["Raw Vault 2.0"]
    Raw --> Business["Business Vault lógica"]
    Business --> Gold["Gold mascarada"]
    Privacy["Classificação e masking"] --> Gold
```

### Arquitetura técnica e controles

```mermaid
flowchart TB
    Git["Git"] --> GitOps["Helm e Argo CD"]
    GitOps --> Kubernetes["Minikube / Kubernetes"]

    subgraph Runtime["Runtime local avançado"]
        Airflow["Airflow"] --> Operator["Spark Operator"]
        Operator --> Spark["Drivers e executores Spark"]
        Spark --> Storage["Delta Lake sobre MinIO / S3A"]
    end

    Kubernetes --> Airflow
    Kubernetes --> Operator
    Quality["Gates de qualidade"] --> Spark
    Spark --> Evidence["Resultado sanitizado"]
    Monitoring["Eventos de monitoramento"] --> Evidence
```

### Fluxo end-to-end

1. O gerador cria arquivos CSV e JSON sintéticos conforme o runtime profile.
2. A ingestão valida os contratos e grava sete tabelas Bronze com metadados
   técnicos e `batch_id`.
3. Jobs separados constroem Hubs, Links e Satellites na Raw Vault.
4. Helpers de estado mais recente formam a Business Vault lógica.
5. Sete marts Gold são materializados a partir da Raw Vault, com
   pseudonimização e masking.
6. O gate Data Vault verifica Hubs, Links, Satellites, lineage, separação de
   paths e origem da Gold.
7. O gate de privacidade verifica colunas proibidas, padrões brutos, funções de
   masking e ocorrências de segredos.
8. Eventos Delta de monitoramento registram estágio, status, lote, duração e
   contagens.
9. O orquestrador de validação projeta somente os campos explicitamente
   permitidos para um JSON de resultado e falha diante de qualquer divergência.

### Baseline local e arquitetura-alvo

| Superfície | Estado verificável | Limite |
|---|---|---|
| `local-small` | Spark local em contêiner; caminho Bronze → Raw Vault → Gold e gates | Prova funcional local, não escala distribuída |
| `local-medium` | Mesmo contrato com volume e recursos locais ampliados | Uma observação controlada, não benchmark estatístico |
| `minikube-horizontal-1` / `minikube-horizontal-3` | Spark Operator em cluster mode, com um ou três executor pods fixos | Scale-out estático local; single-node não prova distribuição física |
| Airflow | DAG importável com oito tasks e imagem construída na CI | Não prova scheduler produtivo, HA ou multi-tenancy |
| Minikube/GitOps | Automação local avançada com Helm, Argo CD e Spark Operator | Execução manual; requer recursos e revisão publicada |
| `cloud-ready` | Configuração de referência com submission `reference-only` | Não é executada por este case |

### Escolha de armazenamento

Delta Lake foi escolhido por oferecer transações ACID, controle de schema e
histórico sobre arquivos. O MinIO preserva uma interface compatível com S3 em
um ambiente local reproduzível, sem transformar a demonstração em evidência de
cloud.

Um data warehouse gerenciado simplificaria parte da operação e do consumo
analítico, mas introduziria dependência de provedor, custo e credenciais. Essa
alternativa permanece como evolução futura e exige validação própria de
segurança, escalabilidade e operação.

Em ambos os profiles do benchmark, `local[*]` usa threads do scheduler local e
uma única JVM. Aumentar memória e partições demonstra comportamento vertical e
sensibilidade a volume; não comprova executores horizontais separados.

## 3. Explicação sobre o Case Desenvolvido

### Reprodução por um único comando

Pré-requisitos:

- Git;
- Windows PowerShell 5.1 ou PowerShell 7+;
- Docker com engine Linux;
- pelo menos 2 CPUs e 4 GiB disponíveis para o Docker;
- acesso à internet no primeiro build da imagem.

Em um clone limpo:

```powershell
git clone https://github.com/ale468/Data-Master-Platform.git
Set-Location Data-Master-Platform

powershell -ExecutionPolicy Bypass `
  -File .\scripts\Invoke-PublicCaseValidation.ps1 `
  -RuntimeProfile local-small
```

O comando:

- confirma Git, Docker, CPU, memória e arquivos obrigatórios;
- exige worktree limpo para associar o resultado ao SHA executado;
- constrói a imagem Spark sem login ou publicação;
- executa como usuário não privilegiado, com o repositório somente para leitura;
- valida pipeline, Data Vault, masking, observabilidade e secret scan;
- grava `build/public-case-validation/case-validation.json`, que é ignorado pelo
  Git;
- termina com `CASE_VALIDATION_STATUS=SUCCESS` somente quando todos os gates
  passam.

O JSON sanitizado não inclui workdir, paths Delta, amostras de masking, variáveis
de ambiente, credenciais ou erro bruto. Um payload ausente, inválido ou
divergente produz exit code diferente de zero.

### Evidência no GitHub Actions

O workflow
[`case-validation.yml`](.github/workflows/case-validation.yml) executa o mesmo
orquestrador em pull requests e por `workflow_dispatch`. Ele publica:

- artefato JSON sanitizado por 14 dias;
- resumo com Pipeline, Data Vault, Masking, Observability, Secret scan e
  resultado geral;
- status de job coerente com o resultado do gate.

O workflow
[`ci.yml`](.github/workflows/ci.yml) separa validações estáticas, Helm, Spark e
Airflow. Ele compila Python, executa testes runtime e Data Vault, valida
streaming/CDC/conector, lineage, masking, falha observável, DAG, YAML, JSON,
PowerShell, links, paths, secrets e os seis charts permitidos. As imagens
Airflow e Spark são construídas, mas nunca publicadas.

Os workflows usam apenas `contents: read`; não fazem login em registry, push
de imagem ou deploy.

### Observabilidade e detecção controlada de falhas

O contrato
[`thresholds.yml`](config/observability/thresholds.yml) define:

| Sinal | Threshold demonstrativo |
|---|---:|
| Eventos de monitoring do fluxo completo | mínimo `5` |
| Geração de amostra | máximo `30 s` |
| Cada estágio Bronze, Raw Vault ou Gold | máximo `180 s` |
| Volume por fonte ou camada | mínimo `1` registro |
| Queda do mesmo volume de referência | máximo `50%` |
| Falhas de qualidade | máximo `0` |
| Falhas de masking | máximo `0` |

O
[`run_observability_failure_smoke.py`](jobs/observability/run_observability_failure_smoke.py)
injeta somente dados sintéticos e temporários. Os cenários exercitados são:

| Cenário | Regra primária | Stage | Contrato de processo |
|---|---|---|---|
| Schema inválido | `source.schema.required_columns` | `bronze` | exit `1` quando detectado corretamente |
| Fonte ausente | `source.file.required` | `bronze` | exit `1` quando detectado corretamente |
| Volume zero | `volume.minimum_rows` | `bronze` | exit `1` quando o gate bloqueia o stage |

Exit `2` representa erro do harness, regra incorreta, ausência de detecção ou
um stage operacional indevidamente bem-sucedido. Portanto, a falha proposital
não é convertida em sucesso.

#### Matriz operacional de sinais e resposta

| Sinal | Origem | Threshold ou contrato | Resposta fail-closed | Evidência ou teste |
|---|---|---|---|---|
| Schema inválido | Inspeção da fonte antes da Bronze | Colunas obrigatórias presentes | `bronze=FAILURE`; smoke termina com exit `1` | [`test_observability_detection.py`](tests/runtime/test_observability_detection.py) |
| Fonte ausente | Inspeção do arquivo de entrada | Fonte obrigatória deve existir | `bronze=FAILURE`; smoke termina com exit `1` | [`run_observability_failure_smoke.py`](jobs/observability/run_observability_failure_smoke.py) |
| Volume zero | Contagem da fonte e da camada | Mínimo `1` registro | Gate bloqueia a Bronze; smoke termina com exit `1` | [`thresholds.yml`](config/observability/thresholds.yml) |
| Executor solicitado, mas não observado | Pods e Spark status API | Observados = solicitados (`1` ou `3`) | Medição e resultado agregado tornam-se `FAIL` | [`test_horizontal_scalability.py`](tests/runtime/test_horizontal_scalability.py) |
| Tasks não distribuídas | Métricas por executor | `tasks_distributed=true`; cada executor executa tasks | Medição torna-se `FAIL` | [`test_horizontal_scalability.py`](tests/runtime/test_horizontal_scalability.py) |
| Input ou output ausente por executor | Spark status API | Input e output positivos em cada executor | Workload torna-se `FAIL` | [`test_horizontal_scalability.py`](tests/runtime/test_horizontal_scalability.py) |
| Divergência de fingerprint | Comparação das seis medições | Dataset, camadas e output devem ser iguais | Resultado agregado torna-se `FAIL` | [`test_committed_horizontal_evidence.py`](tests/runtime/test_committed_horizontal_evidence.py) |
| Reinício do MinIO | Observação do armazenamento compartilhado | Status `PASS` e `restart_count=0` | Medição torna-se `FAIL` | [artefato horizontal](tests/evidence/horizontal-scaling/hscale-20260728064640.json) |
| Falha de masking | Gate de privacidade | Máximo `0` falhas | Medição e resultado agregado tornam-se `FAIL` | [`test_committed_horizontal_evidence.py`](tests/runtime/test_committed_horizontal_evidence.py) |
| Ocorrência de segredo | Scanner do repositório | Exatamente `0` ocorrências | Resultado agregado torna-se `FAIL` | [`test_committed_horizontal_evidence.py`](tests/runtime/test_committed_horizontal_evidence.py) |

Esses sinais são contratos executáveis e evidências locais; não representam
dashboard, paging, SLO, alerta operacional ou processo on-call.

### Benchmark de escalabilidade local controlada

O
[`run_scalability_benchmark.py`](jobs/scalability/run_scalability_benchmark.py)
executa cada profile em processo e workdir separados. O orquestrador impõe
timeout, recompõe os gates do worker, rejeita payload inseguro e não usa speedup
como critério funcional.

Medição observada em 25 de julho de 2026, em uma execução por profile:

| Profile | Registros de fonte | Spark configurado | Pipeline | Total | Throughput do pipeline | Bronze / Hubs / Links / Satellites / Gold | Stage mais lento | Gates |
|---|---:|---|---:|---:|---:|---|---|---|
| `local-small` | 399 | `local[*]`, `768m`, 2 partições | 444,671 s | 596,088 s | 0,897 reg/s | 399 / 204 / 343 / 419 / 295 | Bronze | Data Vault `PASS`; masking `PASS`; monitoring `5` |
| `local-medium` | 17.028 | `local[*]`, `2g`, 8 partições | 395,813 s | 544,579 s | 43,020 reg/s | 17.028 / 7.033 / 11.892 / 17.528 / 5.722 | Bronze | Data Vault `PASS`; masking `PASS`; monitoring `5` |

Comparação descritiva: o volume observado aumentou
`42,677` vezes e a duração end-to-end foi `0,914` vez a observada no profile
menor. O resultado não exige melhora linear.
Startup, cache local, commits Delta e releituras dos gates influenciam a
medição; os números não são SLA, sizing produtivo ou previsão de cloud.

### Scale-out horizontal Spark estático

Escala vertical aumenta recursos de uma unidade de processamento. Escala
horizontal aumenta a quantidade de unidades. O experimento abaixo manteve um
core e 1 GiB de heap por executor e alterou somente o identificador do profile
e `spark.executor_instances`, de `1` para `3`. Dynamic allocation permaneceu
desabilitada.

```mermaid
flowchart TB
    Benchmark["Benchmark horizontal<br/>warm-up + 3 medições por cenário"]
    Operator["Spark Operator<br/>cluster mode"]
    Driver1["driver pod<br/>baseline"]
    Executor1["executor pod 1"]
    Driver3["driver pod<br/>scale-out"]
    Executor31["executor pod 1"]
    Executor32["executor pod 2"]
    Executor33["executor pod 3"]
    Storage["MinIO/S3A compartilhado<br/>prefixo isolado por run"]

    Benchmark --> Operator
    Operator --> Driver1
    Driver1 --> Executor1
    Operator --> Driver3
    Driver3 --> Executor31
    Driver3 --> Executor32
    Driver3 --> Executor33
    Executor1 --> Storage
    Executor31 --> Storage
    Executor32 --> Storage
    Executor33 --> Storage
```

Execução observada em 28 de julho de 2026:

| Contrato controlado | Baseline | Scale-out |
|---|---:|---:|
| Profile | `minikube-horizontal-1` | `minikube-horizontal-3` |
| Executor instances | 1 | 3 |
| Cores por executor | 1 | 1 |
| Heap por executor | 1 GiB | 1 GiB |
| Shuffle partitions | 24 | 24 |
| Dataset / seed | `controlled-horizontal-v1` / `42` | `controlled-horizontal-v1` / `42` |
| Medições válidas | 3 | 3 |
| Durações | 1794,212 s; 1839,091 s; 1840,697 s | 1286,373 s; 1283,205 s; 1336,855 s |
| Throughputs | 181,295; 176,871; 176,716 reg/s | 252,867; 253,491; 243,318 reg/s |
| Mediana de duração | 1839,091 s | 1286,373 s |
| Mediana de throughput | 176,871 reg/s | 252,867 reg/s |
| Executores solicitados / observados | 1 / 1 em todas as medições | 3 / 3 em todas as medições |
| Nós observados | 1 | 1 |

O warm-up de `1750,564 s` foi descartado. Com as medianas, o speedup foi
`1,430` e a eficiência paralela foi `0,477` (`47,7%`). Os três executores do
scale-out executaram tasks e registraram input, output, runtime e shuffle; não
foi inferido paralelismo apenas pela configuração.

O experimento processou `325.281` registros de entrada em cada run. Todas as
seis medições produziram o mesmo dataset fingerprint
`sha256:1bf3ccfb1ab954fd8d33903da922fa3d720b5daa408397568fb7b6b372ee946e`
e o mesmo output fingerprint
`sha256:c8f68e9b86d79e936b0fb4e77e2df8f5604027028eb1ce036f57e1bcabd3762b`.
As contagens equivalentes foram:

- Bronze: `325.281`;
- Raw Vault Hubs: `125.286`;
- Raw Vault Links: `195.294`;
- Raw Vault Satellites: `330.281`;
- Gold: `95.484`.

Data Vault, lineage, masking, monitoring e qualidade retornaram `PASS`; o scan
encontrou zero secrets e a Gold não expôs dados brutos. O resultado agregado
foi `PASS`, vinculado ao commit
`ee198106abda668a833826ace0e16f4e56516025` e à imagem
`sha256:88b8facb12967c01f157bfd1245b44e9c3d101ee4762b0794b2f706e9a85ccac`.
A evidência sanitizada está em
[`hscale-20260728064640.json`](tests/evidence/horizontal-scaling/hscale-20260728064640.json).

<details>
<summary>Como reproduzir o benchmark horizontal (execução manual de aproximadamente quatro horas)</summary>

#### Pré-requisitos e comando

Esta é uma execução manual longa e exclusiva para Windows. Antes de iniciar,
confirme:

- Git, Windows PowerShell 5.1 ou PowerShell 7+, Docker com engine Linux,
  Minikube, Helm e `kubectl`;
- ao menos 4 CPUs lógicas, 16 GiB de memória no host, 11 GiB disponíveis para
  o Docker e 45 GiB livres na unidade `C:`;
- Docker em execução, acesso à internet, porta local `5000` livre e worktree
  Git limpo;
- ausência do profile alvo: por padrão o script cria
  `data-master-horizontal-<timestamp UTC>` e bloqueia, sem alterá-lo, se esse
  profile já existir.

Em um clone limpo, na raiz do repositório:

```powershell
powershell -ExecutionPolicy Bypass `
  -File .\scripts\minikube\Invoke-HorizontalScalingBenchmark.ps1
```

O plano controlado cria um Minikube isolado com 4 CPUs, `11264 MiB` e disco
`40g`. Ele descarta um warm-up com um executor e registra três medições com um
executor e três com três executores. Na execução versionada, os sete workloads
consumiram aproximadamente `3 h 05 min`; reserve cerca de quatro horas para
incluir build, provisionamento e limpeza.

O resultado padrão é gravado em
`tests/evidence/horizontal-scaling/hscale-<timestamp UTC>.json`. Ao terminar,
o script remove o profile criado, o registry efêmero e os arquivos
temporários. `-KeepRunResources` preserva somente o profile Minikube e os
serviços que ainda estiverem nele para inspeção; o registry e os temporários
continuam sendo removidos, e cada `SparkApplication` já é apagada após sua
medição.

| Exit | Resultado | Interpretação |
|---:|---|---|
| `0` | `PASS` | Gates e equivalência passaram, executores foram observados e houve benefício mensurável pela mediana |
| `2` | `INCONCLUSIVE` | Execução funcional, mas sem benefício mensurável |
| `3` | `FAIL` | Medição, gate, observação ou equivalência falhou |
| `4` | `HARNESS_ERROR` | O orquestrador ou agregador não conseguiu produzir uma conclusão válida |
| `5` | `BLOCKED` | Preflight de ambiente ou segurança impediu o início controlado |

Para conferir somente o artefato versionado em segundos, sem Docker, Minikube
ou rede:

```powershell
python -m unittest discover `
  -s tests/runtime `
  -p "test_committed_horizontal_evidence.py" `
  -v
```

</details>

O resultado demonstra scale-out estático do processamento Spark no Minikube.
Como todos os executores foram observados no mesmo nó, ele não comprova
distribuição física multi-node, autoscaling, HPA, dynamic allocation, cloud,
SLA, custo ou sizing produtivo.

### Matriz requisito → implementação → evidência

| Requisito | Implementação no repositório | Evidência verificável |
|---|---|---|
| Reprodutibilidade | [`Invoke-PublicCaseValidation.ps1`](scripts/Invoke-PublicCaseValidation.ps1) | JSON ignorado localmente e artefato do [workflow](https://github.com/ale468/Data-Master-Platform/actions/workflows/case-validation.yml) |
| Observabilidade | [`monitoring.py`](jobs/common/monitoring.py), thresholds e failure smoke | Cinco eventos no end-to-end; três falhas negativas; testes em [`test_observability_detection.py`](tests/runtime/test_observability_detection.py) |
| Escalabilidade | Profiles locais e horizontais, adaptador único e benchmarks isolados | Artefato horizontal sanitizado, métricas de executores e tarefas e contratos em [`test_horizontal_scalability.py`](tests/runtime/test_horizontal_scalability.py) |
| Data Vault e lineage | Hubs, Links, Satellites e quality gate executável | Suíte [`tests/data_vault`](tests/data_vault) e job Spark da CI |
| Gold e privacidade | Marts Raw-derived, pseudonimização, masking e scan | [`run_gold_masking_smoke.py`](jobs/business_vault/run_gold_masking_smoke.py) e gate Spark |
| Streaming e CDC | Structured Streaming com file source e semântica CDC local | Smokes versionados e executados na CI; sem claim de broker ou log capture |
| Orquestração | DAG Airflow com oito tasks que submetem jobs Spark | Import da DAG em imagem Airflow na CI |
| GitOps local | Scripts Minikube, charts Helm e app-of-apps Argo CD | `helm lint/template` na CI e [guia operacional](infra/README-gitops.md) |
| Qualidade da entrega | Workflows separados por tipo de gate | Gates fail-closed, permissões somente leitura e builds sem push; branch protection não foi verificada |

O orquestrador clean-room só aceita um profile Minikube alvo novo e ausente.
Se o profile já existir, o preflight bloqueia a execução antes de qualquer
criação de cluster e não altera nem remove esse profile.

### Estrutura principal

```text
config/                     Profiles, privacidade e thresholds da entrega
dags/                       DAG Airflow
data/sample/                Amostras exclusivamente sintéticas
infra/                      Helm, Argo CD, workloads e referências Terraform
jobs/                       Geração, ingestão, Data Vault, Gold e smokes
scripts/                    Validação do case e automação Minikube
tests/                      Contratos runtime, Data Vault e PowerShell
```

## 4. Melhorias e Considerações Finais

Esta versão acrescenta uma validação de um comando, CI expandida,
detecção negativa de falhas, benchmark controlado, scanner fail-closed e um
guia GitOps alinhado aos manifests atuais. A defesa técnica pode partir do
README, abrir o workflow e, se necessário, reproduzir o artefato a partir do
mesmo SHA.

### Limites que permanecem válidos

| Tema | O que foi demonstrado | O que não deve ser afirmado |
|---|---|---|
| Cloud | Arquitetura e profile de referência | Deploy cloud validado, operação ou custo |
| Escala | Dois volumes locais e scale-out Spark estático de 1 para 3 executores no mesmo nó | Autoscaling, multi-node, speedup linear, escala produtiva ou de todos os componentes |
| Streaming | Microbatch local com file source e checkpoint | Kafka, Kinesis, Event Hubs ou SLA produtivo |
| CDC/conectores | Semântica CDC e contrato de adapter local | Debezium, Airbyte, Kafka Connect ou log capture real |
| LGPD | Classificação, pseudonimização, masking e scan técnico | Certificação, parecer jurídico ou compliance formal |
| Observabilidade | Eventos, contagens, duração, thresholds e falha atribuída | Dashboard, alertas, SLO, paging ou operação on-call |
| Orquestração | DAG, imagens e caminho Minikube avançado | Airflow/Kubernetes produtivo, HA ou multi-tenancy |
| Secrets | Ausência de segredo versionado e Secrets locais gerados | Secret manager corporativo, IAM ou rotação produtiva |

Evoluções futuras exigem ambiente, gate e evidência próprios. Adicionar uma
tecnologia ao desenho não a transforma em capacidade implementada.

Use somente dados sintéticos. Não versione tokens, chaves, senhas ou arquivos
`.env`. Gold deve continuar mascarada.

Para contribuir, leia [CONTRIBUTING.md](CONTRIBUTING.md),
[GOVERNANCE.md](GOVERNANCE.md) e
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## License and Copyright

Copyright (C) 2026 Alexandre Ferreira. O código original do projeto é
licenciado sob [`AGPL-3.0-only`](LICENSE). O repositório original e canônico é
https://github.com/ale468/Data-Master-Platform.

Consulte [COPYRIGHT](COPYRIGHT), [NOTICE](NOTICE) e
[PROVENANCE.md](PROVENANCE.md) para o escopo da atribuição, componentes de
terceiros, cronologia pública e procedimento de release. Dependências, imagens
base, marcas e materiais de terceiros permanecem sob seus próprios termos.
