# ADR 0001: adiar o catálogo Hive Metastore

- Status: aceito para o escopo demonstrativo local atual
- Data: 2026-09-27
- Relacionamento: issue #8

## Contexto

Os jobs Spark, a DAG e o caminho de apresentação acessam tabelas Delta por
endereços `s3a://` no MinIO. O `_delta_log` continua sendo a fonte de verdade
transacional e física. Embora o ambiente instalasse Hive Metastore e um
PostgreSQL exclusivo, nenhum fluxo canônico registrava ou consultava tabelas
por nomes lógicos do catálogo.

Manter esses componentes sem um consumidor real acrescentava duas Applications
Argo CD, dois deployments, dois services, um Secret, um PVC de 5 GiB e duas
imagens ao bootstrap, readiness e diagnóstico do E2E.

## Decisão

O runtime atual usa MinIO + Delta por caminho físico e não instala Hive
Metastore nem o PostgreSQL dedicado a ele. Spark e Jupyter não recebem URI do
Metastore. Os contratos de bootstrap, prontidão e E2E não esperam esses
componentes.

Esta decisão adia o catálogo; ela não estabelece que Hive Metastore nunca será
útil. A issue #8 é resolvida por uma decisão explícita de escopo, e não pela
implementação dos critérios de catálogo originalmente propostos.

## Consequências

Benefícios imediatos:

- menos componentes, imagens, Secret, armazenamento persistente e pontos de
  falha no ambiente local;
- menor superfície de bootstrap, readiness e troubleshooting;
- uma única forma canônica de acesso no escopo atual: caminhos Delta no MinIO.

### Delta estrutural verificável no código

| Recurso | Antes | Depois | Redução |
|---|---:|---:|---:|
| Applications Argo CD, incluindo a raiz | 8 | 6 | 2 |
| Deployments | 2 componentes dedicados | 0 | 2 |
| Services | 2 componentes dedicados | 0 | 2 |
| Secrets | 1 dedicado | 0 | 1 |
| PVC solicitado | 5 GiB | 0 | 5 GiB |
| Imagens de runtime pré-carregadas | 2 dedicadas | 0 | 2 |

No manifesto efetivamente renderizado, o PostgreSQL removido aplicava requests
de 100 millicores e 256 MiB e limits de 200 millicores e 512 MiB. O chart
do Hive declarava 200 millicores/512 MiB de requests e 500 millicores/1 GiB
de limits em `values.yaml`, mas o template não aplicava esse bloco ao pod;
esses valores não são contabilizados como reserva economizada. O consumo real
do Hive só pode ser afirmado a partir da medição observada A/B.

Custos aceitos:

- não há namespaces lógicos compartilhados nem descoberta via `SHOW TABLES`;
- consumidores precisam conhecer os caminhos físicos;
- uma futura adoção de catálogo exigirá migração e evidência próprias.

## Critérios para reavaliar

Reabrir a decisão quando existir ao menos um consumidor real que precise de
nomes lógicos estáveis, descoberta centralizada, integração de metadados ou
independência entre consumidores e caminhos físicos. A proposta futura deve
comparar Hive Metastore com alternativas atuais, definir uma única fonte
canônica de nomes e provar falha explícita sem fallback silencioso.

## Evidência e limites

A economia configurada e observada deve ser comparada em profiles Minikube
isolados, com capacidade de nó, carga, revisão, fase, amostragem e número de
amostras equivalentes. A equivalência funcional continua dependente das
validações Airflow, Delta, Data Vault, masking e Jupyter; métricas de recursos
não substituem esses gates.

Os resultados são locais e não sustentam alegações de economia financeira,
energética, cloud ou produtiva.