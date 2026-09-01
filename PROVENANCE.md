# Proveniência, atribuição e comparação

## Repositório canônico

O repositório original e canônico do Data Master Platform é:

https://github.com/ale468/Data-Master-Platform

Essa declaração identifica a origem pública de referência. Ela não impede
forks, uso comercial, modificações ou obras derivadas permitidas pela
AGPL-3.0-only.

## Baseline público auditado

| Elemento | Valor auditado em 2026-08-27 |
|---|---|
| Primeiro commit público | `7e178a48bdaded31ceaea8d0ea862a7dbd76914f` |
| Data do primeiro commit público | 2026-07-24 |
| HEAD usado como base desta política | `dc5ceb233e0c1fe9d4fbde4e511e1375fb972e4c` |
| Árvore do HEAD base | `03708302e6ee70aa96c122968cb97306c49dfaf2` |
| Tags existentes na auditoria | nenhuma |
| Releases existentes na auditoria | nenhuma |

O histórico público registra 37 commits com autoria atribuída a Alexandre
Ferreira ou à conta `ale468`. Essa consistência é evidência cronológica, mas
não prova, sozinha, que todo conteúdo seja original ou que não existam direitos
de terceiros. Os três commits de merge do baseline foram marcados como
`verified: true` e `reason: valid` pela API do GitHub; os outros 34 commits do
baseline não possuem assinatura criptográfica.

## Cadeia recomendada para uma release de referência

A primeira release de proveniência deve ser criada somente depois de a mudança
ser revisada e integrada em `main`. Como o projeto ainda não declara uma API
pública estável, o marco recomendado é Calendar Versioning:

- tag: `v2026.08.1`;
- título: `Data Master Platform v2026.08.1 — Provenance Baseline`;
- licença: `AGPL-3.0-only`;
- titular das contribuições originais: `Copyright (C) 2026 Alexandre Ferreira`.

`v1.0.0` pode ser adotado posteriormente se o projeto declarar a API pública e
o compromisso de compatibilidade exigidos pelo Semantic Versioning. A versão
da release não representa produção, cloud, escala produtiva ou compliance
jurídico.

Procedimento após o merge:

```bash
git switch main
git pull --ff-only origin main
git rev-parse HEAD
git rev-parse "HEAD^{tree}"
git tag -s v2026.08.1 <COMMIT_SHA> -m "Data Master Platform v2026.08.1 — Provenance Baseline"
git tag -v v2026.08.1
git push origin v2026.08.1
```

A GitHub Release deve usar a tag existente, citar o commit e a árvore, e
registrar o SHA-256 de qualquer ZIP adicional. Se a opção de releases
imutáveis estiver disponível, ela deve ser ativada antes da publicação. Não se
deve mover ou recriar a tag após a release.

Verificação posterior:

```bash
git fetch --tags origin
git verify-tag v2026.08.1
git rev-list -n 1 v2026.08.1
git rev-parse "v2026.08.1^{tree}"
git archive --format=zip --output=data-master-v2026.08.1.zip v2026.08.1
```

O hash do commit identifica o objeto Git; o hash da árvore identifica o
conteúdo versionado; a assinatura vincula a tag à chave do signatário; e a
release fornece um registro público e um snapshot baixável.

## Proteções Git e GitHub recomendadas

1. Configurar assinatura SSH ou GPG para commits e tags, publicar a chave de
   assinatura na conta GitHub e manter uma cópia verificável da chave pública.
2. Ativar um ruleset de `main` que exija pull request, checks obrigatórios,
   commits verificados, conversas resolvidas e bloqueio de force-push e delete.
3. Ativar um ruleset para tags `v*` que bloqueie atualização e exclusão.
4. Publicar releases a partir de tags assinadas e, quando disponível, usar
   releases imutáveis.
5. Registrar commit, árvore, assinatura e hashes de assets nas release notes.

Assinaturas e imutabilidade aumentam a força de integridade e origem. Pull
requests, reviews e nomenclatura de branches melhoram governança, mas não são,
isoladamente, prova criptográfica de autoria.

## Fingerprints públicos legítimos

### Elementos genéricos

- Spark, Delta Lake, Airflow, MinIO, Kubernetes, Helm e Argo CD;
- termos como Bronze, Gold, Data Vault, Hub, Link e Satellite;
- uso de YAML, Python, PowerShell e testes unitários.

Esses elementos são comuns e não demonstram derivação.

### Elementos potencialmente distintivos em combinação

- a sequência `bronze->raw_vault->business_vault_latest->gold`;
- os arquivos `DataMaster.ExecutionEvidence.ps1`,
  `run_observability_failure_smoke.py` e `raw_vault_lineage.py`;
- a combinação de `gold_clientes_protegidos`, `hub_cliente`,
  `sat_cliente_documentos` e `sat_evento_digital_detalhes`;
- o benchmark horizontal com profiles 1 e 3, fingerprints de dataset e
  métricas por executor;
- gates fail-closed que combinam Data Vault, separação de paths, masking,
  observabilidade e evidência sanitizada.

Strings existentes e adequadas para pesquisa pública:

- `BUSINESS_VAULT_GOLD_PATH_SEPARATION_STATUS=PASS`
- `CLEAN_ROOM_PROFILE_PREFLIGHT_STATUS=BLOCKED_PREEXISTING_PROFILE`
- `PRESENTATION_MARKER_COUNT_INVALID`
- `HORIZONTAL_WORKLOAD_RESULT=` combinado com `source.schema.required_columns`
- `AIRFLOW_DURABLE_EVIDENCE_MATERIALIZATION_STATUS=PASS`

Essas strings têm função operacional real. Não foram adicionadas como marcas
d'água secretas.

## Pesquisa passiva e comparação em níveis

Uma busca periódica pode usar GitHub Code Search, GitLab, mecanismos de busca e
outros indexadores públicos. Registre a consulta, a data, a URL encontrada e
um snapshot permitido do resultado antes de qualquer conclusão.

| Nível | Método | Inferência possível | Limite |
|---|---|---|---|
| 1 — cópia literal | SHA-256 por arquivo, hash da árvore, texto e whitespace normalizado | Forte coincidência de conteúdo | Arquivos genéricos ou upstream podem coincidir legitimamente |
| 2 — código renomeado | tokens, AST, funções, strings, testes, MinHash ou fuzzy hashing | Evidência de transformação superficial | Ferramentas produzem score, não autoria ou infração |
| 3 — código reorganizado | grafo de chamadas, schemas, sequência de gates, contratos e estrutura de testes | Evidência estrutural acumulada | Refactors independentes e padrões comuns geram falsos positivos |
| 4 — reimplementação conceitual | requisitos, decisões, arquitetura e comportamento | Similaridade de abordagem | Ideias e arquitetura semelhante não são prova automática de plágio |

Para uma análise defensável, preserve os dois commits comparados, remova
arquivos upstream conhecidos, calcule múltiplas métricas, examine a cronologia
e faça revisão manual por unidade funcional. Não use uma porcentagem única como
veredicto jurídico.

## Automação e limites

O gate `scripts/validate_provenance.py` valida arquivos centrais, headers,
classificação de exceções e inventário de dependências diretas. Arquivos-fonte
que já existiam no commit-base da auditoria ficam em revisão legada, sem
atribuição automática; qualquer novo arquivo-fonte deve receber um header
comprovado ou uma exceção documentada. O gate é executado pela CI e por um hook local
opcional. Para uma auditoria mais ampla, evolua em uma mudança separada para
REUSE, ScanCode Toolkit ou OSS Review Toolkit e gere um SBOM versionado com
Syft ou ferramenta equivalente.

A estratégia fortalece cronologia, atribuição, consistência de licença e
comparabilidade. Ela não impede downloads ou remoção de comentários, não
descobre toda cópia, não protege ideias abstratas, não prova automaticamente
plágio e não substitui análise jurídica.
