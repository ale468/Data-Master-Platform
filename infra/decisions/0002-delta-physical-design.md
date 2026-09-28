# ADR 0002: desenho físico Delta orientado pelo workload

- **Status:** aceito para a demonstração local
- **Escopo:** tabelas Delta Bronze, Raw Vault e Gold
- **Decisão atual:** manter as tabelas físicas sem particionamento por diretório

## Contexto

O caso público usa dados sintéticos e volumes locais. Ele não possui, neste
momento, um workload produtivo representativo que justifique escolher uma
coluna de particionamento, clustering ou uma rotina automática de compactação.
Particionar apenas para exibir a funcionalidade aumentaria o risco de skew,
diretórios pequenos e arquivos pequenos sem benefício de pruning demonstrado.

Três conceitos não são equivalentes:

1. **particionamento da tabela** define diretórios físicos durante a escrita;
2. **particionamento de execução** controla paralelismo de shuffle, DataFrame e
   leitura de snapshot no Spark;
3. **catálogo** fornece nomes e descoberta de metadados, mas não escolhe o
   layout físico.

## Decisão

O inventário versionado em
[`config/delta/physical-design-policy.yml`](../../config/delta/physical-design-policy.yml)
é a fonte canônica. Cada tabela física aparece exatamente uma vez, declara seu
modo de escrita, layout atual, candidatos e disposição. As visões do Business
Vault são lógicas e não entram no inventário de tabelas materializadas.

Os estados são:

- `KEEP_UNPARTITIONED`: baseline atual;
- `OBSERVE`: coletar saúde dos arquivos e workload sem mudar o layout;
- `EXPERIMENT`: comparar variantes equivalentes em ambiente isolado;
- `ADOPT`: somente após decisão separada, revisada e reversível.

Uma promoção não pode ser causada apenas por quantidade de linhas, tamanho da
pasta ou proporção de arquivos pequenos. Um experimento precisa de filtros
representativos, ao menos três execuções por variante, bytes/arquivos lidos
quando observáveis, duração, overhead de escrita/manutenção e fingerprints
funcionais equivalentes.

## Saúde dos arquivos e evidência

O contrato somente leitura registra unidades explícitas: contagem de arquivos,
bytes ativos, mínimo/mediana/p95/máximo, razão de arquivos abaixo do limiar
versionado, número de partições físicas e versão Delta. Não lê nem publica
linhas de negócio. Scan amplification e crescimento são opcionais até que sejam
mensuráveis de maneira equivalente.

Evidência ausente ou incompleta resulta em `INCONCLUSIVE`; divergência funcional
resulta em `NO_CHANGE`. Mesmo evidência completa produz apenas
`EXPERIMENT_CANDIDATE`: adoção continua sendo uma decisão humana separada.

## Manutenção e recuperação

Compactação não é um cronograma padrão. Ela só pode ser considerada quando a
pressão de arquivos pequenos afetar um workload representativo e o custo de
escrita/manutenção também estiver medido. Uma prova deve propagar falhas em vez
de converter erro em aviso de sucesso.

`VACUUM` permanece desautorizado por esta decisão. Uma execução futura exige
autorização explícita, retenção revisada (nunca inferior a 168 horas nesta
política), confirmação de recuperação e preservação das evidências
reprodutíveis. Time travel, retenção e limpeza são o mesmo limite operacional,
não decisões independentes.

## Candidatos e compatibilidade

Datas de transação ou evento são apenas candidatos nas famílias de maior
crescimento. Hash keys, identificadores de alta cardinalidade e dimensões
pequenas são anti-padrões prováveis para particionamento por diretório.
Clustering, Z-order ou recursos mais novos só podem entrar em experimento após
comprovar compatibilidade com a versão Delta fixada pelo projeto.

## Limites das alegações

Esta decisão não comprova escala produtiva, SLA, redução de custo, energia ou
tempo. Ela não autoriza particionamento, `OPTIMIZE`, compactação agendada,
`VACUUM`, alteração de retenção nem seleção de layout produtivo.
