# Domínio bancário sintético e linhagem até a Gold

Este documento é a visão visual canônica do modelo implementado. Ele deve ser
lido na ordem **domínio sintético → Data Vault → consumo Gold**. Os nomes físicos
foram conferidos em
[`PipelineConfig`](../../jobs/common/config.py), nos loaders da
[`Raw Vault`](../../jobs/raw_vault/) e nos builders da
[`Gold`](../../jobs/business_vault/load_gold.py).

O escopo é demonstrativo e usa somente dados sintéticos. O desenho não afirma
modelo produtivo, catálogo corporativo, streaming produtivo, SLA ou conformidade
formal com a LGPD.

## 1. Domínio bancário sintético

O primeiro diagrama descreve o domínio antes da decomposição em Data Vault. As
cardinalidades representam as chaves presentes nas fontes sintéticas. Uma
transação sempre referencia uma conta e pode ou não referenciar um cartão.

```mermaid
erDiagram
    CLIENTE ||--o{ CONTA : possui
    AGENCIA ||--o{ CONTA : atende
    PRODUTO ||--o{ CONTA : classifica
    CONTA ||--o{ CARTAO : emite
    CONTA ||--o{ TRANSACAO : registra
    CARTAO o|--o{ TRANSACAO : pode_originar
    CLIENTE ||--o{ EVENTO_DIGITAL : realiza
    CANAL_DIGITAL ||--o{ EVENTO_DIGITAL : recebe

    CLIENTE {
        string cliente_id PK
        string nome
        string cpf
        string email
        string estado
        string cidade
    }
    CONTA {
        string conta_id PK
        string cliente_id FK
        string agencia_id FK
        string produto_id FK
        string tipo_conta
        decimal saldo
        decimal limite
    }
    CARTAO {
        string cartao_id PK
        string conta_id FK
        string tipo_cartao
        string bandeira
        string status
    }
    TRANSACAO {
        string transacao_id PK
        string conta_id FK
        string cartao_id FK
        string tipo_transacao
        decimal valor
        timestamp data_transacao
    }
    AGENCIA {
        string agencia_id PK
        string numero_agencia
        string nome
        string cidade
        string estado
    }
    PRODUTO {
        string produto_id PK
        string nome_produto
        decimal taxa_juros
        decimal comissao
        string status
    }
    CANAL_DIGITAL {
        string canal_id PK
        string canal
    }
    EVENTO_DIGITAL {
        string evento_id PK
        string cliente_id FK
        string canal_id FK
        string tipo_evento
        timestamp timestamp
        string resultado
    }
```

`EVENTO_DIGITAL` existe como registro de origem. A implementação atual cria o
Hub de `CANAL_DIGITAL`; não cria um Hub separado para `evento_id`.

## 2. Bronze → Raw Vault → Business Vault lógica → Gold

Azul identifica estruturas Delta físicas. Amarelo tracejado identifica
composições lógicas calculadas durante a construção da Gold: a baseline atual
**não materializa tabelas de Business Vault**. Cinza identifica uma observação,
não uma camada de dados.

```mermaid
flowchart LR
    subgraph BRONZE[Bronze física - Delta]
        bronze__clientes["clientes"]
        bronze__contas["contas"]
        bronze__cartoes["cartoes"]
        bronze__transacoes["transacoes"]
        bronze__agencias["agencias"]
        bronze__produtos["produtos"]
        bronze__eventos_digitais["eventos_digitais"]
    end

    subgraph RAW[Raw Vault física - Delta]
        rv_cliente["hub_cliente<br/>sat_cliente_dados_cadastrais<br/>sat_cliente_documentos"]
        rv_conta["hub_conta<br/>sat_conta_detalhes"]
        rv_cartao["hub_cartao<br/>sat_cartao_detalhes"]
        rv_transacao["hub_transacao<br/>sat_transacao_detalhes"]
        rv_agencia["hub_agencia<br/>sat_agencia_detalhes"]
        rv_produto["hub_produto<br/>sat_produto_detalhes"]
        rv_canal["hub_canal_digital<br/>sat_evento_digital_detalhes"]
        rv_links_consumed["link_cliente_conta<br/>link_conta_transacao<br/>link_conta_agencia"]
        rv_links_retained["link_cliente_cartao<br/>link_cartao_transacao<br/>link_conta_produto<br/>link_cliente_evento_digital"]
    end

    subgraph BV[Business Vault lógica - não materializada]
        bv_customers["customers latest-state"]
        bv_accounts["accounts latest-state"]
        bv_cards["cards latest-state"]
        bv_transactions["transactions latest-state"]
        bv_agencies["agencies latest-state"]
        bv_event_history["event history<br/>leitura lógica direta"]
    end

    subgraph GOLD[Gold física - sete marts Delta]
        goldnode__transacoes_por_dia["gold_transacoes_por_dia"]
        goldnode__transacoes_por_cliente["gold_transacoes_por_cliente"]
        goldnode__clientes_protegidos["gold_clientes_protegidos"]
        goldnode__volume_por_produto["gold_volume_por_produto"]
        goldnode__eventos_digitais_por_canal["gold_eventos_digitais_por_canal"]
        goldnode__contas_por_agencia["gold_contas_por_agencia"]
        goldnode__risco_transacional_simplificado["gold_risco_transacional_simplificado"]
    end

    bronze__clientes --> rv_cliente
    bronze__contas --> rv_conta
    bronze__cartoes --> rv_cartao
    bronze__transacoes --> rv_transacao
    bronze__agencias --> rv_agencia
    bronze__produtos --> rv_produto
    bronze__eventos_digitais --> rv_canal

    bronze__clientes --> rv_links_consumed
    bronze__clientes --> rv_links_retained
    bronze__contas --> rv_links_consumed
    bronze__contas --> rv_links_retained
    bronze__cartoes --> rv_links_retained
    bronze__transacoes --> rv_links_consumed
    bronze__transacoes --> rv_links_retained
    bronze__eventos_digitais --> rv_links_retained

    rv_cliente --> bv_customers
    rv_conta --> bv_accounts
    rv_cartao --> bv_cards
    rv_transacao --> bv_transactions
    rv_agencia --> bv_agencies
    rv_canal --> bv_event_history

    bv_transactions --> goldnode__transacoes_por_dia
    bv_transactions --> goldnode__transacoes_por_cliente
    bv_transactions --> goldnode__risco_transacional_simplificado
    bv_customers --> goldnode__transacoes_por_cliente
    bv_customers --> goldnode__clientes_protegidos
    bv_accounts --> goldnode__volume_por_produto
    bv_accounts --> goldnode__contas_por_agencia
    bv_accounts --> goldnode__risco_transacional_simplificado
    bv_cards --> goldnode__volume_por_produto
    bv_agencies --> goldnode__contas_por_agencia
    bv_event_history --> goldnode__eventos_digitais_por_canal
    rv_links_consumed --> goldnode__transacoes_por_cliente
    rv_links_consumed --> goldnode__contas_por_agencia
    rv_links_consumed --> goldnode__risco_transacional_simplificado

    retained["Raw Vault preservada<br/>sem consumo Gold direto na baseline"]
    rv_produto -.-> retained
    rv_links_retained -.-> retained

    classDef physical fill:#e8f1fb,stroke:#2563a6,color:#172b4d;
    classDef logical fill:#fff4cc,stroke:#9a6700,color:#4d3500,stroke-dasharray: 5 5;
    classDef note fill:#f3f4f6,stroke:#6b7280,color:#374151;
    class bronze__clientes,bronze__contas,bronze__cartoes,bronze__transacoes,bronze__agencias,bronze__produtos,bronze__eventos_digitais physical;
    class rv_cliente,rv_conta,rv_cartao,rv_transacao,rv_agencia,rv_produto,rv_canal,rv_links_consumed,rv_links_retained physical;
    class goldnode__transacoes_por_dia,goldnode__transacoes_por_cliente,goldnode__clientes_protegidos,goldnode__volume_por_produto,goldnode__eventos_digitais_por_canal,goldnode__contas_por_agencia,goldnode__risco_transacional_simplificado physical;
    class bv_customers,bv_accounts,bv_cards,bv_transactions,bv_agencies,bv_event_history logical;
    class retained note;
```

Os nós de Links agrupam as relações físicas por uso para manter o diagrama
legível. Somente `link_cliente_conta`, `link_conta_transacao` e
`link_conta_agencia` participam diretamente dos builders Gold atuais. Os demais
Links e a estrutura de produto continuam persistidos na Raw Vault, mas não
devem ser interpretados como dependência de um mart sem que o código passe a
consumi-los.

### Mapa físico da Raw Vault

| Fonte Bronze | Hub físico | Satellite físico | Relações físicas derivadas |
|---|---|---|---|
| `clientes` | `hub_cliente` | `sat_cliente_dados_cadastrais`, `sat_cliente_documentos` | participa de `link_cliente_conta`, `link_cliente_cartao` e `link_cliente_evento_digital` |
| `contas` | `hub_conta` | `sat_conta_detalhes` | `link_cliente_conta`, `link_conta_agencia`, `link_conta_produto`; também auxilia `link_cliente_cartao` |
| `cartoes` | `hub_cartao` | `sat_cartao_detalhes` | `link_cliente_cartao`, `link_cartao_transacao` |
| `transacoes` | `hub_transacao` | `sat_transacao_detalhes` | `link_conta_transacao`, `link_cartao_transacao` |
| `agencias` | `hub_agencia` | `sat_agencia_detalhes` | referenciada por `link_conta_agencia` |
| `produtos` | `hub_produto` | `sat_produto_detalhes` | referenciada por `link_conta_produto` |
| `eventos_digitais` | `hub_canal_digital` | `sat_evento_digital_detalhes` | participa de `link_cliente_evento_digital` |

### Dependências dos sete marts Gold

| Mart físico | Composição lógica e dependências Raw Vault implementadas |
|---|---|
| `gold_transacoes_por_dia` | `transactions latest-state` |
| `gold_transacoes_por_cliente` | `transactions latest-state` + `customers latest-state` + `link_conta_transacao` + `link_cliente_conta` |
| `gold_clientes_protegidos` | `customers latest-state`; aplica pseudonimização e masking demonstrativos |
| `gold_volume_por_produto` | `accounts latest-state` + `cards latest-state`; na baseline, agrupa `tipo_conta` e `tipo_cartao`, não consome `hub_produto` |
| `gold_eventos_digitais_por_canal` | histórico físico de `sat_evento_digital_detalhes`, lido logicamente sem seleção latest-state |
| `gold_contas_por_agencia` | `accounts latest-state` + `agencies latest-state` + `link_conta_agencia` |
| `gold_risco_transacional_simplificado` | `transactions latest-state` + `accounts latest-state` + `link_conta_transacao`; regra ilustrativa, não decisão de crédito |

### Contrato entre camadas

| Camada | Estado nesta baseline | Persistência |
|---|---|---|
| Bronze | cópia validada das sete fontes sintéticas, com metadados técnicos | física, Delta |
| Raw Vault | sete Hubs, sete Links e oito Satellites | física, Delta |
| Business Vault | helpers latest-state e leitura lógica usados pelos builders | lógica, não materializada |
| Gold | sete marts reconstruídos pelos builders | física, Delta |

Ao alterar uma tabela no [`PipelineConfig`](../../jobs/common/config.py), um
loader Raw Vault ou um builder Gold, atualize este documento e execute o teste
de contrato visual. O teste rejeita nomes físicos ausentes ou extras no
diagrama, reduzindo o risco de deriva entre código e documentação.
