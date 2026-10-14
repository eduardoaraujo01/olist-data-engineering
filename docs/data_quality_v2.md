# Qualidade de dados — V2

**Status:** `processed` e `curated` avaliam a política V2; `processed` aplica quarentena em `enforce`.  
**Configuração:** `config/data_quality_v2.json`  
**Versão atual das regras:** `quality-v2-2`

## Objetivo e decisões

A política classifica achados por etapa e decide se a execução passa, passa com avisos, coloca linhas em quarentena ou bloqueia a publicação. Cada manifesto associa a avaliação ao `batch_id` e ao `run_id` da tentativa.

| Ação | Efeito |
|---|---|
| `fail_batch` | Bloqueia a publicação da etapa quando `affected_rows` excede `max_affected_rows`. |
| `quarantine_rows` | Em `processed`, retira as linhas afetadas do Parquet aprovado e grava um Parquet de quarentena. O limite por regra e o limite agregado por tabela precisam passar. |
| `keep_and_warn` | Mantém as linhas e registra um aviso. |

Erros técnicos de leitura, esquema ou construção dos dados também falham a etapa. A política não converte falhas técnicas em avisos.

## Modos

### `observe`

Avalia as regras e os limites sem remover linhas nem bloquear a etapa por uma decisão de qualidade. O manifesto informa `decision: "observe_only"` e `would_decision` com a decisão simulada. Os Parquets aprovados mantêm todas as linhas.

### `enforce`

Aplica as decisões das regras antes de publicar Parquets. Um bloqueio de qualidade deixa o manifesto da etapa como `failed` e não publica as saídas aprovadas daquela execução. Avisos `keep_and_warn` não bloqueiam.

A configuração atual usa `enforce`. Para voltar a apenas observar, altere `mode` em `config/data_quality_v2.json`.

## Limites de quarentena

A política usa dois limites, ambos percentuais:

1. `max_quarantine_rate_pct` na regra limita as linhas distintas afetadas por aquela regra.
2. `table_limits.<tabela>.max_quarantine_rate_pct` limita a união de todas as linhas distintas em quarentena naquela tabela.

A taxa é `linhas distintas afetadas / linhas avaliadas * 100`. Uma linha que viole duas regras conta uma vez no limite total da tabela, mas pode aparecer na contagem de cada regra. O lote é bloqueado somente quando a taxa fica **acima** do limite; atingir o limite é permitido. A comparação usa a taxa sem arredondamento.

A política atual define `0,01%` por regra de quarentena e por tabela. O valor é um limite, não uma meta de linhas a remover. Em tabelas menores que 10 mil linhas, uma única linha pode exceder `0,01%`.

As regras de quarentena atuais se aplicam a:

- `key.primary_key_duplicate`: `customers`, `orders`, `product_category_name_translation`, `products`, `sellers`, `order_items`, `order_payments` e `order_reviews`;
- `fk.transactional_orphan`: `orders`, `order_items`, `order_payments` e `order_reviews`.

`geolocation` tem um limite de tabela declarado para manter a configuração explícita, mas não participa dessas duas regras atualmente.

## Regras por etapa

### `processed`

| Regra | Check | Ação | Decisão |
|---|---|---|---|
| `key.primary_key_duplicate` | `duplicate_candidate_key` | `quarantine_rows` | Quarentena sujeita ao limite da regra e ao agregado da tabela. |
| `fk.transactional_orphan` | `orphan_foreign_key` | `quarantine_rows` | Quarentena sujeita ao limite da regra e ao agregado da tabela. |
| `postal.invalid_prefix` | `invalid_postal_code_prefix` | `keep_and_warn` | Mantém a linha e registra aviso. |
| `date.unparseable` | `unparseable_date` | `keep_and_warn` | Mantém a linha e registra aviso. |
| `category.without_translation` | `category_without_translation` em `products` | `keep_and_warn` | Mantém o valor original e registra aviso. |
| `products.category_missing` | `product_category_missing` em `products` | `keep_and_warn` | Mantém o produto e registra aviso. |

### `curated`

| Regra | Check | Ação | Decisão |
|---|---|---|---|
| `curated.natural_key_missing` | `missing_natural_key` | `fail_batch`, máximo 0 | Bloqueia chave natural ausente em `customers` ou `orders`. |
| `curated.natural_key_duplicate` | `duplicate_natural_key` | `fail_batch`, máximo 0 | Bloqueia chave natural duplicada em `customers` ou `orders`. |
| `curated.order_customer_reference_missing` | `order_without_customer_id`, `customer_reference_not_found` | `fail_batch`, máximo 0 | Bloqueia pedido sem cliente ou sem correspondência em `customers`. |
| `orders.temporal_inversion` | `timestamp_order_inversion` | `keep_and_warn` | Registra pedidos com inversão entre timestamps reais; não remove linhas. |
| `orders.estimate_before_purchase` | `estimated_delivery_before_purchase` | `keep_and_warn` | Registra previsão anterior à compra; não remove linhas. |
| `orders.delivery_after_estimate` | `delivery_after_estimate` | `keep_and_warn` | Registra entrega posterior à previsão; não remove linhas. |
| `orders.unparseable_date` | `unparseable_date` | `keep_and_warn` | Registra datas que não puderam ser interpretadas; o valor vira nulo na saída curated. |

A camada curated mantém os avisos e bloqueia as regras `fail_batch` antes de enviar qualquer Parquet curated. Nesta versão, nenhuma regra curated usa `quarantine_rows`; a quarentena por linha está implementada em `processed`.

## Categorias de produtos

Em `processed`, categoria ausente e categoria preenchida sem tradução são checks diferentes. No lote analisado anteriormente, houve 610 produtos sem categoria e 13 produtos com categoria sem tradução. Ambas as regras são avisos; não alteram `raw` nem removem produtos de `processed`.

## Quarentena e linhagem

Quando uma regra de quarentena encontra linhas e a decisão passa, `processed` grava o arquivo em:

```text
quarantine/batches/<batch_id>/<tabela>/<run_id>.parquet
```

As linhas preservam as colunas originais e recebem `_batch_id`, `_run_id`, `_source_table`, `_rule_ids` e `_quarantined_at_utc`. Se mais de uma regra afetar a mesma linha, ela aparece uma única vez no arquivo, com todos os identificadores em `_rule_ids`.

O manifesto registra caminho, quantidade, hash e tamanho do arquivo, além de `rows_by_table` e `table_limits`. Se o limite de uma regra ou da tabela for excedido, a decisão fica `blocked`; os Parquets aprovados não são publicados.

## Manifestos e execução

Os manifestos ficam em:

```text
control/manifests/processing/<batch_id>/<run_id>.json
control/manifests/curated/<batch_id>/<run_id>.json
```

Cada manifesto inclui o bloco `quality` com `ruleset_version`, `mode`, `decision`, resultados por regra, limites de tabela, ocorrências sem regra e contagens. Em `processed`, `quality.quarantine_objects` aponta os arquivos de quarentena gravados. Em `curated`, o bloco registra os checks e decisões daquela etapa.

Execute as etapas com o mesmo `BATCH_ID`:

```powershell
docker compose --profile jobs run --build --rm ingestion
$batchId = "b_COLE_AQUI_O_ID_COMPLETO"
docker compose --profile jobs run --build --rm -e "BATCH_ID=$batchId" processing
docker compose --profile jobs run --build --rm -e "BATCH_ID=$batchId" curated
```

Um manifesto `succeeded` com `quality.decision` igual a `pass` ou `pass_with_warnings` indica que a política permitiu a publicação. `status: "failed"` com `quality.decision: "blocked"` indica bloqueio por regra ou limite; consulte `quality.rules` e `quality.table_limits`.

## Limitações atuais

- Limites percentuais para `keep_and_warn` ainda não são aplicados.
- Curated não grava linhas em quarentena; suas regras atuais são bloqueios e avisos.
- As saídas por tentativa ainda usam caminhos estáveis por `batch_id`; execução concorrente e isolamento de retentativas não estão cobertos.
- A política de qualidade ainda não substitui uma política de acesso, mascaramento ou retenção de PII.
- Airflow, dbt, streaming, CI/CD, Terraform e implantação GCP permanecem fora desta etapa.
