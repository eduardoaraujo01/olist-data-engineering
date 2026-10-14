# Geração da camada curated

A etapa lê `customers.parquet` e `orders.parquet` do lote em `processed/batches/<BATCH_ID>/` e gera quatro objetos: `dim_customer`, `dim_date`, `dim_order_status` e `fact_orders`. `fact_orders` mantém uma linha por `order_id`.

A política compartilhada em `config/data_quality_v2.json` também é avaliada na etapa `curated`. As chaves naturais ausentes ou duplicadas e as referências de cliente inválidas bloqueiam a publicação em `enforce`. Inversões de timestamps, datas não interpretáveis e entregas após a previsão são registradas como avisos. Avisos não removem linhas. Os Parquets raw e processed não são alterados.

## Pré-requisitos

- Docker Desktop e Docker Compose disponíveis.
- Arquivo `.env` configurado com as credenciais locais.
- MinIO e o bucket definido por `MINIO_BUCKET` em execução.
- Os nove CSVs da Olist em `data/source/` na primeira execução.

## Executar

Na raiz do repositório, no PowerShell:

```powershell
docker compose up -d minio postgres
docker compose --profile jobs run --build --rm ingestion
$batchId = "b_COLE_AQUI_O_ID_COMPLETO"
docker compose --profile jobs run --build --rm -e "BATCH_ID=$batchId" processing
docker compose --profile jobs run --build --rm -e "BATCH_ID=$batchId" curated
```

O comando de ingestão imprime o identificador completo do lote. Use esse mesmo `BATCH_ID` nas duas etapas seguintes. A etapa `curated` publica seus objetos somente depois da avaliação das regras em `enforce`.

## Conferir o resultado

No MinIO Console, abra os manifestos:

```text
control/manifests/processing/<BATCH_ID>/<run_id>.json
control/manifests/curated/<BATCH_ID>/<run_id>.json
```

No manifesto curated, confira `status`, `quality.mode`, `quality.decision`, `quality.rules` e `rows`. Uma execução permitida termina com `status: "succeeded"` e decisão `pass` ou `pass_with_warnings`. Um erro de qualidade termina com `status: "failed"` e decisão `blocked`; essa tentativa não envia novos Parquets curated.

Os avisos cronológicos aparecem em `quality.rules` e também no resumo `chronology`. Em `enforce`, eles não bloqueiam a geração.

## Aplicar o esquema e continuar para o warehouse

Em bancos novos, o PostgreSQL aplica o SQL durante a inicialização. Para uma instalação existente, aplique o esquema dimensional e carregue o mesmo lote:

```powershell
docker compose exec -T postgres psql -U olist -d olist_analytics -v ON_ERROR_STOP=1 -f /docker-entrypoint-initdb.d/01_create_orders_model.sql
docker compose --profile jobs run --build --rm -e "BATCH_ID=$batchId" warehouse_load
```

Se `POSTGRES_USER` ou `POSTGRES_DB` tiverem sido alterados no `.env`, substitua `olist` e `olist_analytics`. A carga PostgreSQL valida as tabelas e a integridade referencial dentro de uma transação.

## Limites desta etapa

As saídas curated ainda usam caminhos estáveis por `BATCH_ID`; evite execuções simultâneas do mesmo lote. Não há regra curated configurada para quarentena por linha. A política atual decide entre bloquear e manter com aviso nessa etapa.
