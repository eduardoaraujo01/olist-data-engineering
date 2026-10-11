# Geração da camada curated

Esta etapa lê `processed/olist_customers_dataset.parquet` e `processed/olist_orders_dataset.parquet` no MinIO e gera quatro arquivos em `curated/`: `dim_customer`, `dim_date`, `dim_order_status` e `fact_orders`.

`fact_orders` mantém uma linha por `order_id`. A junção entre pedidos e clientes é muitos-para-um; chaves ausentes ou referências sem correspondência são registradas no manifesto. Inversões entre timestamps também são reportadas e não removem linhas. Os Parquets processed e os arquivos raw permanecem inalterados.

## Pré-requisitos

- Docker Desktop e Docker Compose disponíveis.
- Arquivo `.env` criado a partir de `.env.example`, com senhas locais definidas.
- MinIO e o bucket configurado em `MINIO_BUCKET` em execução.
- Os nove CSVs da Olist em `data/source/` para a primeira execução.

## Executar o pipeline até curated

Na raiz do repositório, no PowerShell:

```powershell
Copy-Item .env.example .env
# Substitua as senhas de desenvolvimento no .env antes de iniciar os serviços.
docker compose up -d minio postgres
```

Crie o bucket configurado em `MINIO_BUCKET` pelo MinIO Console (`http://localhost:9001`) se ele ainda não existir. Execute as etapas na ordem:

```powershell
docker compose --profile jobs run --build --rm ingestion
docker compose --profile jobs run --build --rm processing
docker compose --profile jobs run --build --rm curated
```

O serviço `curated` é o caminho oficial para executar `processing/build_curated.py`, no mesmo ambiente Python das demais tarefas. O Compose aguarda o MinIO ficar saudável. A etapa falha se os Parquets processed necessários não existirem ou se as chaves naturais de clientes ou pedidos estiverem ausentes ou duplicadas.

## Conferir o resultado

No MinIO Console, confira os quatro objetos `curated/*.parquet` e o manifesto mais recente em `curated/_manifests/`. O manifesto deve indicar `status: succeeded` e conter hashes e contagens das entradas e saídas. Problemas de cronologia permanecem registrados em `issues` e `chronology`.

## Aplicar o esquema e continuar para o warehouse

O SQL dimensional é montado no container PostgreSQL. Em bancos novos, o PostgreSQL o executa durante a inicialização. Para uma instalação existente, aplique-o explicitamente:

```powershell
docker compose exec -T postgres psql -U olist -d olist_analytics -v ON_ERROR_STOP=1 -f /docker-entrypoint-initdb.d/01_create_orders_model.sql
docker compose --profile jobs run --build --rm warehouse_load
```

Se `POSTGRES_USER` ou `POSTGRES_DB` tiverem sido alterados no `.env`, substitua `olist` e `olist_analytics` pelos valores configurados. O SQL usa `CREATE ... IF NOT EXISTS` e pode ser reaplicado. A carga PostgreSQL valida as tabelas e a integridade referencial dentro de uma transação.

## Limite desta versão

A V1 trabalha com um snapshot local por vez. Os nomes dos objetos processed e curated são fixos; uma nova execução substitui esses objetos. Não execute duas cópias do pipeline simultaneamente sobre o mesmo bucket. Consulte o [contrato de lote](batch_contract.md) para os limites de identificação e rastreabilidade.