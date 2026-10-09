# Carga do warehouse: curated para PostgreSQL

## Objetivo

Carregar a primeira fatia analítica do MinIO para o schema `analytics`, mantendo uma linha por cliente, data, status e pedido. O carregador preserva os Parquets de origem e não corrige as anomalias de timestamps encontradas em `curated/`.

## Entradas

O serviço lê estes objetos do bucket configurado em `MINIO_BUCKET`:

- `curated/dim_customer.parquet`
- `curated/dim_date.parquet`
- `curated/dim_order_status.parquet`
- `curated/fact_orders.parquet`

O destino é o PostgreSQL, schema `analytics`, criado por `sql/curated/01_create_orders_model.sql`.

## Executar

Na raiz do projeto, rode:

```powershell
docker compose --profile jobs run --build --rm warehouse_load
```

O serviço `warehouse_load` aguarda MinIO e PostgreSQL ficarem saudáveis. As credenciais são fornecidas pelo Compose a partir do `.env`; elas não devem ser colocadas no código nem nos manifestos.

## Observabilidade da carga

Cada tentativa cria um manifesto JSON no mesmo bucket, em `warehouse/_manifests/<run_id>.json`. O carregador grava primeiro o estado `running` e atualiza o mesmo objeto ao terminar, para que uma interrupção abrupta deixe ao menos a tentativa registrada. O `run_id` é um timestamp UTC com microssegundos.

O manifesto contém status da execução e da escrita no banco, horário inicial/final, duração, objetos Curated usados, contagens de entrada e destino, resultado das validações e, quando há falha, a etapa, o tipo e a mensagem do erro. Credenciais não são gravadas. As saídas do processo continuam aparecendo no terminal do Compose; o manifesto é o histórico durável para consulta posterior.

Para consultar, abra o bucket configurado por `MINIO_BUCKET` no MinIO Console e navegue até `warehouse/_manifests/`. Cada arquivo corresponde a uma tentativa. `failed` indica falha antes do commit; `succeeded` indica que a carga e o manifesto foram registrados. `succeeded_with_observability_error` indica que o banco foi commitado, mas a gravação do manifesto final falhou; nesse caso o comando termina com erro para chamar atenção e o objeto pode ainda estar no estado `running` gravado no início.

Se a falha ocorrer durante a escrita/validação do PostgreSQL, a transação é revertida antes de o manifesto `failed` ser gravado. Se o próprio MinIO estiver indisponível, o carregador não consegue persistir a auditoria e a mensagem fica apenas na saída do Compose.

## Como a carga funciona

1. Baixa os quatro Parquets do MinIO para uma pasta temporária.
2. Valida chaves naturais, grão, correspondência entre `date_key` e `full_date`, e referências entre fato e dimensões.
3. Insere ou atualiza as dimensões por suas chaves naturais. O PostgreSQL gera `customer_key` e `order_status_key`; o carregador consulta essas chaves para montar a fato. `date_key` já é a chave da dimensão calendário.
4. Insere ou atualiza `fact_orders` por `order_id`.
5. Faz a escrita em uma transação e confere contagens e referências antes do commit. Se a validação falhar, a transação é revertida.
6. Registra no MinIO o resultado e os detalhes da execução em `warehouse/_manifests/`.

A estratégia `UPSERT` torna uma repetição com o mesmo snapshot idempotente: não duplica clientes, datas, status nem pedidos. O carregador valida que o total final de cada tabela corresponde à entrada; por isso, linhas antigas que não façam parte de um snapshot futuro causariam rollback. Uma sincronização de snapshots diferentes precisa ser desenhada antes de trocar a fonte.

## Evidência da cópia atual

Em 2026-10-08, duas execuções com o mesmo snapshot concluíram com os mesmos totais:

| Tabela | Linhas no PostgreSQL |
|---|---:|
| `dim_customer` | 99.441 |
| `dim_date` | 800 |
| `dim_order_status` | 8 |
| `fact_orders` | 99.441 |

`fact_orders` continha 99.441 identificadores `order_id` distintos. As referências às dimensões de cliente, status e às cinco funções de data não tinham órfãos.

O manifesto curated continua registrando as inversões de timestamps por par. A carga mantém esses valores como foram recebidos; regras de análise devem considerar essa limitação quando dependerem dos horários afetados.
