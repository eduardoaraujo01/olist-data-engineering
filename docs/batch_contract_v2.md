# Contrato de lote — V2, primeiro entregável

## Objetivo

A V2 propaga um mesmo identificador de lote desde a ingestão até o PostgreSQL. Cada lote representa um conjunto específico dos nove CSVs da Olist.

Lotes diferentes ficam em caminhos diferentes no MinIO e no schema `analytics_v2` do PostgreSQL. Assim, uma nova versão dos arquivos não substitui os dados de um lote anterior.

## Identificadores

### `batch_id`

`batch_id` identifica o conteúdo dos nove arquivos de origem.

Seu valor é calculado a partir de uma lista ordenada que contém, para cada arquivo:

- o nome do arquivo;
- o tamanho em bytes;
- o hash SHA-256 do conteúdo.

O horário da execução não participa desse cálculo.

Consequências:

- os mesmos nove arquivos produzem o mesmo `batch_id`;
- se qualquer arquivo mudar, o `batch_id` muda;
- todos os lotes devem conter os nove CSVs esperados;
- um lote parcial não deve ser processado.

### `run_id`

`run_id` identifica uma tentativa de executar uma etapa, como ingestão, processamento, curated ou carga no PostgreSQL.

Cada tentativa recebe um `run_id` novo. Repetir uma etapa mantém o mesmo `batch_id`, desde que os arquivos de origem sejam os mesmos.

## Caminhos dos objetos no MinIO

Os arquivos e resultados da V2 devem incluir o `batch_id` em seus caminhos:

```text
raw/batches/<batch_id>/<arquivo>.csv
processed/batches/<batch_id>/<tabela>.parquet
curated/batches/<batch_id>/<tabela>.parquet
```

Os manifestos devem ser separados por etapa, lote e tentativa:

```text
control/manifests/<etapa>/<batch_id>/<run_id>.json
```

Exemplo:

```text
raw/batches/b_<hash>/olist_orders_dataset.csv
processed/batches/b_<hash>/orders.parquet
curated/batches/b_<hash>/fact_orders.parquet
control/manifests/ingestion/b_<hash>/<run_id>.json
```

O prefixo `b_<hash>` representa o formato definido para `batch_id`. A implementação deve usar o mesmo valor em todas as etapas.

## Conteúdo dos manifestos

Cada manifesto deve registrar, no mínimo:

- `batch_id`;
- `run_id`;
- nome da etapa;
- estado da execução;
- início e término em UTC;
- objetos lidos;
- objetos produzidos;
- quantidade de linhas lidas e produzidas, quando aplicável;
- validações executadas e seus resultados;
- erro e etapa em que ocorreu, quando houver.

Os estados permitidos são:

- `running`: a etapa começou;
- `succeeded`: a etapa terminou com sucesso;
- `failed`: a etapa falhou.

Uma etapa seguinte só pode consumir uma saída quando o manifesto da etapa que a produziu indicar `succeeded`.

Cada tentativa grava seu manifesto em um caminho próprio. Uma nova tentativa não deve apagar o manifesto de uma tentativa anterior.

## Reexecução

- Repetir a ingestão dos mesmos arquivos produz o mesmo `batch_id`.
- O arquivo de um lote não deve ser gravado no caminho de outro lote.
- Repetir uma etapa pode atualizar os resultados daquele mesmo lote.
- Uma repetição não pode apagar nem alterar objetos de outro `batch_id`.
- Cada nova tentativa recebe um `run_id` e um manifesto próprios.
- O processamento deve ler os objetos raw pertencentes ao `batch_id` solicitado.
- A etapa curated deve ler os objetos processed pertencentes ao mesmo `batch_id`.
- A carga no PostgreSQL deve afetar somente os registros daquele `batch_id`.

Nesta primeira entrega, as etapas serão executadas sequencialmente. Não executar duas tentativas do mesmo lote ao mesmo tempo.

## PostgreSQL V2

Os objetos da V2 devem ser criados no schema `analytics_v2`. O schema `analytics` da V1 permanece separado.

As tabelas da V2 devem registrar `batch_id`. Na tabela de fatos de pedidos, uma linha representa um pedido dentro de um lote; a combinação de `batch_id` e `order_id` identifica unicamente essa linha.

Uma reexecução no PostgreSQL deve ocorrer em transação e afetar somente o lote informado. Ela não deve excluir nem substituir dados de outros lotes.

## Fora do escopo da integração local

A orquestração local do fluxo é feita pelo Airflow com acionamento manual e XCom somente para o batch_id. Composer, Kafka ou Pub/Sub, processamento incremental, Terraform, CI/CD e publicação na GCP permanecem fora deste entregável. Esses componentes podem ser acrescentados em entregáveis posteriores, usando o contrato de lote definido aqui.
