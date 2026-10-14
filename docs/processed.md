# Processamento: CSV raw para Parquet processed

Esta etapa lê as nove tabelas do lote no MinIO e grava um Parquet aprovado por tabela em `processed/batches/<BATCH_ID>/`. A camada raw permanece intacta. A qualidade é avaliada conforme `config/data_quality_v2.json`.

## Executar no PowerShell

Use o mesmo identificador de lote emitido pela ingestão:

```powershell
$batchId = "b_COLE_AQUI_O_ID_COMPLETO"
docker compose --profile jobs run --build --rm -e "BATCH_ID=$batchId" processing
```

A tarefa usa o ambiente Python do Compose e se conecta ao MinIO pela rede interna.

## Regras e decisões

- IDs e códigos postais são lidos como texto; sequências e valores numéricos recebem tipos próprios.
- Prefixos de CEP são normalizados para cinco caracteres. Valores fora do formato esperado geram aviso.
- Datas preenchidas que não puderem ser interpretadas viram nulas em processed e geram aviso; raw preserva o valor original.
- Duplicidades de chaves candidatas e referências órfãs são avaliadas em `enforce`. Linhas afetadas dentro dos limites são retiradas do Parquet aprovado e gravadas em quarentena.
- A quarentena precisa passar tanto o limite percentual da regra quanto o limite percentual agregado da tabela. A tabela conta linhas distintas, mesmo que uma linha viole mais de uma regra.
- Categoria ausente e categoria sem tradução em produtos são avisos; os produtos são preservados.
- Cada Parquet aprovado é conferido contra a contagem de linhas aprovada antes do envio. Linhas em quarentena são contadas separadamente.
- Limite excedido ou achado sem regra mapeada bloqueia a publicação dos Parquets aprovados daquela execução.

## Saídas e manifesto

Parquets aprovados:

```text
processed/batches/<BATCH_ID>/<tabela>.parquet
```

Quarentena:

```text
quarantine/batches/<BATCH_ID>/<tabela>/<run_id>.parquet
```

Manifesto de processamento:

```text
control/manifests/processing/<BATCH_ID>/<run_id>.json
```

Confira `status`, `quality.mode`, `quality.decision`, `quality.rules`, `quality.table_limits`, `quality.rows_by_table` e `quality.quarantine_objects`. Em uma execução `enforce` permitida, a soma de linhas aprovadas e quarentenadas deve ser igual às linhas de entrada por tabela.
