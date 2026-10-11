# Contrato de lote — V1

## Definição

Um lote da V1 é uma execução sequencial do pipeline sobre um único snapshot dos nove CSVs públicos da Olist. O lote não é incremental: cada etapa processa o conteúdo disponível nos objetos de entrada daquele momento.

## Identificação e rastreabilidade

- A ingestão cria um `batch_id` em UTC e grava um manifesto em `raw/_manifests/<batch_id>.json`.
- O manifesto raw registra, por arquivo, nome, caminho do objeto, número de linhas, tamanho e SHA-256. O hash identifica o conteúdo efetivamente lido.
- Processed registra os hashes dos objetos raw lidos e dos Parquets gerados em `processed/_manifests/<run_id>.json`.
- Curated registra os hashes dos Parquets processed lidos, os Parquets de saída e as validações em `curated/_manifests/<run_id>.json`.
- A carga PostgreSQL registra início, fim, duração, objetos de entrada, contagens, validações e resultado em `warehouse/_manifests/<run_id>.json`.
- `batch_id` identifica a execução de ingestão; `run_id` identifica tentativas das etapas seguintes. A V1 não propaga um identificador comum por todas as etapas. Use os caminhos e hashes dos manifestos para acompanhar a linhagem.

## Ordem e publicação

Executar uma etapa por vez, nesta ordem: `ingestion → processing → curated → warehouse_load`. Só iniciar a etapa seguinte quando o comando anterior terminar com código zero e o manifesto correspondente indicar sucesso. Investigar falhas antes de prosseguir.

Curated bloqueia a publicação quando encontra chaves naturais ausentes ou duplicadas em clientes ou pedidos. Outras ocorrências, como inversões temporais, são preservadas e registradas para análise. O warehouse confirma a carga somente quando a transação e as validações terminam com sucesso.

## Reexecução e limites

- A ingestão não reenvia um arquivo cujo hash seja igual ao do objeto raw atual. Se o arquivo mudar, a chave raw fixa é atualizada e o novo manifesto registra o hash novo.
- Os objetos processed e curated têm caminhos fixos; uma nova execução os substitui. Os manifestos anteriores permanecem, mas não preservam versões antigas dos Parquets.
- A carga PostgreSQL é idempotente para reexecução do mesmo snapshot. O carregador compara as contagens finais às de entrada; um snapshot diferente que deixe linhas antigas no banco pode causar rollback.
- O bucket representa um único fluxo local. Não há isolamento de objetos por lote nem suporte a execuções concorrentes.
- Se qualquer CSV mudar, repetir o pipeline completo desde a ingestão. Não misturar etapas executadas sobre snapshots diferentes.

Este contrato é limitado ao snapshot estático da V1. Lotes imutáveis, concorrência, incrementalidade e propagação de um `batch_id` comum ficam para a V2.