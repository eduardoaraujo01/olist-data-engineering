# Primeira ingestão: CSVs para a camada raw

Esta etapa copia os nove CSVs esperados de data/source para o bucket olist-data-lake no MinIO. Os arquivos são enviados sem limpeza nem conversão: raw preserva a origem.

## Executar no PowerShell

Na pasta raiz do projeto, execute:

    docker compose --profile jobs run --build --rm ingestion

O Docker constrói um pequeno ambiente Python para esta tarefa, instala as dependências e executa a ingestão. Você não precisa instalar Python no Windows. O MinIO precisa estar ativo, o bucket olist-data-lake precisa existir e os nove CSVs devem estar em data/source.

## O que o script registra

Os objetos ficam em raw/<nome-do-arquivo>. Para cada arquivo, o manifesto registra número de linhas (sem contar o cabeçalho), tamanho em bytes, chave no MinIO e hash SHA-256. Cada execução cria um manifesto em raw/_manifests/.

Ao executar novamente, um arquivo com o mesmo hash não é reenviado. Se o conteúdo local mudar, o objeto correspondente em raw/ é atualizado; o manifesto da nova execução registra o novo hash. Os CSVs continuam fora do Git e também são excluídos do contexto enviado para construir a imagem Python.
