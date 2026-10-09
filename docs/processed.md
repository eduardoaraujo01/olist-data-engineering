# Processamento: CSV raw para Parquet processed

Esta etapa lê as nove tabelas da camada raw no MinIO e grava um Parquet por tabela em processed/. A camada raw permanece intacta.

## Executar no PowerShell

Na pasta raiz do projeto, execute:

    docker compose --profile jobs run --build --rm processing

A tarefa usa o mesmo ambiente Python do serviço de ingestão e se conecta ao MinIO pela rede interna do Compose.

## Regras desta versão

- IDs e códigos postais são lidos como texto; chaves numéricas de sequência e valores numéricos recebem tipos próprios.
- Prefixos de CEP são normalizados para cinco caracteres. Valores que continuarem fora do formato esperado são sinalizados.
- Datas são convertidas para data/hora. Datas preenchidas que não puderem ser interpretadas são sinalizadas e ficam nulas em processed; raw preserva o valor original.
- Valores ausentes não são preenchidos e nenhuma linha é removida.
- Tabelas permanecem separadas, sem JOIN. Isso mantém o grão da fonte e evita multiplicação de linhas nesta camada.
- Duplicidades de chaves candidatas, referências sem correspondência e categorias sem tradução são reportadas, não corrigidas automaticamente.
- O número de linhas do Parquet é comparado ao número lido do CSV antes de enviar o arquivo.

Os arquivos Parquet são gravados em processed/<nome-da-tabela>.parquet. O manifesto da execução fica em processed/_manifests/ e reúne tipos de coluna, nulos, hashes, integridade referencial e problemas para revisão.
