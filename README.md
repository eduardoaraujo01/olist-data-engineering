# Olist Data Engineering Case

Este repositório documenta um pipeline local de Engenharia de Dados que transforma os dados públicos de pedidos da Olist em uma camada analítica reproduzível e em um dashboard para investigar demanda e desempenho de entrega.

O projeto cobre o fluxo desde a preservação e validação dos dados de origem até a modelagem dimensional, a carga repetível no warehouse e a construção de análises com definições, denominadores e limitações explícitos.

![Resumo do pipeline e da análise de entregas](docs/assets/visao-do-case.svg)

> Os números apresentados são referências do snapshot analisado. Setembro e outubro de 2018 têm cobertura parcial. A imagem resume resultados reais do case; não é uma captura de tela do Metabase.

## O problema analítico

Uma operação de e-commerce precisa entender como os pedidos evoluem e se as entregas chegam até a previsão. Um painel sozinho não responde à pergunta de negócio: é preciso definir o grão, o denominador, as regras para datas incompletas e o que uma taxa baixa permite — ou não permite — concluir.

## Escopo analítico do dashboard

- **Evolução de pedidos e compradores:** série mensal de pedidos e de compradores identificados por `customer_unique_id`, com sinalização dos meses de cobertura parcial.
- **Composição por status:** participação mensal dos status em gráfico empilhado em 100%; os rótulos da visualização são apresentados em português.
- **Desempenho das entregas:** quantidade de pedidos elegíveis, registros excluídos por datas ausentes ou inconsistentes, prazo médio e percentual entregue até a previsão.
- **Pontualidade por UF:** comparação da taxa de entrega no prazo junto ao tamanho de cada amostra. A visualização descreve diferenças observadas, sem atribuí-las a transportadoras ou causas que não estão identificadas na fonte.

## Leitura analítica do snapshot

![Escopo, resultados e limites das métricas de entrega](docs/assets/leituras-para-decisao.svg)

No snapshot analisado, 91,80% de 95.082 pedidos elegíveis chegaram até a data prevista, com prazo médio de 12,62 dias. Outros 1.396 pedidos entregues foram excluídos dessas métricas porque faltavam timestamps necessários ou a sequência cronológica era inválida; eles continuam visíveis na auditoria de qualidade.

O RJ é um ponto inicial para investigação: teve 12.210 pedidos elegíveis, pontualidade de 86,40% e prazo médio de 15,38 dias — 5,4 pontos percentuais abaixo do resultado geral. MA (79,97%; 704 pedidos) e AL (76,34%; 393 pedidos) também tiveram taxas menores, mas com bases menores. A decisão prudente é validar esses recortes e procurar evidências operacionais antes de priorizar uma intervenção. O dashboard identifica onde olhar; não prova por que o atraso ocorreu.

## Arquitetura e fluxo

![Etapas do pipeline local de Engenharia de Dados](docs/assets/arquitetura-pipeline.svg)

| Etapa | Responsabilidade | Evidência / cuidado |
|---|---|---|
| Origem | Nove CSVs públicos da Olist | Os originais não são alterados nem versionados junto ao código. |
| Ingestão e raw | Python copia os arquivos para MinIO e registra hashes e manifestos | Uma nova execução não reenvia arquivos sem mudança. |
| Processed | Tipagem, validações e Parquet por arquivo | Preserva o grão original; ausências e anomalias são contadas. |
| Curated | Dimensões e fato de pedidos em Parquet | Camada preparada para análise, sem descartar silenciosamente registros problemáticos. |
| Warehouse | PostgreSQL, schema `analytics` | Carga repetível com chaves resolvidas e validação de integridade referencial. |
| Consumo | SQL analítico e Metabase | Indicadores com definições, contexto e limites legíveis para decisão. |

## Decisões de engenharia demonstradas

- **Grão explícito:** `fact_orders` contém uma linha por pedido (`order_id`). Compradores únicos são contados por `customer_unique_id`, que pode aparecer em mais de um pedido.
- **Fatos não são misturadas:** itens, pagamentos e avaliações podem ter várias linhas por pedido. Juntá-las diretamente pode multiplicar linhas e inflar totais; a V1 mantém o foco em pedidos e entrega.
- **Qualidade rastreável:** inversões entre compra, aprovação, despacho, entrega e previsão são sinalizadas. Métricas de entrega incluem somente pedidos entregues com timestamps necessários presentes e cronologia válida.
- **Atraso não é anomalia temporal:** entrega depois da previsão é um atraso operacional; não é removida como se fosse uma inversão de datas.
- **Integridade e reexecução:** dimensões são carregadas antes da fato, chaves são resolvidas no PostgreSQL e a carga foi reexecutada com o mesmo snapshot sem duplicar pedidos ou produzir órfãos.
- **Apresentação separada dos códigos de origem:** os status originais continuam sendo valores de dados; a pergunta usada na visualização converte os rótulos para português para facilitar a leitura.

## Resultados de validação

Resultados observados na cópia analisada, não constantes esperadas para qualquer versão futura do dataset:

| Verificação | Resultado |
|---|---:|
| Pedidos na fato | 99.441 |
| Linhas únicas por pedido | 99.441 |
| Clientes / datas / status na camada dimensional | 99.441 / 800 / 8 |
| Órfãos nas referências de cliente, status e data | 0 |
| Pedidos entregues elegíveis para as métricas de prazo | 95.082 |
| Pedidos entregues excluídos por datas ausentes/inconsistentes | 1.396 |
| Entregas até a previsão / prazo médio | 91,80% / 12,62 dias |

A auditoria também encontrou 166 despachos anteriores à compra, 1.359 anteriores à aprovação e 23 entregas ao cliente anteriores ao despacho. O controle de atraso após a previsão identificou 6.535 timestamps de entrega posteriores ao dia estimado no escopo da auditoria. Esses grupos têm definições e escopos próprios; não devem ser somados como se fossem necessariamente pedidos distintos.

## Explorar por período de compra

Na instância local do Metabase, o dashboard tem o filtro **Período da compra**, uma faixa de datas baseada em `fact_orders.order_purchase_at`. Ele está conectado às cinco visualizações de dados: pedidos e compradores por mês, pedidos por status, total de pedidos, indicadores gerais de entrega e comparação por UF. Assim, volume e métricas de entrega são recalculados para os pedidos comprados no intervalo selecionado; os indicadores de entrega continuam considerando apenas pedidos entregues elegíveis e mantêm os excluídos por timestamps separados.

O filtro foi conferido com 1º de janeiro a 31 de março de 2018: as séries exibiram somente esses três meses, o total foi 21.208 pedidos e os indicadores de entrega retornaram 20.589 elegíveis, 38 excluídos, 15,75 dias de prazo médio e 85,40% no prazo. Ao limpar o filtro, o painel retorna ao snapshot completo.

As caixas de texto **Cobertura dos dados** e **Leituras para decisão** são interpretações fixas do snapshot completo e não acompanham a faixa selecionada. Considere-as como contexto de referência; para uma análise de período específico, use os cartões numéricos e gráficos que respondem ao filtro. A configuração do dashboard permanece na instância local do Metabase e não é exportada junto ao repositório nesta versão.

## Escopo e limitações

- A V1 é local: Docker Compose, MinIO, Python, Parquet, PostgreSQL e Metabase. Nuvem, Airflow, cargas incrementais e histórico SCD estão fora do escopo atual.
- Setembro e outubro de 2018 são meses parciais; a redução aparente no fim das séries não deve ser lida como queda de demanda ou mudança de operação.
- A taxa de pontualidade descreve pedidos entregues elegíveis, não todos os pedidos.
- A UF é a do cliente. Não há identificador de transportadora nem evidência de causa raiz.
- Na análise do catálogo, algumas categorias não tinham tradução na tabela de referência; os valores de origem foram preservados em vez de traduzidos por suposição.
- O dashboard não substitui investigação operacional nem estima custo de atraso; essas perguntas exigem dados adicionais e uma definição de negócio.

## Reproduzir e explorar

O roteiro operacional completo está em [docs/curated.md](docs/curated.md). Ele explica como configurar o ambiente, executar cada etapa na ordem, aplicar o esquema dimensional e conferir os manifestos.

Na raiz do projeto, no PowerShell, execute:

```powershell
Copy-Item .env.example .env
# Substitua as senhas de desenvolvimento no .env antes de iniciar os serviços.
docker compose up -d minio postgres
```

Crie no MinIO Console (`http://localhost:9001`) o bucket definido em `MINIO_BUCKET`, caso ele ainda não exista. Coloque os nove CSVs em `data/source/` e execute as etapas sequencialmente:

```powershell
docker compose --profile jobs run --build --rm ingestion
docker compose --profile jobs run --build --rm processing
docker compose --profile jobs run --build --rm curated
docker compose exec -T postgres psql -U olist -d olist_analytics -v ON_ERROR_STOP=1 -f /docker-entrypoint-initdb.d/01_create_orders_model.sql
docker compose --profile jobs run --build --rm warehouse_load
```

Os argumentos `olist` e `olist_analytics` correspondem aos valores padrão de `POSTGRES_USER` e `POSTGRES_DB`; ajuste-os se o `.env` usar outros valores. O arquivo `.env` e os CSVs locais não devem ser versionados.

O contrato da execução, a identificação por hashes, a reexecução e os limites de concorrência estão em [docs/batch_contract.md](docs/batch_contract.md).

Documentação e código:

- [Contrato de dados](docs/data_contract.md)
- [Contrato de lote](docs/batch_contract.md)
- [Modelo dimensional](docs/dimensional_model.md)
- [Ingestão](docs/ingestion.md)
- [Processamento](docs/processed.md)
- [Geração da camada curated](docs/curated.md)
- [Warehouse](docs/warehouse.md)
- [Definições e validação das consultas](docs/analytics_queries.md)
- [Consultas analíticas SQL](sql/analytics/01_orders_delivery.sql)

## Evoluções futuras

A V1 concentra-se em pedidos e desempenho de entrega. Evoluções possíveis incluem tornar as notas executivas sensíveis ao período selecionado e ampliar o modelo para itens, pagamentos e avaliações. Cada conjunto deve permanecer em uma fato com grão próprio; comparações entre fatos exigem agregação prévia a um grão comum.
