# Modelo dimensional — Olist

**Status:** implementado para a fatia de pedidos e entregas. Raw e processed preservam as nove tabelas de origem; curated gera as dimensões de cliente, data e status do pedido, além de `fact_orders`; PostgreSQL e o dashboard local do Metabase consomem essa primeira fatia analítica. Itens, pagamentos e avaliações permanecem como extensões documentadas, não como fatos implementados na V1.

## Objetivo do case

Construir uma visão analítica reproduzível do e-commerce da Olist, partindo dos CSVs públicos, preservando a origem e deixando explícitos o significado das métricas e os limites das junções.

O modelo deve ajudar a responder:

1. **Como a demanda evolui?** Quantos pedidos e compradores únicos aparecem por mês, estado e situação do pedido?
2. **Quais produtos, categorias e vendedores concentram o valor dos itens?** Como esse valor e o frete variam ao longo do tempo?
3. **Como está o desempenho de entrega?** Qual é o prazo entre a compra e a entrega, e qual proporção dos pedidos entregues chegou até a data estimada?
4. **O que os dados de pagamento e avaliação mostram?** Quanto foi registrado em pagamentos por método e qual é a distribuição das notas? Essas perguntas usam fatos separados para não multiplicar linhas.

Essas são perguntas analíticas escolhidas para demonstrar modelagem, qualidade e SQL em uma entrevista. Como não há uma área de negócio validando o case, as definições abaixo são **hipóteses técnicas e analíticas do portfólio**, não regras oficiais da Olist.

## Arquitetura

CSV em data/source  
↓ ingestão sem alteração  
MinIO: raw/  
↓ tipagem e validações  
MinIO: processed/ (Parquet por tabela)  
↓ modelo dimensional e métricas  
MinIO: curated/ (Parquet para análise)  
↓ carga analítica  
PostgreSQL  
↓ consultas e visualizações  
Metabase

Cada camada tem uma função. raw guarda o arquivo recebido; processed aplica tipos e registra verificações sem excluir linhas; curated organiza os dados para responder perguntas analíticas. PostgreSQL será a camada de consumo SQL e Metabase, a interface de dashboards.

## Fatos e grão

**Grão** é a descrição precisa do que uma linha significa. Métricas só podem ser somadas ou comparadas depois de identificar esse grão.

| Fato proposto | O que uma linha representa | Chave natural candidata | Fonte principal | Medidas e atributos |
|---|---|---|---|---|
| fact_orders | Um pedido | order_id | orders, com dados de cliente por customer_id | order_count = 1, estado do pedido, datas de compra/aprovação/entrega/estimativa, prazo de entrega e indicador de pontualidade |
| fact_order_items | Um item de um pedido | order_id + order_item_id | order_items, com contexto de pedido, cliente e datas | item_count = 1, price e freight_value |
| fact_order_payments | Uma parcela ou sequência de pagamento de um pedido | order_id + payment_sequential | order_payments, com contexto do pedido | payment_value, payment_type e payment_installments |
| fact_order_reviews | Uma avaliação associada a um pedido | review_id + order_id | order_reviews, com contexto do pedido | review_score, texto da avaliação e datas da avaliação/resposta |

As chaves acima são **chaves naturais da fonte** e candidatas a unicidade; a carga deve verificá-las. No Parquet de curated, manter essas chaves facilita rastrear cada linha até a origem. No PostgreSQL, a dimensão pode receber uma chave substituta numérica e conservar a chave da Olist como coluna única de referência.

As quatro fatos não devem ser unidas diretamente umas às outras por order_id. Um pedido pode ter vários itens, vários pagamentos e mais de uma avaliação. Unir esses detalhes antes de agregá-los pode multiplicar as linhas e inflar somas. Para comparar, por exemplo, valor de itens e pagamentos, agregue cada fato separadamente ao mesmo grão — uma linha por pedido — e só então compare os resultados.

## Dimensões

| Dimensão proposta | Grão e chave natural | Atributos principais | Observações |
|---|---|---|---|
| dim_customer | Uma linha por customer_id | customer_unique_id, cidade, estado e prefixo de CEP | customer_id identifica a linha da tabela da fonte; customer_unique_id pode se repetir. Para contar compradores distintos, usar COUNT(DISTINCT customer_unique_id), não contar linhas da dimensão. |
| dim_product | Uma linha por product_id | Categoria original, nome da categoria traduzido, dimensões e peso do produto | A tradução é uma junção à esquerda pela categoria e só deve ocorrer depois de validar que a tabela de tradução tem uma linha por categoria. Preservar o nome original. |
| dim_seller | Uma linha por seller_id | Cidade, estado e prefixo de CEP | Não anexar diretamente geolocalização: um CEP pode ter várias coordenadas. |
| dim_date | Uma linha por data do calendário | Data, ano, trimestre, mês, dia da semana e indicador de fim de semana | Pode ser usada em papéis diferentes: compra, entrega, previsão e avaliação. As datas de origem continuam disponíveis nas fatos. |
| dim_order_status | Uma linha por valor de order_status | Código/status e descrição aprovada para o case | Nesta versão, não traduzir nem agrupar status sem documentar a regra. |

customer_unique_id representa melhor a identidade do comprador para análises de recorrência, mas não é chave primária em olist_customers_dataset.csv. A dimensão mantém o grão e a chave candidata declarados no contrato. Criar uma dimensão deduplicada por customer_unique_id exigiria decidir como tratar vários customer_id e possíveis diferenças de endereço; essa regra não será inventada nesta versão.

## Relações permitidas para montar as fatos

As junções abaixo são propostas porque o lado de referência tem uma chave candidata única. Antes de cada junção, validar a unicidade dessa chave; depois, conferir que o número de linhas da fato permaneceu igual.

- orders.customer_id → customers.customer_id
- order_items.order_id → orders.order_id
- order_items.product_id → products.product_id
- order_items.seller_id → sellers.seller_id
- order_payments.order_id → orders.order_id
- order_reviews.order_id → orders.order_id
- products.product_category_name → product_category_name_translation.product_category_name

As relações de itens, pagamentos e avaliações com pedidos são de muitos para um: cada linha filha recebe atributos do pedido, sem agregar nem perder o grão da tabela filha. A geolocalização fica fora das dimensões desta versão até que exista uma regra aprovada para resumir várias coordenadas por prefixo de CEP.

## Definição das métricas

As definições abaixo deixam claro o que cada número mede. Elas são hipóteses do case e devem aparecer nos nomes, descrições ou filtros do dashboard.

| Métrica | Cálculo proposto | Limite de interpretação |
|---|---|---|
| Pedidos | Contar linhas de fact_orders | Um pedido continua contado mesmo se seu status for cancelado; mostrar a distribuição por status. |
| Compradores únicos | Contar customer_unique_id distintos em dim_customer ou na fato apropriada | Não contar customer_id como pessoa distinta. |
| Itens | Contar linhas de fact_order_items | A fato deve manter uma linha por item. |
| Valor bruto dos itens | Somar price em fact_order_items | Não chamar de receita líquida ou valor pago. Não inclui frete e não aplica regras contábeis. |
| Frete registrado | Somar freight_value em fact_order_items | Mostrar separado do valor dos itens. |
| Valor de pagamentos | Somar payment_value em fact_order_payments | Não somar diretamente com price; usar como comparação/reconciliação em nível de pedido. |
| Prazo de entrega | Diferença entre order_delivered_customer_date e order_purchase_timestamp | Calcular somente para pedidos entregues com as duas datas válidas. |
| Entrega no prazo | Entrega até order_estimated_delivery_date | Denominador: pedidos entregues com data real e estimada válidas. Datas ausentes não são consideradas atraso nem pontualidade. |
| Nota média | Média de review_score em fact_order_reviews | Média no grão da avaliação; não duplicar notas ao juntá-las aos itens. |

Para comparar pagamentos com valor dos itens e frete, agregar cada fato por order_id. Diferenças devem ser registradas para investigação; não presumir que sejam erros nem forçar os totais a coincidir.

## Regras para a camada curated

1. Criar as dimensões e fatos a partir dos Parquets de processed/, não voltar aos CSVs como fonte de transformação.
2. Preservar o grão de cada fato e validar suas chaves candidatas.
3. Antes de uma junção, verificar unicidade da chave do lado referenciado. Depois, comparar a contagem de linhas antes e depois.
4. Não remover linhas com chaves duplicadas, datas inválidas, referências sem correspondência ou categorias não traduzidas. Registrar as ocorrências para revisão.
5. Manter valores ausentes como ausentes. Para datas preenchidas que falharem na conversão, preservar o CSV em raw e registrar a exceção no relatório de qualidade.
6. Não aplicar a geolocalização a clientes/vendedores até que a regra de agregação por CEP seja definida.
7. Manter os fatos separados. Se um dashboard precisar cruzar fatos de grãos diferentes, agregar cada fato ao grão comum antes da comparação.
8. Como a fonte é um snapshot histórico, a primeira carga no PostgreSQL pode ser uma recarga completa reproduzível. Incrementalidade e histórico de mudanças dimensionais (SCD) ficam fora do escopo da V1.

### Categorias sem tradução

O processamento encontrou **13 linhas de produtos** cujos nomes de categoria não encontram tradução, incluindo pc_gamer e portateis_cozinha_e_preparadores_de_alimentos. A dimensão de produtos deve manter product_category_name original e deixar product_category_name_english nulo nesses casos. No dashboard, pode-se exibir o nome original ou um rótulo de apresentação como “Sem tradução”, sem gravar uma tradução inventada.

## Validações de aceite

Uma implementação de curated estará pronta para carga quando:

- cada dimensão tiver no máximo uma linha por chave natural definida;
- fact_orders tiver o mesmo número de linhas de orders;
- fact_order_items tiver o mesmo número de linhas de order_items;
- fact_order_payments tiver o mesmo número de linhas de order_payments;
- fact_order_reviews tiver o mesmo número de linhas de order_reviews;
- cada fato mantiver uma linha por chave candidata, sem duplicação causada por junção;
- referências sem correspondência e campos nulos estiverem contabilizados em relatório;
- totais de itens, frete e pagamentos forem calculados em suas fatos próprias;
- a camada raw permanecer inalterada.

Como referência para a cópia atual, processed contém 99.441 pedidos, 112.650 itens, 103.886 pagamentos, 99.224 avaliações, 32.951 produtos, 99.441 linhas de clientes, 3.095 vendedores, 1.000.163 linhas de geolocalização e 71 traduções de categoria. Esses números são uma verificação da versão analisada, não uma garantia para outras cópias do dataset.

## Fora do escopo e decisões pendentes

- Definição contábil de receita, GMV, descontos, estornos e cancelamentos.
- Regra para reconciliar payment_value com itens e frete.
- Política geográfica para escolher/agregar coordenadas por CEP.
- Deduplicação por customer_unique_id e histórico de endereços.
- Fuso horário comercial das datas; no case, as datas ficam como timestamps sem conversão de fuso.
- Carga incremental, dimensões lentamente mutáveis e atualização automática.
- Traduções adicionais para categorias sem correspondência.

Esses limites são úteis na apresentação: mostram quais decisões foram tomadas para a V1 e quais dependem de uma definição de negócio que o dataset, sozinho, não fornece.
