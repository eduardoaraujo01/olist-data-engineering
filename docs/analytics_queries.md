# Consultas analíticas Olist

Arquivo de consultas: `sql/analytics/01_orders_delivery.sql`.

## Modelo consultado

As consultas seguem o modelo PostgreSQL definido em `sql/curated/01_create_orders_model.sql`:

- `analytics.fact_orders`: uma linha por pedido; guarda timestamps, chaves de cliente e status, `delivery_days` e `delivered_on_time`.
- `analytics.dim_customer`: uma linha por `customer_id`, com `customer_unique_id` e UF.
- `analytics.dim_order_status`: uma linha por status.

As colunas de data são `TIMESTAMP WITHOUT TIME ZONE`. A data de compra é usada para agrupar os meses. Compradores são contados por `COUNT(DISTINCT customer_unique_id)`, como definido no contrato e no modelo dimensional.

## Consultas

1. Pedidos por mês e status: total, total sem anomalia temporal e total sinalizado.
2. Pedidos e compradores únicos por mês, com contagem de pedidos afetados por inversão de timestamps.
3. Prazo médio em dias e percentual no prazo para pedidos `delivered` elegíveis.
4. As mesmas métricas de entrega por UF do cliente; pedidos sem dimensão correspondente ficam como `UNKNOWN`.
5. Auditoria resumida para reconciliar inversões temporais e atraso após a data estimada.

As consultas mantêm os fatos no grão de pedido. Não unem fatos de itens, pagamentos ou avaliações, que podem ter várias linhas por pedido. Para a média e a pontualidade, são excluídos e contabilizados pedidos entregues com timestamps ausentes ou sequência impossível entre compra, aprovação, despacho, entrega e previsão anterior à compra. Uma entrega após a estimativa continua sendo atraso operacional, não inversão cronológica.

## Validação

A consulta 5 permite confrontar as inversões conhecidas no snapshot com o manifesto/relatório de qualidade. A cópia analisada registrou 166 despachos anteriores à compra, 1.359 anteriores à aprovação, 23 entregas ao cliente anteriores ao despacho e 6.535 timestamps de entrega ao cliente após o dia estimado. A última verificação inclui qualquer status com data de entrega preenchida, reproduzindo o escopo do relatório; as métricas operacionais nas consultas 3 e 4 consideram somente pedidos `delivered`. Os valores são referências do snapshot e podem mudar se os dados de entrada mudarem.

Os valores de `delivery_days` e `delivered_on_time` armazenados na fato são mantidos para rastreabilidade, mas as consultas de entrega recalculam essas métricas sobre datas válidas para não deixar inversões conhecidas distorcerem os resultados.
