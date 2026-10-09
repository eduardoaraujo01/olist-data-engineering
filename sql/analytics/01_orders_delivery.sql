-- Consultas analíticas da primeira fatia dimensional Olist.
-- Schema e grãos conforme sql/curated/01_create_orders_model.sql:
--   analytics.fact_orders: uma linha por pedido (order_id)
--   analytics.dim_customer: uma linha por customer_id
--   analytics.dim_order_status: uma linha por order_status
-- Não unir fatos de itens, pagamentos ou avaliações a fact_orders para estas
-- métricas; relações nesses grãos podem multiplicar pedidos.

-- Regras temporais aplicadas nas consultas:
-- 1. Sinalizar qualquer inversão entre os quatro eventos reais: compra,
--    aprovação, despacho ao transportador e entrega ao cliente.
-- 2. Sinalizar previsão anterior à compra.
-- 3. Métricas de entrega usam apenas status delivered com timestamps de compra,
--    aprovação, despacho, entrega e previsão presentes e sem inversões.
-- 4. Comparar prazo por timestamp completo (como definido no modelo); a
--    pontualidade é entrega real <= previsão.

-- 1) Pedidos por mês de compra e status.
WITH orders_analyzed AS (
    SELECT
        f.order_id,
        f.order_purchase_at,
        s.order_status,
        COALESCE((
            f.order_approved_at < f.order_purchase_at
            OR f.order_carrier_at < f.order_purchase_at
            OR f.order_carrier_at < f.order_approved_at
            OR f.order_delivered_at < f.order_purchase_at
            OR f.order_delivered_at < f.order_approved_at
            OR f.order_delivered_at < f.order_carrier_at
            OR f.order_estimated_delivery_at < f.order_purchase_at
            OR (s.order_status = 'delivered' AND (
                f.order_purchase_at IS NULL
                OR f.order_approved_at IS NULL
                OR f.order_carrier_at IS NULL
                OR f.order_delivered_at IS NULL
                OR f.order_estimated_delivery_at IS NULL
            ))
            OR (s.order_status <> 'delivered' AND f.order_delivered_at IS NOT NULL)
        ), FALSE) AS has_timestamp_issue
    FROM analytics.fact_orders AS f
    LEFT JOIN analytics.dim_order_status AS s
        ON s.order_status_key = f.order_status_key
)
SELECT
    date_trunc('month', order_purchase_at)::date AS purchase_month,
    order_status,
    count(*) AS orders_total,
    count(*) FILTER (WHERE NOT has_timestamp_issue) AS orders_without_timestamp_issue,
    count(*) FILTER (WHERE has_timestamp_issue) AS orders_flagged
FROM orders_analyzed
GROUP BY 1, 2
ORDER BY 1, 2;


-- 2) Pedidos e compradores únicos por mês.
-- Conta pessoas por customer_unique_id, não customer_id, conforme o modelo.
WITH orders_analyzed AS (
    SELECT
        f.order_id,
        f.customer_key,
        f.order_purchase_at,
        COALESCE((
            f.order_approved_at < f.order_purchase_at
            OR f.order_carrier_at < f.order_purchase_at
            OR f.order_carrier_at < f.order_approved_at
            OR f.order_delivered_at < f.order_purchase_at
            OR f.order_delivered_at < f.order_approved_at
            OR f.order_delivered_at < f.order_carrier_at
            OR f.order_estimated_delivery_at < f.order_purchase_at
        ), FALSE) AS has_timestamp_inversion
    FROM analytics.fact_orders AS f
)
SELECT
    date_trunc('month', o.order_purchase_at)::date AS purchase_month,
    count(*) AS orders_total,
    count(DISTINCT c.customer_unique_id) AS unique_buyers,
    count(*) FILTER (WHERE o.has_timestamp_inversion) AS orders_with_timestamp_inversion
FROM orders_analyzed AS o
LEFT JOIN analytics.dim_customer AS c
    ON c.customer_key = o.customer_key
GROUP BY 1
ORDER BY 1;


-- CTE compartilhada conceitualmente pelas duas próximas métricas:
-- pedido entregue elegível requer sequência cronológica completa e válida.
-- 3) Prazo médio e percentual no prazo, visão geral.
WITH orders_analyzed AS (
    SELECT
        f.order_id,
        f.order_purchase_at,
        f.order_approved_at,
        f.order_carrier_at,
        f.order_delivered_at,
        f.order_estimated_delivery_at,
        s.order_status,
        COALESCE((
            f.order_approved_at < f.order_purchase_at
            OR f.order_carrier_at < f.order_purchase_at
            OR f.order_carrier_at < f.order_approved_at
            OR f.order_delivered_at < f.order_purchase_at
            OR f.order_delivered_at < f.order_approved_at
            OR f.order_delivered_at < f.order_carrier_at
            OR f.order_estimated_delivery_at < f.order_purchase_at
            OR f.order_purchase_at IS NULL
            OR f.order_approved_at IS NULL
            OR f.order_carrier_at IS NULL
            OR f.order_delivered_at IS NULL
            OR f.order_estimated_delivery_at IS NULL
        ), TRUE) AS has_timestamp_issue
    FROM analytics.fact_orders AS f
    LEFT JOIN analytics.dim_order_status AS s
        ON s.order_status_key = f.order_status_key
    WHERE s.order_status = 'delivered'
), delivery_metrics AS (
    SELECT
        *,
        extract(epoch FROM (order_delivered_at - order_purchase_at)) / 86400.0
            AS delivery_days,
        order_delivered_at <= order_estimated_delivery_at AS delivered_on_time
    FROM orders_analyzed
    WHERE NOT has_timestamp_issue
)
SELECT
    (SELECT count(*) FROM delivery_metrics) AS delivered_orders_eligible,
    (SELECT count(*) FROM orders_analyzed WHERE has_timestamp_issue)
        AS delivered_orders_excluded_timestamp_issue,
    round(avg(delivery_days)::numeric, 2) AS avg_delivery_days,
    round(100.0 * count(*) FILTER (WHERE delivered_on_time)
        / NULLIF(count(*), 0), 2) AS delivered_on_time_pct
FROM delivery_metrics;


-- 4) Desempenho de entrega por estado do cliente.
-- O LEFT JOIN preserva pedidos sem cliente correspondente como UNKNOWN.
WITH orders_analyzed AS (
    SELECT
        f.order_id,
        f.customer_key,
        f.order_purchase_at,
        f.order_approved_at,
        f.order_carrier_at,
        f.order_delivered_at,
        f.order_estimated_delivery_at,
        s.order_status,
        COALESCE((
            f.order_approved_at < f.order_purchase_at
            OR f.order_carrier_at < f.order_purchase_at
            OR f.order_carrier_at < f.order_approved_at
            OR f.order_delivered_at < f.order_purchase_at
            OR f.order_delivered_at < f.order_approved_at
            OR f.order_delivered_at < f.order_carrier_at
            OR f.order_estimated_delivery_at < f.order_purchase_at
            OR f.order_purchase_at IS NULL
            OR f.order_approved_at IS NULL
            OR f.order_carrier_at IS NULL
            OR f.order_delivered_at IS NULL
            OR f.order_estimated_delivery_at IS NULL
        ), TRUE) AS has_timestamp_issue
    FROM analytics.fact_orders AS f
    LEFT JOIN analytics.dim_order_status AS s
        ON s.order_status_key = f.order_status_key
    WHERE s.order_status = 'delivered'
), delivery_metrics AS (
    SELECT
        o.*,
        COALESCE(c.customer_state::text, 'UNKNOWN') AS customer_state,
        extract(epoch FROM (o.order_delivered_at - o.order_purchase_at)) / 86400.0
            AS delivery_days,
        o.order_delivered_at <= o.order_estimated_delivery_at AS delivered_on_time
    FROM orders_analyzed AS o
    LEFT JOIN analytics.dim_customer AS c
        ON c.customer_key = o.customer_key
)
SELECT
    customer_state,
    count(*) FILTER (WHERE NOT has_timestamp_issue) AS delivered_orders_eligible,
    count(*) FILTER (WHERE has_timestamp_issue) AS delivered_orders_excluded_timestamp_issue,
    round((avg(delivery_days) FILTER (WHERE NOT has_timestamp_issue))::numeric, 2)
        AS avg_delivery_days,
    round(100.0 * count(*) FILTER (
        WHERE NOT has_timestamp_issue AND delivered_on_time
    ) / NULLIF(count(*) FILTER (WHERE NOT has_timestamp_issue), 0), 2)
        AS delivered_on_time_pct
FROM delivery_metrics
GROUP BY customer_state
ORDER BY delivered_orders_eligible DESC, customer_state;


-- 5) Auditoria resumida. As quatro primeiras contagens são inversões entre
-- eventos reais; atraso reproduz o controle de qualidade da fonte para todos
-- os pedidos que têm timestamp de entrega ao cliente, independente do status.
SELECT
    count(*) FILTER (WHERE order_approved_at < order_purchase_at)
        AS approved_before_purchase,
    count(*) FILTER (WHERE order_carrier_at < order_purchase_at)
        AS carrier_before_purchase,
    count(*) FILTER (WHERE order_carrier_at < order_approved_at)
        AS carrier_before_approval,
    count(*) FILTER (WHERE order_delivered_at < order_carrier_at)
        AS customer_delivery_before_carrier,
    count(*) FILTER (WHERE order_delivered_at::date > order_estimated_delivery_at::date)
        AS delivered_after_estimate_by_day
FROM (
    SELECT
        f.order_approved_at,
        f.order_purchase_at,
        f.order_carrier_at,
        f.order_delivered_at,
        f.order_estimated_delivery_at,
        s.order_status
    FROM analytics.fact_orders AS f
    LEFT JOIN analytics.dim_order_status AS s
        ON s.order_status_key = f.order_status_key
) AS orders_with_status;
