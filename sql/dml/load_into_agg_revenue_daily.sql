
BEGIN TRANSACTION;

DELETE FROM agg_revenue_daily;

WITH order_revenue AS (
    SELECT
        oi.order_id,
        COUNT(*) AS items_in_order,
        SUM(oi.price) AS order_price,
        SUM(oi.freight_value) AS order_freight,
        SUM(oi.price + oi.freight_value) AS order_gross_revenue
    FROM fact_order_items oi
    GROUP BY oi.order_id
)
INSERT INTO agg_revenue_daily (
    order_date,
    gross_revenue,
    total_freight,
    aov,
    avg_items_per_order
)
SELECT
    o.order_purchase_date AS order_date,
    SUM(r.order_gross_revenue) AS gross_revenue,
    SUM(r.order_freight) AS total_freight,
    SUM(r.order_gross_revenue) * 1.0
        / NULLIF(COUNT(o.order_id), 0) AS aov,
    AVG(r.items_in_order * 1.0) AS avg_items_per_order
FROM fact_orders o
JOIN order_revenue r
    ON o.order_id = r.order_id
WHERE o.is_canceled = 0
GROUP BY o.order_purchase_date;

COMMIT;
