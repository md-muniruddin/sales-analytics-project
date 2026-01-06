DECLARE @cutoff_date DATE = DATEADD(DAY, -14, CAST(GETDATE() AS DATE));

BEGIN TRANSACTION;

DELETE FROM agg_revenue_daily
WHERE order_date >= @cutoff_date;

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
    o.order_purchase_date,
    SUM(r.order_gross_revenue),
    SUM(r.order_freight),
    SUM(r.order_gross_revenue) * 1.0 / NULLIF(COUNT(o.order_id), 0),
    AVG(r.items_in_order * 1.0)
FROM fact_orders o
JOIN order_revenue r
    ON o.order_id = r.order_id
WHERE
    o.is_canceled = 0
    AND o.order_purchase_date >= @cutoff_date
GROUP BY o.order_purchase_date;

COMMIT;
