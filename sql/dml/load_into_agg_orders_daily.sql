BEGIN TRANSACTION;

DELETE FROM agg_orders_daily;

INSERT INTO agg_orders_daily (
    order_date,
    total_orders,
    non_canceled_orders,
    canceled_orders,
    cancellation_rate
)
SELECT
    order_purchase_date AS order_date,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN is_canceled = 0 THEN 1 ELSE 0 END) AS non_canceled_orders,
    SUM(CASE WHEN is_canceled = 1 THEN 1 ELSE 0 END) AS canceled_orders,
    SUM(CASE WHEN is_canceled = 1 THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(*), 0) AS cancellation_rate
FROM fact_orders
GROUP BY order_purchase_date;

COMMIT;
