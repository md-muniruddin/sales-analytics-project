DECLARE @cutoff_date DATE = DATEADD(DAY, -14, CAST(GETDATE() AS DATE));

BEGIN TRANSACTION;

DELETE FROM agg_orders_daily
WHERE order_date >= @cutoff_date;

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
    SUM(CASE WHEN is_canceled = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_canceled = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_canceled = 1 THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(*), 0)
FROM fact_orders
WHERE order_purchase_date >= @cutoff_date
GROUP BY order_purchase_date;

COMMIT;
