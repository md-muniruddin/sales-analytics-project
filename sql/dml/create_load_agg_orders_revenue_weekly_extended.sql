CREATE TABLE agg_orders_revenue_weekly_extended (
    year_week                       CHAR(8)     NOT NULL,
    week_start_date                 DATE        NOT NULL,

    total_orders                    INT         NOT NULL,
    delivered_orders                INT         NOT NULL,

    gross_revenue                   DECIMAL(18,2) NOT NULL,
    avg_order_value                 DECIMAL(18,2) NOT NULL,
    revenue_per_delivered_order     DECIMAL(18,2) NULL,

    CONSTRAINT pk_agg_orders_revenue_weekly_ext
        PRIMARY KEY CLUSTERED (year_week)
);

WITH weekly_base AS (
    SELECT
        d.year_week,
        MIN(d.date) AS week_start_date,

        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(CASE WHEN o.is_delivered = 1 THEN 1 ELSE 0 END) AS delivered_orders
    FROM fact_orders o
    JOIN dim_date d
        ON d.date = o.order_purchase_date
    GROUP BY d.year_week
    HAVING COUNT(DISTINCT d.date) = 7
),
weekly_revenue AS (
    SELECT
        d.year_week,
        SUM(oi.price) AS gross_revenue
    FROM fact_order_items oi
    JOIN fact_orders o
        ON o.order_id = oi.order_id
    JOIN dim_date d
        ON d.date = o.order_purchase_date
    GROUP BY d.year_week
)
INSERT INTO agg_orders_revenue_weekly_extended (
    year_week,
    week_start_date,
    total_orders,
    delivered_orders,
    gross_revenue,
    avg_order_value,
    revenue_per_delivered_order
)
SELECT
    b.year_week,
    b.week_start_date,
    b.total_orders,
    b.delivered_orders,
    r.gross_revenue,

    CAST(r.gross_revenue AS DECIMAL(18,2)) /
    CAST(b.total_orders AS DECIMAL(18,2)) AS avg_order_value,

    CASE
        WHEN b.delivered_orders = 0 THEN NULL
        ELSE
            CAST(r.gross_revenue AS DECIMAL(18,2)) /
            CAST(b.delivered_orders AS DECIMAL(18,2))
    END AS revenue_per_delivered_order
FROM weekly_base b
JOIN weekly_revenue r
    ON b.year_week = r.year_week;
