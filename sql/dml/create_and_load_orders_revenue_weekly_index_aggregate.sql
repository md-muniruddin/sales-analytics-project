CREATE TABLE agg_orders_revenue_weekly_indexed (
    year_week               CHAR(8)     NOT NULL,
    week_start_date         DATE        NOT NULL,

    total_orders            INT         NOT NULL,
    gross_revenue           DECIMAL(18,2) NOT NULL,

    orders_index_base_100   DECIMAL(10,2) NOT NULL,
    revenue_index_base_100  DECIMAL(10,2) NOT NULL,

    CONSTRAINT pk_agg_weekly_indexed
        PRIMARY KEY CLUSTERED (week_start_date)
);




WITH weekly_orders AS (
    SELECT
        d.year_week,
        MIN(d.date) AS week_start_date,
        COUNT(DISTINCT d.date) AS days_in_week,
        SUM(o.total_orders) AS total_orders
    FROM agg_orders_daily o
    JOIN dim_date d
        ON o.order_date = d.date
    GROUP BY d.year_week
),
weekly_revenue AS (
    SELECT
        d.year_week,
        MIN(d.date) AS week_start_date,
        SUM(r.gross_revenue) AS gross_revenue
    FROM agg_revenue_daily r
    JOIN dim_date d
        ON r.order_date = d.date
    GROUP BY d.year_week
),
weekly_combined AS (
    SELECT
        o.year_week,
        o.week_start_date,
        o.total_orders,
        r.gross_revenue,
        o.days_in_week
    FROM weekly_orders o
    JOIN weekly_revenue r
        ON o.year_week = r.year_week
),
base_values AS (
    SELECT TOP 1
        total_orders  AS base_orders,
        gross_revenue AS base_revenue
    FROM weekly_combined
    WHERE days_in_week = 7
      AND total_orders > 0
    ORDER BY week_start_date
)
INSERT INTO agg_orders_revenue_weekly_indexed (
    year_week,
    week_start_date,
    total_orders,
    gross_revenue,
    orders_index_base_100,
    revenue_index_base_100
)
SELECT
    w.year_week,
    w.week_start_date,
    w.total_orders,
    w.gross_revenue,
    ROUND((CAST(w.total_orders AS DECIMAL(18,4)) / b.base_orders) * 100, 2),
    ROUND((w.gross_revenue / b.base_revenue) * 100, 2)
FROM weekly_combined w
CROSS JOIN base_values b
WHERE w.days_in_week = 7;
