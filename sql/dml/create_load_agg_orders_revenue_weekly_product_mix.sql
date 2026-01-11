CREATE TABLE agg_orders_revenue_weekly_product_mix (
    year_week           CHAR(8)      NOT NULL,
    week_start_date     DATE         NOT NULL,
    product_category    VARCHAR(100) NOT NULL,

    total_orders        INT          NOT NULL,
    gross_revenue       DECIMAL(18,2) NOT NULL,
    avg_order_value     DECIMAL(18,2) NOT NULL,

    CONSTRAINT pk_agg_orders_rev_weekly_prod
        PRIMARY KEY CLUSTERED (year_week, product_category)
);




WITH valid_weeks AS (
    SELECT
        d.year_week,
        MIN(d.date) AS week_start_date
    FROM dim_date d
    GROUP BY d.year_week
    HAVING COUNT(DISTINCT d.date) = 7
),
weekly_product AS (
    SELECT
        v.year_week,
        v.week_start_date,
        p.product_category_name AS product_category,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(oi.price) AS gross_revenue
    FROM fact_order_items oi
    JOIN fact_orders o
        ON o.order_id = oi.order_id
    JOIN dim_date d
        ON d.date = o.order_purchase_date
    JOIN valid_weeks v
        ON v.year_week = d.year_week
    JOIN dim_product p
        ON p.product_id = oi.product_id
    WHERE o.is_delivered = 1
    GROUP BY
        v.year_week,
        v.week_start_date,
        p.product_category_name
)
INSERT INTO agg_orders_revenue_weekly_product_mix
SELECT
    year_week,
    week_start_date,
    product_category,
    total_orders,
    gross_revenue,
    gross_revenue / NULLIF(total_orders, 0) AS avg_order_value
FROM weekly_product;
