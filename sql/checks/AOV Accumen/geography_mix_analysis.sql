WITH weekly_geo_mix AS (
    SELECT
        d.year_week,
        MIN(d.date) AS week_start_date,
        c.customer_state,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(oi.price) AS gross_revenue
    FROM fact_orders o
    JOIN fact_order_items oi
        ON oi.order_id = o.order_id
    JOIN dim_customers c
        ON c.customer_id = o.customer_id
    JOIN dim_date d
        ON d.date = o.order_purchase_date
    WHERE o.is_delivered = 1
    GROUP BY
        d.year_week,
        c.customer_state
)
SELECT
    customer_state,
    sum(total_orders),
    sum(gross_revenue),
    sum(gross_revenue) / NULLIF(sum(total_orders), 0) AS state_aov,
    137.041585 as portfolio_aov
FROM weekly_geo_mix
group by customer_state
ORDER BY sum(gross_revenue) / NULLIF(sum(total_orders), 0)


