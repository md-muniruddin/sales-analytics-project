SELECT
    product_category,
    SUM(gross_revenue) AS total_revenue,
    AVG(avg_order_value) AS category_aov
FROM agg_orders_revenue_weekly_product_mix
GROUP BY product_category
ORDER BY total_revenue DESC;

---- checking the high & lov category revenue and category aov  to verify which category are the driver of aov decline

SELECT
    product_category,
    SUM(gross_revenue)        AS category_revenue,
    SUM(total_orders)         AS category_orders,
    SUM(gross_revenue) / SUM(total_orders) AS category_aov,
    (
 SELECT
    SUM(gross_revenue) / SUM(total_orders) AS portfolio_aov
FROM agg_orders_revenue_weekly_extended) as port_folio_aov,

case
     when (SUM(gross_revenue) / SUM(total_orders)) >= 136.83 Then 'Above' Else 'Below'
END as aov_status
FROM agg_orders_revenue_weekly_product_mix
GROUP BY product_category
ORDER BY category_revenue DESC;
---

SELECT
    year_week,
    product_category,
    SUM(gross_revenue) AS weekly_revenue
FROM agg_orders_revenue_weekly_product_mix
GROUP BY year_week, product_category
ORDER BY year_week;


--- for monthlu scale
SELECT
    DATEFROMPARTS(YEAR(o.order_purchase_timestamp),MONTH(o.order_purchase_timestamp),1) as month,
    product_category_name,
    COUNT(*) as items_sold,
    -- Calculate share of total items for this month
    COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (PARTITION BY DATEFROMPARTS(YEAR(o.order_purchase_timestamp),MONTH(o.order_purchase_timestamp),1)) as mix_share_pct,
    AVG(price) as avg_category_price
FROM fact_orders o
JOIN fact_order_items oi ON o.order_id = oi.order_id
JOIN dim_products p ON oi.product_id = p.product_id
WHERE is_delivered=1
GROUP BY DATEFROMPARTS(YEAR(o.order_purchase_timestamp),MONTH(o.order_purchase_timestamp),1),product_category_name
ORDER BY 1, 3 DESC;
