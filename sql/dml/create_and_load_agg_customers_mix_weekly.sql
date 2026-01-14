CREATE TABLE agg_customers_mix_weekly (
    year_week           CHAR(8)      NOT NULL,
    week_start_date     DATE         NOT NULL,
    Customer_type    VARCHAR(100) NOT NULL,

    total_orders        INT          NOT NULL,
    gross_revenue       DECIMAL(18,2) NOT NULL,
    avg_order_value     DECIMAL(18,2) NOT NULL,

    CONSTRAINT pk_agg_customer_mix_weekly_prod
        PRIMARY KEY CLUSTERED (year_week, Customer_type)
);



with base_customer as (
    Select
        c.customer_id,
        o.order_id,
        c.customer_unique_id,
        MIN(o.order_purchase_date) over(partition by customer_unique_id) first_purchase_date,
        o.order_purchase_date,
        row_number() over (partition by c.customer_unique_id order by o.order_purchase_date) as rn

from fact_orders o
join dim_customers c on c.customer_id=o.customer_id
where o.is_delivered=1
)
,customer_clasification as (
select
    *,
    case
        when rn=1 then 'New' Else 'Repeat'
    ENd as Customer_type
from base_customer
)
,weekly_share_by_customer as (
    Select
        d.year_week,
        MIN(d.date) as week_start_date,
        cc.Customer_type,
        Count(Distinct cc.order_id) as Total_orders,
        Sum(oi.price) as Revenue,
        sum(oi.price)/Count(Distinct cc.order_id) as Customer_aov
from customer_clasification cc
join fact_order_items oi on oi.order_id=cc.order_id
join dim_date d on d.date=cc.order_purchase_date
group by d.year_week,cc.Customer_type
)
insert INTO agg_customers_mix_weekly
    select
        year_week,
        week_start_date,
        Customer_type,
        Total_orders,
        Revenue,
        Customer_aov
from weekly_share_by_customer