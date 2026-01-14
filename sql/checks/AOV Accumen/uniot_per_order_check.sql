with uop as (
Select
d.year_week,
MIN(o.order_purchase_date) as week_start_date,
oi.order_id,
count(distinct oi.order_id) as orders,
count(oi.order_item_id) as unit_per_order,
sum(price) as price_per_order
from fact_orders o
join fact_order_items oi on oi.order_id=o.order_id
join dim_date d on d.date=o.order_purchase_date
where  o.is_delivered=1
group by d.year_week,oi.order_id
),
aov as
(
select
year_week,
sum(orders) as total_orders,
sum(unit_per_order) as total_units,
sum(price_per_order) as price,
sum(unit_per_order)/sum(orders) as units_per_order,
sum(price_per_order)/sum(orders) as aov
from uop
group by year_week
)
select
year_week,
total_orders,
total_units,
total_units * 1.0 / total_orders AS units_per_order,
price,
LAG(price) over( order by year_week) as previous_week_price,
(price - LAG(price) over( order by year_week))/LAG(price) over( order by year_week) * 100 as wow_pct_change,
aov,
LAG(aov) over (order by year_week) as previous_aov,
(aov - LAG(aov) over (order by year_week))/LAG(aov) over (order by year_week) * 100 as wow_aov_pct_change
from aov
order by year_week


--Case 1 — Units/order ↓ roughly matches AOV ↓

--→ Basket thinning is a primary contributor

--Case 2 — Units/order ↓ slightly, AOV ↓ much more

--→ Basket thinning is secondary
--→ Product/category mix is still primary

--Case 3 — Units/order flat, AOV ↓

--→ Basket is NOT the cause
--→ Category or customer mix is the cause

--Anything else = misinterpretation.
