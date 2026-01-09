
CREATE TABLE agg_orders_weekly_quality (
    year_week               CHAR(8) NOT NULL,
    week_start_date         DATE NOT NULL,
    total_orders            INT NOT NULL,
    delivered_orders        INT NOT NULL,
    canceled_orders         INT NOT NULL,
    cancellation_rate       DECIMAL(10,4) NOT NULL,
    CONSTRAINT pk_agg_orders_weekly
        PRIMARY KEY CLUSTERED (year_week)
);

with weekly_orders as
(
        select
        year_week,
        MIN(d.date) as week_start_date,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN o.is_delivered =1  THEN 1 ELSE 0 END) AS delivered_orders,
        SUM(CASE WHEN is_canceled = 1 THEN 1 ELSE 0 END) AS canceled_orders
from fact_orders o
        join dim_date d on d.date=o.order_purchase_date
        group by year_week
        having count( Distinct d.date)=7
)
INSERT INTO agg_orders_weekly_quality
(
        year_week,
        week_start_date,
        total_orders,
        delivered_orders,
        canceled_orders,
        cancellation_rate
)
SELECT
        year_week,
        week_start_date,
        total_orders,
        delivered_orders,
        canceled_orders,
        cast(canceled_orders as decimal(10,4))/cast(total_orders as decimal(10,4))*100 as cancellation_rate
from weekly_orders
