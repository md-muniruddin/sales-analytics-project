CREATE NONCLUSTERED INDEX idx_fact_order_items_order_agg
ON fact_order_items (order_id)
INCLUDE (price, freight_value);


CREATE NONCLUSTERED INDEX idx_fact_orders_date_cancel
ON fact_orders (order_purchase_date, is_canceled)
INCLUDE (order_id, order_delivered_customer_date, order_estimated_delivery_date);