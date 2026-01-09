/* =========================================================
   FACT TABLE: fact_orders
   Grain: 1 row = 1 order
   Purpose: order volume, lifecycle, funnel analysis
   ========================================================= */



CREATE TABLE fact_orders (
    order_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    order_status VARCHAR(20) NOT NULL,

    -- Analytics-friendly date (for BI & joins)
    order_purchase_date DATE NOT NULL,

    -- Operational timestamp (for detailed analysis)
    order_purchase_timestamp DATETIME2 NOT NULL,

    order_approved_at DATETIME2 NULL,
    order_delivered_carrier_date DATETIME2 NULL,
    order_delivered_customer_date DATETIME2 NULL,
    order_estimated_delivery_date DATETIME2 NULL,

    -- Flags (non-destructive)
    has_approved_date BIT NOT NULL,
    has_carrier_date BIT NOT NULL,
    has_delivery_date BIT NOT NULL,
    is_canceled BIT NOT NULL,
    is_unavailable BIT NOT NULL,,
    is_delivered BIT NOT NULL,


    CONSTRAINT pk_fact_orders
        PRIMARY KEY (order_id)
);

-- Indexes for common access patterns
CREATE INDEX idx_fact_orders_purchase_date
    ON fact_orders (order_purchase_date);

CREATE INDEX idx_fact_orders_status
    ON fact_orders (order_status);
