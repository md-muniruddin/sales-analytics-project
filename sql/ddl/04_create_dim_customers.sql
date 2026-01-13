Create Table dim_customers(
customer_id VARCHAR(50) NOT NULL,
customer_unique_id VARCHAR(50) NOT NULL,
customer_zip_code_prefix INT NOT NULL,
customer_city VARCHAR(50) NOT NULL,
customer_state VARCHAR(50) NOT NULL,
customer_city_en VARCHAR(50) NOT NULL,
CONSTRAINT pk_customer_id_dim_customer
PRIMARY Key (customer_id)
)

create INDEX idx_customer_id on dim_customers(customer_id)