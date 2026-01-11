Create table dim_products(
product_id VARCHAR(50) NOT NULL,
product_category_name VARCHAR(50) NUll,
product_name_length DECIMAL(10,2)  NULL,
product_description_length DECIMAL(10,2)  NULL,
product_photos_qty DECIMAL(10,2) NULL,
product_weight_g DECIMAL(10,2) NOT NULL,
product_length_cm DECIMAL(10,2) NOT NULL,
product_height_cm DECIMAL(10,2) NOT NULL,
product_width_cm DECIMAL(10,2) NOT NULL,
has_Unknown_category BIT NOT NULL,
CONSTRAINT pk_dim_products PRIMARY KEY (product_id)

)


CREATE INDEX idx_dim_products
    ON dim_products (product_id);

update dim_products
set product_category_name='UNKNOWN'
where has_Unknown_category=1