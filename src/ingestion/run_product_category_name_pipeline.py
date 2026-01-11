import pandas as pd
from src.cleaning.clean_orders import clean_orders
from src.logging_config import get_logger
logger = get_logger("load_fact_order_items")
RAW_PATH = "D:/DA project/sales-analytics-project/data/raw/product_category_name_translation.csv"
CLEAN_PATH = "D:/DA project/sales-analytics-project/data/clean/product_category_name_clean.csv"

