import pandas as pd
from src.logging_config import get_logger
from src.cleaning.clean_products import clean_products
logger = get_logger("run_products_pipeline_")

RAW_PATH = "D:/DA project/sales-analytics-project/data/raw/olist_products_dataset.csv"
CLEAN_PATH = "D:/DA project/sales-analytics-project/data/clean/products_clean.csv"

def main():
    logger.info(f"Loading raw data from {RAW_PATH}")
    df=pd.read_csv(RAW_PATH,dtype={'product_category_name':'string'}, parse_dates=False)

    logger.info(f"Cleaning data  Started")
    df_clean=clean_products(df)
    df_clean.to_csv(CLEAN_PATH, index=False)
    logger.info(f"Clean orders written to {CLEAN_PATH}")

    print(f"Clean orders written to {CLEAN_PATH}")


if __name__ =="__main__":
    main()