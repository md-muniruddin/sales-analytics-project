import pandas as pd
from src.logging_config import get_logger
from src.cleaning.clean_customers import clean_customers
logger = get_logger("run_customer_pipeline_")

RAW_PATH = "D:/DA project/sales-analytics-project/data/raw/olist_customers_dataset.csv"
CLEAN_PATH = "D:/DA project/sales-analytics-project/data/clean/customers_clean.csv"

def main():
    logger.info(f"Loading raw data from {RAW_PATH}")
    df=pd.read_csv(RAW_PATH )

    logger.info(f"Cleaning data  Started")
    df_clean=clean_customers(df)
    df_clean.to_csv(CLEAN_PATH, index=False)
    logger.info(f"Clean customers written to {CLEAN_PATH}")

    print(f"Clean customers written to {CLEAN_PATH}")


if __name__ =="__main__":
    main()