import pandas as pd
from src.logging_config import get_logger
from src.cleaning.clean_geolocation import clean_geolocation
logger = get_logger("run_customer_pipeline_")

RAW_PATH = "D:/DA project/sales-analytics-project/data/raw/olist_geolocation_dataset.csv"
CLEAN_PATH = "D:/DA project/sales-analytics-project/data/clean/geolocation_clean.csv"

def main():
    logger.info(f"Loading raw data from {RAW_PATH}")
    df=pd.read_csv(RAW_PATH)
    logger.info(f"Loading geolocation dataset Sucessfull | rows=%d {len(df)}")
    logger.info(f"Cleaning data  Started")
    df_clean=clean_geolocation(df)
    logger.info(f"Cleaning Geolocation Dataset Sucessfull | rows={len(df_clean)}")
    df_clean.to_csv(CLEAN_PATH, index=False)
    logger.info(f"Clean geolocation written to {CLEAN_PATH}")

    print(f"Clean geolocation written to {CLEAN_PATH}")


if __name__ =="__main__":
    main()