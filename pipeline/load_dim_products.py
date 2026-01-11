import pandas as pd
import os
from sqlalchemy import create_engine
import urllib
import time
from pipeline.logging_config import get_logger

logger = get_logger("load_dim_products_pipeline_")

def main():
    logger.info("Starting dim_products loading pipeline")
    start_time = time.time()
    try:
        # -------------------------------
        # Resolve paths
        # -------------------------------
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        DATA_DIR = os.path.join(BASE_DIR, "data", "clean")
        ITEMS_FILE = os.path.join(DATA_DIR, "products_clean.csv")

        if not os.path.exists(ITEMS_FILE):
            raise FileNotFoundError(f"Order items file not found: {ITEMS_FILE}")

        # -------------------------------
        # SQL Server connection
        # -------------------------------

        server=r".\SQLEXPRESS"
        database="sales-analytics"

        params=urllib.parse.quote_plus(
            f"DRIVER=ODBC Driver 17 for SQL Server;"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
        logger.info("SQL Server connection created")

        # -------------------------------
        # Read data
        # -------------------------------
        products_col=[
            'product_id',
            'product_category_name',
            'product_name_length',
            'product_description_length',
            'product_photos_qty',
            'product_weight_g',
            'product_length_cm',
            'product_height_cm',
            'product_width_cm',
            'has_Unknown_category'
        ]

        products=pd.read_csv(
            ITEMS_FILE)
        logger.info("products CSV loaded | rows=%d", len(products))

        if products["product_id"].isnull().any():
            raise ValueError("Null product_id detected in products")

        logger.info(
            "Prepared products dataframe | rows=%d | distinct_products=%d",
            len(products),
            products["product_id"].nunique()
        )

        # -------------------------------
        # Load
        # -------------------------------

        # CPU-heavy work
        start_time = time.time()

        products.to_sql(
            name="dim_products",
            con=engine,
            schema="dbo",
            if_exists="append",
            index=False,
            chunksize=1000
        )

        end_time = time.time()
        duration = end_time - start_time
        logger.info("products successfully loaded")
        logger.info(f"Data loading took {duration:.2f} seconds")

    except Exception:
        logger.exception("products load FAILED")
        raise
    finally:
        logger.exception("Ending products load pipeline")

if __name__ == "__main__":
    main()