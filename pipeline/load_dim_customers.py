import pandas as pd
import os
from sqlalchemy import create_engine
import urllib
import time

from sqlalchemy.dialects.mssql.information_schema import columns

from pipeline.logging_config import get_logger

logger = get_logger("load_dim_customers_pipeline_")

def main():
    logger.info("Starting dim_customers loading pipeline")
    start_time = time.time()
    try:
        # -------------------------------
        # Resolve paths
        # -------------------------------
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        DATA_DIR = os.path.join(BASE_DIR, "data", "clean")
        ITEMS_FILE = os.path.join(DATA_DIR, "customers_clean.csv")

        if not os.path.exists(ITEMS_FILE):
            raise FileNotFoundError(f"Customers file not found: {ITEMS_FILE}")

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
        customers_col=[
            'customer_id',
            'customer_unique_id',
            'customer_zip_code_prefix',
            'customer_city',
            'customer_state',
            'customer_city_en'
        ]

        customers=pd.read_csv(
            ITEMS_FILE,usecols=customers_col)
        logger.info("Customers CSV loaded | rows=%d", len(customers))

        if customers["customer_id"].isnull().any():
            raise ValueError("Null customer_id detected in customers")

        logger.info(
            "Prepared Customers dataframe | rows=%d | distinct_customers=%d",
            len(customers),
            customers["customer_id"].nunique()
        )

        # -------------------------------
        # Load
        # -------------------------------

        # CPU-heavy work
        start_time = time.time()

        customers.to_sql(
            name="dim_customers",
            con=engine,
            schema="dbo",
            if_exists="append",
            index=False,
            chunksize=1000
        )

        end_time = time.time()
        duration = end_time - start_time
        rows = pd.read_sql(f"SELECT COUNT(*) cnt FROM dim_customers", engine)['cnt'].iloc[0]
        logger.info(f"customers Data loaded  Successfully | rows={rows}  duration={duration:.2f} seconds")

    except Exception:
        logger.exception("customers load FAILED")
        raise
    finally:
        logger.exception("Ending customers load pipeline")

if __name__ == "__main__":
    main()