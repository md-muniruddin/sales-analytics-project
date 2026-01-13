import pandas as pd
import os
from sqlalchemy import create_engine
import urllib
import time

from sqlalchemy.dialects.mssql.information_schema import columns

from pipeline.logging_config import get_logger

logger = get_logger("load_dim_geolocations_pipeline_")

def main():
    logger.info("Starting dim_geolocations loading pipeline")
    start_time = time.time()
    try:
        # -------------------------------
        # Resolve paths
        # -------------------------------
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        DATA_DIR = os.path.join(BASE_DIR, "data", "clean")
        ITEMS_FILE = os.path.join(DATA_DIR, "geolocation_clean.csv")

        if not os.path.exists(ITEMS_FILE):
            raise FileNotFoundError(f"geolocations file not found: {ITEMS_FILE}")

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
        geolocations_col=[
            'geolocation_zip_code_prefix',
            'geolocation_lat',
            'geolocation_lng',
            'geolocation_city',
            'geolocation_state',
            'geolocation_city_en'
        ]

        geolocations=pd.read_csv(
            ITEMS_FILE,usecols=geolocations_col)
        logger.info("geolocations CSV loaded | rows=%d", len(geolocations))
        # -------------------------------
        # Load
        # -------------------------------

        # CPU-heavy work
        start_time = time.time()

        geolocations.to_sql(
            name="dim_geolocations",
            con=engine,
            schema="dbo",
            if_exists="append",
            index=False,
            chunksize=1000
        )

        end_time = time.time()
        duration = end_time - start_time
        rows=pd.read_sql(f"SELECT COUNT(*) cnt FROM dim_geolocations", engine)['cnt'].iloc[0]
        logger.info(f"Geolocation Data loaded  Successfully | rows={rows}  duration={duration:.2f} seconds")

    except Exception:
        logger.exception("geolocations load FAILED")
        raise
    finally:
        logger.exception("Ending geolocations load pipeline")

if __name__ == "__main__":
    main()