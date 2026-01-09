import pandas as pd
import urllib
import os
from sqlalchemy import create_engine
from logging_config import get_logger

logger = get_logger("load_fact_orders")

def main():
    logger.info("Starting load_fact_orders pipeline")

    try:
        # -------------------------------
        # Resolve paths
        # -------------------------------
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        DATA_DIR = os.path.join(BASE_DIR, "data", "clean")
        ORDERS_FILE = os.path.join(DATA_DIR, "orders_clean.csv")

        if not os.path.exists(ORDERS_FILE):
            raise FileNotFoundError(f"Orders file not found: {ORDERS_FILE}")

        # -------------------------------
        # SQL Server connection (Windows Auth)
        # -------------------------------
        server = r".\SQLEXPRESS"
        database = "SalesDW"

        params = urllib.parse.quote_plus(
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
        orders = pd.read_csv(
            ORDERS_FILE,
            parse_dates=[
                "order_purchase_timestamp",
                "order_approved_at",
                "order_delivered_carrier_date",
                "order_delivered_customer_date",
                "order_estimated_delivery_date"
            ]
        )

        logger.info("Orders CSV loaded | rows=%d", len(orders))

        # -------------------------------
        # Transform
        # -------------------------------
        orders["order_purchase_date"] = orders["order_purchase_timestamp"].dt.date

        fact_orders_cols = [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_date",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "has_approved_date",
            "has_carrier_date",
            "has_delivery_date",
            "is_canceled",
            "is_unavailable",
            "is_delivered"
        ]

        fact_orders_df = orders[fact_orders_cols]

        if fact_orders_df["order_id"].isnull().any():
            raise ValueError("Null order_id detected")

        logger.info(
            "Prepared fact_orders dataframe | rows=%d | distinct_orders=%d",
            len(fact_orders_df),
            fact_orders_df["order_id"].nunique()
        )

        # -------------------------------
        # Load
        # -------------------------------
        fact_orders_df.to_sql(
            name="fact_orders",
            con=engine,
            schema="dbo",
            if_exists="append",
            index=False,
            chunksize=1000
        )

        logger.info("fact_orders successfully loaded")

    except Exception:
        logger.exception("fact_orders load FAILED")
        raise

    finally:
        logger.info("Ending load_fact_orders pipeline")


if __name__ == "__main__":
    main()
