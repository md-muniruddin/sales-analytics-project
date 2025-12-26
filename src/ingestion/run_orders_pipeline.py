import pandas as pd
from src.cleaning.clean_orders import clean_orders

RAW_PATH = "D:/DA project/sales-analytics-project/data/raw/olist_orders_dataset.csv"
CLEAN_PATH = "D:/DA project/sales-analytics-project/data/clean/orders_clean.csv"


def main():
    df = pd.read_csv(
        RAW_PATH,
        dtype={"order_id": "string", "customer_id": "string"},
        parse_dates=False
    )

    df_clean = clean_orders(df)

    df_clean.to_csv(CLEAN_PATH, index=False)
    print(f"Clean orders written to {CLEAN_PATH}")


if __name__ == "__main__":
    main()
