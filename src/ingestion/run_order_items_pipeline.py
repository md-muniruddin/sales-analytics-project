import pandas as pd
from src.cleaning.clean_order_items import clean_order_items

RAW_PATH='D:/DA project/sales-analytics-project/data/raw/olist_order_items_dataset.csv'
CLEAN_PATH = "D:/DA project/sales-analytics-project/data/clean/order_items_clean.csv"


def main():
    df_raw = pd.read_csv(RAW_PATH, parse_dates=False)

    df_clean = clean_order_items(df_raw)

    df_clean.to_csv(CLEAN_PATH, index=False)
    print(f"Clean order_items written to {CLEAN_PATH}")


if __name__ == "__main__":
    main()
