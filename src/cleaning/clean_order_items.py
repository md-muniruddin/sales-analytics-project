import pandas as pd
def clean_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean order_items at item-level grain.
    This function is NON-DESTRUCTIVE:
    - No row deletion
    - No aggregation
    - No revenue logic
    """

    df = df.copy()

    # ----------------------------
    # Enforce identifier types
    # ----------------------------
    df["order_id"] = df["order_id"].astype("string")
    df["product_id"] = df["product_id"].astype("string")

    # ----------------------------
    # Convert shipping date safely
    # ----------------------------
    df["shipping_limit_date"] = pd.to_datetime(
        df["shipping_limit_date"],
        errors="coerce"
    )

    # ----------------------------
    # Enforce numeric safety
    # ----------------------------
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["freight_value"] = pd.to_numeric(df["freight_value"], errors="coerce")

    # ----------------------------
    # Financial sanity flags
    # ----------------------------
    df["is_zero_price"] = df["price"] == 0
    df["is_negative_price"] = df["price"] < 0
    df["is_high_freight"] = df["freight_value"] > df["price"]

    return df
