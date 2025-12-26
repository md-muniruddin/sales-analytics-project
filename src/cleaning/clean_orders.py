import pandas as pd
from typing import List


TIMESTAMP_COLUMNS: List[str] = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


def convert_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert known timestamp columns to datetime safely.
    Invalid values are coerced to NaT (never dropped).
    """
    df = df.copy()

    for col in TIMESTAMP_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def add_order_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add non-destructive flags for downstream analytics.
    No business logic or revenue assumptions here.
    """
    df = df.copy()

    df["has_approved_date"] = df["order_approved_at"].notna()
    df["has_carrier_date"] = df["order_delivered_carrier_date"].notna()
    df["has_delivery_date"] = df["order_delivered_customer_date"].notna()

    df["is_canceled"] = df["order_status"].isin(["canceled", "unavailable"])

    return df


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Master cleaning function for orders dataset.
    This produces a SQL-ready, analytics-safe table.
    """
    df = convert_timestamps(df)
    df = add_order_flags(df)

    return df
