import pandas as pd
from typing import List
from src.logging_config import get_logger
logger = get_logger("clean_products")

def drop_nulls(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logger.info("Dropping null values")
        df=df.copy()
        rows=int(df.shape[0])
        """ dropping the only 4 column's Null Value
         as it is < 1% nearly of negligible count """
        cols = ["product_weight_g", "product_length_cm", "product_width_cm", "product_height_cm"]
        df=df.dropna(axis=0, how='any', subset=cols)
        dropped=rows - int(df.shape[0])
        logger.info("Dropped : | rows=%d", dropped)
    except Exception:
        logger.error("Error while dropping null values")
        raise
    finally:
        logger.info("Ending drop_nulls pipeline")

    return df


def clean_product_category(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logger.info("loading Product_category_name_transalation dataset")

        new_path="D:/DA project/sales-analytics-project/data/raw/product_category_name_translation.csv"
        df_translation=pd.read_csv(new_path,dtype={'product_category_name':'string'}, parse_dates=False)

        logger.info("Cleaning product category Name")
        df=df.copy()
        """ we are adding the product_category_name column in english to main df"""
        df = df.merge(
            df_translation,
            on='product_category_name',
            how='left',
            suffixes=('', '_translation')
        )

        """ since the null value for the product_gategory_name > 1% we are going to flag it 
                & keep the flag for later analytics"""

        logger.info("Creating Null category Flag")
        df['has_Unknown_category'] = (df['product_category_name'].isnull())


        """ Replace the underscore in the category name with space
        and changing the dtype from string to category"""

        df['product_category_name']=df['product_category_name'].str.lower().str.replace("_"," ")
        df['product_category_name_english'] = df['product_category_name_english'].str.lower().str.replace("_", " ")
        df['product_category_name'] = df['product_category_name'].astype('category')
        df['product_category_name_english'] = df['product_category_name_english'].astype('category')
        logger.info("Cleaned Product Category Names")
    except Exception:
        logger.error("Error while cleaning product category")
        raise
    finally:
        logger.info("Ending clean_product_category pipeline")

    return df




def correct_columns_spelling(df:pd.DataFrame) -> pd.DataFrame:
    try:
        logger.info("Correcting columns spelling")
        df=df.copy()
        """ renaming these columns 'product_name_lenght' &  'product_name_lenght'
         ---> 'product_name_length' & 'product_name_length' """
        logger.info("Renaming columns with wrong 'length' spelling as 'product_name_lenght' and 'product_name_length' --->  'product_name_length' & 'product_description_length'")
        df = df.rename(columns={'product_name_lenght': 'product_name_length','product_description_lenght': 'product_description_length'}, errors="raise")
        logger.info("Corrected columns spelling")
    except Exception:
        logger.error("Error while correct columns spelling")
        raise
    finally:
        logger.info("Ending correct_columns_spelling pipeline")

    return df

def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """
       Master cleaning function for products dataset.
       This produces a SQL-ready, analytics-safe table.
       """
    df=drop_nulls(df)
    df=clean_product_category(df)
    df=correct_columns_spelling(df)
    return df
