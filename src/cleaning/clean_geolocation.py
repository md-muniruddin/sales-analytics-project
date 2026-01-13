import pandas as pd
from src.logging_config import get_logger
import time
from deep_translator import GoogleTranslator

logger = get_logger("clean_geolocation")


def translate_geolocation_city(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    try:
        start_time = time.time()
        """  we will be only translating the most used city name across
            rather than translating all names.

            we will fetch the city name which has count more than 500 """

        logger.info("Started fetching Frequency of The city Name in Dataset")
        df_city_count = (
            df.groupby('geolocation_city')
            .agg(
                total=('geolocation_city', 'count'),
            )
            .sort_values(by='total', ascending=False)
            .reset_index()
        )
        df_name = df_city_count[df_city_count['total'] >= 500]
        most_names = df_name['geolocation_city']

        logger.info("frequency of city names >500 | rows=%d", len(most_names))

        logger.info("Translatation of city name from Portuguese -> English Started")
        eng_name = {}
        for name in most_names:
            eng = GoogleTranslator(source='auto', target='en').translate(name)
            eng_name[name] = eng

        df['geolocation_city_en'] = df['geolocation_city'].map(eng_name).fillna(df['geolocation_city'])
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Translatation of city name from Portuguese -> English Completed in {duration:.2f} Seconds")

        logger.info("Cleaning Accent in geolocation city name")
        df['geolocation_city_en'] = df['geolocation_city_en'].str.replace('ã', 'a').str.replace('ç', 'c')

    except Exception:
        logger.exception("Translatation of city name failed")
        raise
    finally:
        logger.info("Translatation of City Name Ended")

    return df


def clean_geolocation(df: pd.DataFrame) -> pd.DataFrame:
    df = translate_geolocation_city(df)

    return df


