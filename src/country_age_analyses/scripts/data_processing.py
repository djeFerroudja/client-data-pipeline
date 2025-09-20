import logging

import pandas as pd

from country_age_analyses.scripts.utils import get_department

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def filter_by_age(df: pd.DataFrame, age: int) -> pd.DataFrame:
    """
    Filter clients with age greater than the specified threshold.

    Args:
        df (pd.DataFrame): DataFrame containing an 'age' column.
        age (int): Age threshold.

    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    df_copy = df.copy()
    df_copy["age"] = pd.to_numeric(df_copy["age"], errors="coerce").astype("Int64")
    filtered = df_copy[df_copy["age"] > age]
    logging.info(f"{len(filtered)} adult clients found (age > {age})")
    return filtered


def merge_city_clients(df_clients: pd.DataFrame, df_city: pd.DataFrame) -> pd.DataFrame:
    """
    Merge clients and city DataFrames on the 'zip' column.

    Args:
        df_clients (pd.DataFrame)
        df_city (pd.DataFrame)

    Returns:
        pd.DataFrame: Merged DataFrame.
    """
    merged = pd.merge(df_clients, df_city, on="zip", how="inner")
    logging.info(f"Merged DataFrames: {len(merged)} rows")
    return merged


def add_department(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a 'department' column based on the zip code.

    Args:
        df (pd.DataFrame)

    Returns:
        pd.DataFrame: DataFrame with 'department' column added.
    """
    df_copy = df.copy()
    df_copy["department"] = df_copy["zip"].apply(get_department)
    return df_copy
