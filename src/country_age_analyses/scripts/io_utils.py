import os
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def load_file_from_csv(file_path: str, sep: str = ",") -> pd.DataFrame:
    """
    Load a CSV file into a pandas DataFrame with all columns as strings.

    Args:
        file_path (str): Path to the CSV file to load.
        sep (str): separator of the csv file (with comma as default value).

    Returns:
        pd.DataFrame: DataFrame containing the CSV data, all columns as string type.

    Raises:
        FileNotFoundError: If the specified file does not exist.

    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} does not exist.")

    try:
        df = pd.read_csv(file_path, sep=sep, dtype=str)
    except pd.errors.ParserError as e:
        logger.error(f"Failed to parse CSV file: {file_path} ({e})")
        raise

    logger.info(f"Loaded file: {file_path} ({len(df)} rows)")
    return df


def save_data(df: pd.DataFrame, csv_file: str = None, parquet_file: str = None):
    """
    Save a pandas DataFrame to CSV and/or Parquet files.

    Args:
        df (pd.DataFrame): DataFrame to save.
        csv_file (str, optional): Path to save as CSV. Defaults to None.
        parquet_file (str, optional): Path to save as Parquet. Defaults to None.

    Notes:
        - CSV is saved with UTF-8 encoding, comma separator, no index.
        - Parquet is saved using PyArrow engine, partitioned by 'department' if present.
        - The directories will be created automatically if they do not exist.
    """
    if csv_file:
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
        df.to_csv(csv_file, sep=",", index=False, encoding="utf-8")
        logger.info(f"CSV saved: {csv_file}")
    if parquet_file:
        os.makedirs(os.path.dirname(parquet_file), exist_ok=True)
        df.to_parquet(
            parquet_file, engine="pyarrow", index=False, partition_cols=["department"]
        )
        logger.info(f"Parquet saved: {parquet_file}")
