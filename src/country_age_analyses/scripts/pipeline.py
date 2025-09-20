import pandas as pd
import logging

from country_age_analyses.scripts.data_processing import (
    filter_by_age,
    merge_city_clients,
    add_department,
)
from country_age_analyses.scripts.io_utils import load_file_from_csv, save_data

logger = logging.getLogger(__name__)


def run_pipeline(
    city_file, clients_file, age_criteria, output_csv=None, output_parquet=None
):
    """
    Full pipeline: load data, filter by age, merge, add department, save output.

    Args:
        city_file (str): Path to city CSV file.
        clients_file (str): Path to clients CSV file.
        age_criteria (int): Minimum age for filtering clients.
        output_csv (str, optional): Path to save CSV output.
        output_parquet (str, optional): Path to save Parquet output.

    Returns:
        pd.DataFrame: Final processed DataFrame.
    """

    # Step 1: Load files
    logger.info("*** Files reading ---------------------------")
    df_city = load_file_from_csv(city_file)
    df_clients = load_file_from_csv(clients_file)

    # Step 2: Filter clients by age
    logger.info("*** Data Processing ---------------------------")
    df_major = filter_by_age(df_clients, age_criteria)

    # Step 3: Merge clients with city
    df_merge = merge_city_clients(df_major, df_city)

    # Step 4: Add department
    df_with_dept = add_department(df_merge)

    # Step 5: Save output
    logger.info("*** Saving output files --------------------------------")
    save_data(df_with_dept, csv_file=output_csv, parquet_file=output_parquet)
    logger.info("Saving output files -------------------------------")

    logger.info("Pipeline completed successfully ✅")
    return df_with_dept
