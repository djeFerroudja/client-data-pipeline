"""
Main entry point for the country_age_analyses pipeline.

Usage:
    python main.py --engine pandas
"""

from typing import Literal
import pkg_resources
import argparse

from country_age_analyses.scripts.data_processing import DataProcessor
from country_age_analyses.utils.custom_logger import CustomLogger

def main(engine: Literal["pyspark", "pandas", "polars"] = "pandas"):
    """Execute the full data processing pipeline."""
    # ------------------------------
    # Configuration
    # ------------------------------
    city_file = pkg_resources.resource_filename(
        "country_age_analyses", "resources/input/city_zipcode.csv"
    )
    clients_file = pkg_resources.resource_filename(
        "country_age_analyses", "resources/input/clients_bdd.csv"
    )

    # ------------------------------
    # Initialize DataProcessor objects
    # ------------------------------
    file_city = DataProcessor(file_path=city_file, engine=engine)
    file_clients = DataProcessor(file_path=clients_file, engine=engine)

    # ------------------------------
    # Step 1: Load CSV files
    # ------------------------------
    CustomLogger.info("*** Files reading ---------------------------")
    file_city.load_file_from_csv()
    file_clients.load_file_from_csv()

    # ------------------------------
    # Step 2: Filter clients by age
    # ------------------------------
    CustomLogger.info("*** Data Processing ---------------------------")
    df_major = file_clients.filter_by_age(age=18)

    # ------------------------------
    # Step 3: Merge clients with city data
    # ------------------------------
    df_merge = file_city.merge_df(df_to_merge=df_major, on_column="zip")

    # ------------------------------
    # Step 4: Add department column
    # ------------------------------
    df_with_dept = file_city.add_department()

    # ------------------------------
    # Step 5: Save output to Parquet
    # ------------------------------
    CustomLogger.info("*** Saving output files --------------------------------")
    file_city.save_to_parquet(
        file_name="country_age_analyses", partitioning_by=["department"]
    )
    CustomLogger.info("Saving output files -------------------------------")

    # ------------------------------
    # Pipeline completion message
    # ------------------------------
    CustomLogger.info("Pipeline completed successfully ✅")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run country_age_analyses pipeline")
    parser.add_argument(
        "--engine",
        choices=["pandas", "pyspark", "polars"],
        default="pandas",
        help="Engine to use for processing (default: pandas)"
    )
    args = parser.parse_args()
    main(engine=args.engine)
