import os
import shutil
import pandas as pd
import pytest

from country_age_analyses.scripts.io_utils import save_data

OUTPUT_DIR = "resources/output"


class TestSaveData:
    """
    Test suite for the `save_data` function.
    Verifies saving DataFrame as CSV and Parquet (partitioned by 'department').
    """

    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)

    @pytest.fixture
    def sample_df(self):
        """
        Provides a sample DataFrame for testing.
        """
        return pd.DataFrame(
            {
                "name": ["Latch"],
                "age": [67],
                "zip": ["17540"],
                "city": ["VERINES"],
                "department": ["17"],
            }
        )

    def test_save_csv(self, sample_df):
        """
        Test saving a DataFrame as CSV.
        Verifies that the CSV file is created and its content matches the original DataFrame.
        """
        csv_path = os.path.join(OUTPUT_DIR, "output.csv")
        save_data(sample_df, csv_file=csv_path)

        # Check that the CSV file exists
        assert os.path.exists(csv_path), f"CSV file {csv_path} not found"

        # Read and verify content
        df_loaded = pd.read_csv(csv_path, dtype=sample_df.dtypes.to_dict())
        pd.testing.assert_frame_equal(df_loaded, sample_df)

    def test_save_parquet(self, sample_df):
        """
        Test saving a DataFrame as Parquet with partitioning by 'department'.
        Verifies that the partition directory is created and contains at least one Parquet file.
        """

        parquet_path = os.path.join(OUTPUT_DIR, "output.parquet")
        save_data(sample_df, parquet_file=parquet_path)

        # Check that the partition directory 'department=17' exists
        partition_dir = os.path.join(parquet_path, "department=17")
        assert os.path.exists(
            partition_dir
        ), f"Partition directory {partition_dir} not found"

        # Check that there is at least one Parquet file in this partition
        parquet_files = [f for f in os.listdir(partition_dir) if f.endswith(".parquet")]
        assert parquet_files, f"No Parquet file found in {partition_dir}"
