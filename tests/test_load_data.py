import os

import pandas as pd
import pytest

from country_age_analyses.scripts.io_utils import load_file_from_csv


class TestLoadFileFromCsv:
    def test_missing_file_raises(self, tmp_path):
        """
        Test that loading a missing file raises FileNotFoundError.
        """
        missing_file = os.path.join(tmp_path, "missing.csv")

        with pytest.raises(FileNotFoundError):
            load_file_from_csv(missing_file)

    def test_valid_csv_format(self):
        """
        Test that a valid CSV from tests/resources is correctly loaded into a DataFrame.
        """
        # Arrange: build path to resources file
        resources_dir = os.path.join(os.path.dirname(__file__), "resources/input")
        file_path = os.path.join(resources_dir, "valid_clients_bdd.csv")

        # Act
        df = load_file_from_csv(file_path)

        # Assert
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["name", "age", "zip"]
        assert len(df) == 2
        assert df.iloc[0]["name"] == "Latch"

    def test_invalid_csv_format(self):
        """
        Test that a valid CSV from tests/resources is correctly loaded into a DataFrame.
        """
        # Arrange: build path to resources file
        resources_dir = os.path.join(os.path.dirname(__file__), "resources/input")
        file_path = os.path.join(resources_dir, "invalid_city_zipcode.csv")

        # Act & Assert
        with pytest.raises(pd.errors.ParserError):
            load_file_from_csv(file_path)

    def test_wrong_separator(self):
        """
        Test that a CSV with the wrong separator does not produce expected columns.
        """
        # Arrange: build path to resources file
        resources_dir = os.path.join(os.path.dirname(__file__), "resources/input")
        file_path = os.path.join(resources_dir, "invalid_separator.csv")
        expected_sep = ";"
        # Act
        df = load_file_from_csv(file_path, expected_sep)

        # Assert
        assert list(df.columns) != ["name", "age", "zip"]


if __name__ == "__main__":
    pytest.main()
