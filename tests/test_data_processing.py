import pandas as pd
import pytest
from country_age_analyses.scripts import utils
from country_age_analyses.scripts.data_processing import (
    add_department,
    merge_city_clients,
    filter_by_age,
)


class TestDataProcessing:
    """
    Professional unit tests for data processing functions:
    - filter_by_age
    - merge_city_clients
    - add_department
    """

    @pytest.fixture(autouse=True)
    def setup_data(self, monkeypatch):
        """
        Fixture to prepare test DataFrames and patch get_department.
        """
        # Mock mapping for real departments
        self.mock_departments = {"17540": "17", "69001": "69", "13001": "13"}

        # Patch get_department to return correct department code
        monkeypatch.setattr(
            utils, "get_department", lambda zip_code: self.mock_departments[zip_code]
        )

        # Clients DataFrame
        self.df_clients = pd.DataFrame(
            {
                "name": ["Latch", "Bob", "Alice"],
                "age": ["67", "17", "25"],
                "zip": ["17540", "69001", "13001"],
            }
        )

        # Cities DataFrame
        self.df_city = pd.DataFrame(
            {
                "zip": ["17540", "69001", "13001"],
                "city": ["VERINES", "LYON", "MARSEILLE"],
            }
        )

    def test_filter_by_age(self):
        """
        Verify that filter_by_age returns only clients with age > threshold.
        """
        filtered = filter_by_age(self.df_clients, 18)
        assert len(filtered) == 2
        assert all(filtered["age"] > 18)
        assert set(filtered["name"]) == {"Latch", "Alice"}

    def test_merge_city_clients(self):
        """
        Verify that merge_city_clients correctly merges the DataFrames on 'zip'.
        """
        merged = merge_city_clients(self.df_clients, self.df_city)
        assert len(merged) == 3
        for _, row in merged.iterrows():
            assert row["city"] in ["VERINES", "LYON", "MARSEILLE"]

    def test_add_department(self):
        """
        Verify that add_department correctly adds the 'department' column using get_department.
        """
        df_with_dept = add_department(self.df_clients)
        assert "department" in df_with_dept.columns
        expected = [self.mock_departments[z] for z in self.df_clients["zip"]]
        assert list(df_with_dept["department"]) == expected
