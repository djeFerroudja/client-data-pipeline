import pandas as pd
import pytest
from country_age_analyses.scripts.pipeline import run_pipeline


@pytest.fixture
def sample_data():
    """
    Provides sample clients and cities DataFrames along with department mapping.
    """
    df_clients = pd.DataFrame(
        {
            "name": ["Latch", "Smith", "Weingard"],
            "age": ["67", "17", "25"],
            "zip": ["17540", "52500", "88650"],
        }
    )

    df_city = pd.DataFrame(
        {"zip": ["17540", "52500", "88650"], "city": ["VERINES", "BELMONT", "ANOULD"]}
    )

    # Mock departments corresponding to zip codes
    mock_departments = {"17540": "17", "52500": "52", "88650": "88"}

    return df_clients, df_city, mock_departments


def test_pipeline_run(sample_data, monkeypatch):
    """
    Test the full run_pipeline function using DataFrames instead of files.
    Ensures full coverage without file I/O or logging.
    """
    df_clients, df_city, mock_departments = sample_data

    # Patch file I/O functions inside the pipeline module
    monkeypatch.setattr(
        "country_age_analyses.scripts.pipeline.load_file_from_csv",
        lambda path: df_city if "city" in path else df_clients,
    )
    monkeypatch.setattr(
        "country_age_analyses.scripts.pipeline.save_data",
        lambda df, csv_file=None, parquet_file=None: None,
    )

    # Run the pipeline
    result = run_pipeline(
        city_file="dummy_city.csv",
        clients_file="dummy_clients.csv",
        age_criteria=18,
        output_csv=None,
        output_parquet=None,
    )

    # Check age filtering
    assert all(result["age"].astype(int) > 18)
    assert set(result["name"]) == {"Latch", "Weingard"}

    # Check city merge
    assert set(result["city"]) <= {"VERINES", "BELMONT", "ANOULD"}

    # Check department column
    expected_dept = [mock_departments[z] for z in result["zip"]]
    assert list(result["department"]) == expected_dept
