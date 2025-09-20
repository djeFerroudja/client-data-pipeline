import pkg_resources

from country_age_analyses.scripts.pipeline import run_pipeline

age_criteria = 18

city_file = pkg_resources.resource_filename(
    "country_age_analyses", "resources/city_zipcode.csv"
)
clients_file = pkg_resources.resource_filename(
    "country_age_analyses", "resources/clients_bdd.csv"
)
parquet_file = pkg_resources.resource_filename(
    "country_age_analyses", "resources/output/city_clients_dept_csv"
)
csv_file = pkg_resources.resource_filename(
    "country_age_analyses", "resources/output/city_clients_dept_parquet"
)


run_pipeline(
    city_file=city_file,
    clients_file=clients_file,
    age_criteria=age_criteria,
    output_csv=csv_file,
    output_parquet=parquet_file,
)
