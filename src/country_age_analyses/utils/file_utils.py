import os
import shutil
from typing import Literal, Union, Optional, List

import pandas as pd
import pkg_resources
import polars
import pyspark
from pyspark.sql import SparkSession

from country_age_analyses.utils.custom_logger import CustomLogger


class FileUtils:
    """
    Utility class to handle file operations (CSV loading and Parquet saving)
    using different engines: pandas, polars, or PySpark.

    Attributes:
        spark (Optional[pyspark.sql.SparkSession]): Spark session instance if engine is PySpark.
        df (Optional[Union[pd.DataFrame, pyspark.sql.DataFrame, polars.DataFrame]]):
            The loaded DataFrame.
        file_path (str): Path to the file to load.
        sep (str): Delimiter used for CSV files.
        header (Optional[bool]): Whether the CSV file has a header row.
        engine (Literal["pyspark", "pandas", "polars"]): Engine to use for data operations.
        file_name (str): Base name of the file without extension.
    """

    spark: Optional[pyspark.sql.SparkSession] = None
    df: Optional[Union[pd.DataFrame, pyspark.sql.DataFrame, polars.DataFrame]] = None

    def __init__(
        self,
        file_path: str,
        sep: str = ",",
        header: Optional[bool] = True,
        engine: Literal["pyspark", "pandas", "polars"] = "pandas",
    ) -> None:
        """
        Initialize FileUtils instance.

        Args:
            file_path (str): Path to the file to load.
            sep (str, optional): CSV separator. Defaults to ",".
            header (Optional[bool], optional): Whether CSV has header. Defaults to True.
            engine (Literal["pyspark", "pandas", "polars"], optional):
                Engine to use for loading and saving files. Defaults to "pandas".

        Raises:
            FileNotFoundError: If the file does not exist.
            SystemExit: If the specified engine is not implemented.
        """
        self.file_path = file_path
        self.sep = sep
        self.header = header
        self.engine = engine
        self.file_name = os.path.basename(file_path).split(".")[0]

        if self.engine not in ["pyspark", "pandas", "polars"]:
            CustomLogger.error(f"Engine [{self.engine}] not implemented.")
            exit(-1)

        if self.engine == "pyspark":
            self.spark = SparkSession.builder.appName("test").getOrCreate()

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} does not exist.")

    def load_file_from_csv(
        self,
        file_path: Optional[str] = None,
        sep: str = ",",
        header: Optional[bool] = True,
    ) -> Union[pd.DataFrame, pyspark.sql.DataFrame, polars.DataFrame]:
        """
        Load a CSV file into a DataFrame using the specified engine.

        Args:
            file_path (Optional[str], optional): Path to CSV file. Defaults to self.file_path.
            sep (str, optional): CSV separator. Defaults to ",".
            header (Optional[bool], optional): Whether CSV has header. Defaults to True.

        Returns:
            Union[pd.DataFrame, pyspark.sql.DataFrame, polars.DataFrame]:
                Loaded DataFrame with all columns as strings.

        Raises:
            FileNotFoundError: If the file does not exist.
            pd.errors.ParserError: If the CSV file cannot be parsed.
            NotImplementedError: If the engine is not implemented.
        """
        file_path = file_path if file_path else self.file_path
        sep = sep if sep else self.sep
        header = header if header else self.header

        try:
            if self.engine == "pandas":
                header_option = 0 if header else None
                df = pd.read_csv(file_path, sep=sep, dtype=str, header=header_option)
                df_len = len(df)

            elif self.engine == "polars":
                df = polars.read_csv(file_path, separator=sep, infer_schema=False, has_header=header)
                df_len = len(df)

            elif self.engine == "pyspark":
                df = self.spark.read.options(header=True, sep=sep, inferSchema=False).csv(file_path)
                df_len = df.count()

            else:
                CustomLogger.error(f"Engine {self.engine} not implemented.")
                raise NotImplementedError(f"Engine {self.engine} not implemented.")

        except Exception as exp:
            if isinstance(exp, pd.errors.ParserError):
                CustomLogger.error(f"Failed to parse CSV file: {os.path.basename(file_path)} ({exp})")
            raise exp

        CustomLogger.info(f"Loaded file: {self.file_name} ({df_len} rows)")
        self.df = df
        return self.df

    def save_to_parquet(
        self,
        output_dir: Optional[str] = None,
        file_name: Optional[str] = None,
        partitioning_by: Optional[List[str]] = None,
        df: Optional[Union[pd.DataFrame, pyspark.sql.DataFrame, polars.DataFrame]] = None,
    ):
        """
        Save a DataFrame to a Parquet file.

        Args:
            output_dir (Optional[str], optional): Directory to save the Parquet file.
                Defaults to package default output directory.
            file_name (Optional[str], optional): Name of the Parquet file. Defaults to self.file_name.
            partitioning_by (Optional[List[str]], optional): Columns to partition by. Defaults to None.
            df (Optional[Union[pd.DataFrame, pyspark.sql.DataFrame, polars.DataFrame]], optional):
                DataFrame to save. Defaults to self.df.

        Raises:
            SystemExit: If saving fails or engine is not implemented.
        """
        output_path = (
            output_dir
            if output_dir
            else pkg_resources.resource_filename("country_age_analyses", "resources/output")
        )

        file_name = file_name if file_name else self.file_name
        output_path = os.path.join(output_path, f"from_{self.engine}", f"{file_name}.PARQUET")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Remove existing file or folder
        if os.path.exists(output_path):
            if os.path.isdir(output_path):
                shutil.rmtree(output_path)  # remove directory (case pyspark or partitioned parquet)
            else:
                os.remove(output_path)  # remove single file
            CustomLogger.warning(f"Existing parquet output removed: {output_path}")

        self.df = df if df else self.df

        try:
            if self.engine == "pandas":
                self.df.to_parquet(output_path, partition_cols=partitioning_by, engine="pyarrow")

            elif self.engine == "pyspark":
                # Uncomment when Spark saving is needed
                # self.df.write.mode("overwrite").parquet(output_path, partitionBy=partitioning_by)
                pass

            elif self.engine == "polars":
                self.df.write_parquet(output_path, partition_by=partitioning_by)

            else:
                CustomLogger.error(f"Engine {self.engine} not implemented.")
                exit(-1)

            CustomLogger.success(f"Parquet saved successfully with engine [{self.engine}]: {output_path}")

        except Exception as exp:
            CustomLogger.error(f"Failed to save parquet file: {os.path.basename(output_path)} ({exp})")
            exit(-1)
