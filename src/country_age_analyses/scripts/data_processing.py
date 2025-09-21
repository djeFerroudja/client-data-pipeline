from typing import Union, Optional, Literal

import pandas as pd
import polars as pl
import pyspark as ps
import pyspark.sql.functions as func
from pyspark.sql.types import StringType

from country_age_analyses.utils.custom_logger import CustomLogger
from country_age_analyses.utils.file_utils import FileUtils

HowJoinType = Literal["left", "right", "inner", "outer", "cross"]
DfType = Union[pd.DataFrame, ps.sql.DataFrame, pl.DataFrame]


class DataProcessor(FileUtils):
    """
    DataProcessor extends FileUtils to provide additional data processing
    functionalities such as filtering by age, merging DataFrames, and
    adding department codes based on zipcodes.

    Attributes inherited from FileUtils:
        df: Loaded DataFrame.
        engine: Engine used ('pandas', 'polars', 'pyspark').
    """

    def __init__(
        self,
        file_path: str,
        sep: str = ",",
        header: Optional[bool] = True,
        engine: Literal["pyspark", "pandas", "polars"] = "pandas",
    ):
        """
        Initialize DataProcessor instance.

        Args:
            file_path (str): Path to the input CSV file.
            sep (str, optional): CSV separator. Defaults to ",".
            header (Optional[bool], optional): Whether CSV has header. Defaults to True.
            engine (Literal['pyspark', 'pandas', 'polars'], optional): Engine to use. Defaults to 'pandas'.
        """
        FileUtils.__init__(
            self, file_path=file_path, sep=sep, header=header, engine=engine
        )

    def filter_by_age(self, age: int) -> DfType:
        """
        Filter the DataFrame to include only rows where 'age' > given age.

        Args:
            age (int): Minimum age to filter by.

        Returns:
            DfType: Filtered DataFrame.

        Notes:
            - Converts 'age' column to integer type before filtering.
            - Updates self.df with the filtered DataFrame.
        """
        filtered: Optional[DfType] = None
        df_copy = self.df

        if self.engine == "pandas":
            df_copy["age"] = pd.to_numeric(df_copy["age"], errors="coerce").astype("Int64")
            filtered = df_copy[df_copy["age"] > age]
            CustomLogger.info(f"{len(filtered)} adult clients found (age > {age})")

        elif self.engine == "polars":
            filtered = df_copy.with_columns(pl.col("age").cast(pl.Int64))
            filtered = filtered.filter(pl.col("age") > age)
            CustomLogger.info(f"{len(filtered)} adult clients found (age > {age})")

        elif self.engine == "pyspark":
            filtered = df_copy.withColumn("age", func.col("age").cast("int"))
            filtered = filtered.filter(func.col("age") > age)
            CustomLogger.info(f"{filtered.count()} adult clients found (age > {age})")

        self.df = filtered
        return filtered

    def merge_df(
        self, df_to_merge, on_column: str, join_type: HowJoinType = "inner"
    ) -> DfType:
        """
        Merge the current DataFrame with another DataFrame.

        Args:
            df_to_merge (DfType): DataFrame to merge with.
            on_column (str): Column name to join on.
            join_type (HowJoinType, optional): Type of join. Defaults to 'inner'.

        Returns:
            DfType: Merged DataFrame.

        Raises:
            NotImplementedError: If the engine is not supported.
        """
        try:
            if self.engine == "pandas":
                merged = pd.merge(self.df, df_to_merge, on=on_column, how=join_type)
                df_len = len(merged)
            elif self.engine == "polars":
                merged = self.df.join(df_to_merge, on=on_column, how=join_type)
                df_len = len(merged)
            elif self.engine == "pyspark":
                merged = self.df.join(df_to_merge, on=on_column, how=join_type)
                df_len = merged.count()
            else:
                CustomLogger.error(f"Engine {self.engine} not implemented.")
                raise NotImplementedError(f"Engine {self.engine} not implemented.")
        except Exception as exp:
            CustomLogger.error(f"Failed to merge DataFrames: {exp}")
            raise exp

        CustomLogger.info(f"Merged DataFrames: {df_len} rows")
        self.df = merged
        return merged

    @staticmethod
    def get_department(zipcode: str) -> str:
        """
        Return the French department code based on a given zipcode.

        Special handling for Corsica:
            - Zipcodes 20000 to 20199 → '2A' (Corse-du-Sud)
            - Zipcodes 20200 to 20999 → '2B' (Haute-Corse)

        For all other zipcodes, the department code is derived from the first two digits.

        Args:
            zipcode (str or int): The zipcode to convert.

        Returns:
            str: The corresponding department code.

        Raises:
            ValueError: If the zipcode is not a valid 5-digit number.
        """
        if zipcode is None or zipcode == "":
            raise ValueError(f"Invalid zipcode: {zipcode}")

        z = str(zipcode).zfill(5)

        if not z.isdigit():
            raise ValueError(f"Invalid zipcode format: {zipcode}")

        if z.startswith("20"):
            dept = "2A" if int(z[2:]) <= 199 else "2B"
        else:
            dept = z[:2]
        return dept

    def add_department(self) -> DfType:
        """
        Add a 'department' column to the DataFrame based on the 'zip' column.

        Returns:
            DfType: Updated DataFrame with 'department' column added.

        Raises:
            NotImplementedError: If the engine is not supported.
        """
        df = self.df

        if self.engine == "pandas":
            df_copy = df.copy()
            df_copy["department"] = df_copy["zip"].apply(self.get_department)

        elif self.engine == "polars":
            df_copy = df.with_columns(
                [
                    pl.col("zip")
                    .map_elements(self.get_department, return_dtype=pl.Utf8)
                    .alias("department")
                ]
            )

        elif self.engine == "pyspark":
            get_dept_udf = func.udf(self.get_department, StringType())
            df_copy = df.withColumn("department", get_dept_udf(func.col("zip")))

        else:
            raise NotImplementedError(f"Engine {self.engine} not implemented.")

        self.df = df_copy
        return df_copy
