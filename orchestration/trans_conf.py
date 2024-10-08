import logging
import time
from collections.abc import Callable
from inspect import signature
from pyspark.sql import SparkSession
from pyspark.sql.types import _parse_datatype_string
from typing import List, Dict, Set
from exceptions import SchemaDoesNotMatchExpectedException


def get_parameters_names(transformer: Callable) -> List[str]:
    params = signature(transformer).parameters
    return [str(p) for p in params if str(p)[0] != "_"]


class TransConf:
    """
    Transformation configuration

    Attributes:
        result_table (str): Table where result should be written to
        input_tables (List[str]): list of tables needed to build result table
        transformer ((...) -> DataFrame): function that builds resulting
                table from input tables. Every parameter must be DataFrame and have
                name that correspond to some table
    """

    DEFAULT_MATCH_COLUMNS = {
        "farm_license",
        "device_type",
        "device_number",
        "time",
    }
    DEFAULT_PARTITION_COLUMNS = {"year_month"}

    def __init__(
        self,
        result_table: str,
        transformer: Callable,
        schema_str: str,
        match_columns: Set[str] = None,
        partition_columns: Set[str] = None,
    ):
        """
        Transformation configuration
        Args:
            result_table (str): Table where result should be written to
            transformer (Callable[..., DataFrame]): function that builds resulting
                table from input tables. Every parameter must be DataFrame and have
                name that correspond to some table
            schema_str (str): resulting table schema
            match_columns (Set[str]): fields that are used on merge stage to identify
                same entries
            partition_columns (List[str]): list of columns for partition resulting table
        """
        self.result_table = result_table
        self.transformer = transformer
        self.input_tables = get_parameters_names(transformer)
        self.schema_str = schema_str
        self.schema = _parse_datatype_string(schema_str)
        self.match_columns = match_columns or TransConf.DEFAULT_MATCH_COLUMNS
        self.partition_columns = (
            partition_columns or TransConf.DEFAULT_PARTITION_COLUMNS
        )
        self.logger = logging.getLogger("CustomLogger")

    def _load_data(self, spark: SparkSession, table_dict: Dict[str, str]) -> dict:
        """
        Prepare dictionary with dataframe from input tables.
        Args:
            spark: Spark session
            table_dict: dictionary with all input tables in format:
                table_name: location (where the table is stored)
        Returns:
            Dictionary where key is a table name and value is a dataframe
        """
        res = {}
        for table_name, location in table_dict.items():
            self.logger.info(f"Reading table {table_name}")
            res[table_name] = spark.read.load(f"{location}\{table_name}")  # noqa W605
        return res

    def perform_transformation(
        self,
        spark: SparkSession,
        input_dir_name: str,
        result_dir_location: str,
    ):
        """
        Produces result tables according to transformation configuration.
        Queries input tables, puts them into transformation,
        writes down results to result table.
        Args:
            spark (SparkSession): Spark session
            input_dir_name (str): Location with all input tables
            result_dir_location (str): Location where result tables will be stored
        """
        start_time = time.time()
        self.logger.info(f"Start processing table {self.result_table}")

        default_dir_dict = {
            table_name: input_dir_name for table_name in self.input_tables
        }

        input_dfs = self._load_data(spark, default_dir_dict)
        result_df = self.transformer(**input_dfs)

        if result_df.schema != self.schema:
            raise SchemaDoesNotMatchExpectedException(
                result_table=self.result_table,
                expected_schema=self.schema,
                actual_schema=result_df.schema,
            )

        (
            result_df.write.format("delta")
            .mode("overwrite")
            .partitionBy(list(self.partition_columns))
            .save(f"{result_dir_location}/{self.result_table}")
        )
        result_df.sort("farm_license").limit(10).show(truncate=False)

        end_time = time.time()
        time_diff = round((end_time - start_time) / 60, 2)
        self.logger.info(
            f"Table {self.result_table} is processed." f"It took: {time_diff} minutes"
        )
