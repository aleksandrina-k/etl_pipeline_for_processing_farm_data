import logging
from datetime import datetime, timedelta
from os import path
from typing import Callable, Set

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import _parse_datatype_string
import time


class ParserConf:
    """
    Parser configuration
    Attributes:
        result_table (str): Table where result should be stored
        parser ((DataFrame) -> DataFrame): function that transform bronze
            dataframe to silver_layer
        schema_str (str): Table schema
        match_columns (Set[str]): fields that are used on merge stage to identify
            same entries
        partition_columns (List[str]): list of columns for partition resulting table
        zorder_columns (List[str]): list of columns for ZORDER optimisation of
                resulting table
    """

    DEFAULT_MATCH_COLUMNS = {
        "farm_license",
        "device_type",
        "device_number",
        "time",
    }
    DEFAULT_ZORDER_COLUMNS = {"farm_license"}
    DEFAULT_PARTITION_COLUMNS = {"year_month"}

    def __init__(
        self,
        result_table: str,
        parser: Callable[[DataFrame], DataFrame],
        schema_str: str,
        match_columns: Set[str] = None,
        partition_columns: Set[str] = None,
        zorder_columns: Set[str] = None,
        enable_change_data_feed: bool = False,
    ):
        """
        Parser configuration constructor
        Args:
            result_table (str): Table where result should be stored
            parser ((DataFrame) -> DataFrame): function that transform bronze
                dataframe to silver0_layer
            schema_str (str): table schema
            match_columns (Set[str]): fields that are used on merge stage to identify
                same entries
            partition_columns (List[str]): list of columns for partition resulting table
            zorder_columns (List[str]): list of columns for ZORDER optimisation of
                resulting table
            enable_change_data_feed (bool): enable change data feed on the table
        """
        self.result_table = result_table
        self.parser = parser
        self.schema_str = schema_str
        self.schema = _parse_datatype_string(schema_str)
        self.match_columns = match_columns or ParserConf.DEFAULT_MATCH_COLUMNS
        self.partition_columns = (
            partition_columns or ParserConf.DEFAULT_PARTITION_COLUMNS
        )
        self.zorder_columns = zorder_columns or ParserConf.DEFAULT_ZORDER_COLUMNS
        self.enable_change_data_feed = enable_change_data_feed
        self.logger = logging.getLogger("CustomLogger")

    def optimize_result_table(self, spark: SparkSession, result_dir_location: str):
        """
        Run optimization command on result table
        Args:
            spark: Spark session
            result_dir_location: the location where the delta table is stored
        """
        start_time = time.time()
        year_month_3_days_ago = (datetime.now() - timedelta(days=3)).strftime("%Y-%m")
        where_statement = f"year_month >= {year_month_3_days_ago}"

        table_location = path.join(result_dir_location, self.result_table)
        delta_table = DeltaTable.forPath(spark, table_location)
        delta_table.optimize().where(where_statement).executeZOrderBy(
            list(self.zorder_columns)
        )

        end_time = time.time()
        time_diff = round((end_time - start_time) / 60, 2)
        self.logger.info(
            f"Optimized table: {self.result_table} in {time_diff} minutes "
            f"where {where_statement} and zordered by {self.zorder_columns}"
        )
