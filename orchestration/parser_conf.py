import logging
from typing import Callable, Set
from pyspark.sql import DataFrame
from pyspark.sql.types import _parse_datatype_string


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
        self.enable_change_data_feed = enable_change_data_feed
        self.logger = logging.getLogger("CustomLogger")
