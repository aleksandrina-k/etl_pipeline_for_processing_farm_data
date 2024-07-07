from orchestration.parser_conf import ParserConf
from schemas.silver0 import (
    robot_config_schema,
    load_start_schema,
    ration_names_schema,
    kitchen_feed_names_schema,
    load_done_schema,
    load_done_result_schema,
)
from transformers.parsers import (
    parse_robot_config,
    parse_load_start,
    parse_load_done,
    parse_load_done_result,
    parse_kitchen_feed_names,
    parse_ration_names,
)


def parser_mapping():
    """Settings for creating silver_layer table of certain message type"""
    return {
        "ROBOT_CONFIG": [
            ParserConf(
                result_table="robot_config",
                parser=parse_robot_config,
                schema_str=robot_config_schema,
                enable_change_data_feed=True,
            )
        ],
        "LOAD_START": [
            ParserConf(
                result_table="load_start",
                parser=parse_load_start,
                schema_str=load_start_schema,
                match_columns=ParserConf.DEFAULT_MATCH_COLUMNS
                | {"ration_id", "seq_nr"},
                enable_change_data_feed=True,
            )
        ],
        "LOAD_DONE": [
            ParserConf(
                result_table="load_done",
                parser=parse_load_done,
                schema_str=load_done_schema,
                match_columns=ParserConf.DEFAULT_MATCH_COLUMNS
                | {"ration_id", "seq_nr", "feed_id"},
                enable_change_data_feed=True,
            ),
            ParserConf(
                result_table="load_done_result",
                parser=parse_load_done_result,
                schema_str=load_done_result_schema,
                match_columns=ParserConf.DEFAULT_MATCH_COLUMNS
                | {"ration_id", "seq_nr"},
                enable_change_data_feed=True,
            ),
        ],
        "KITCHEN_FEED_NAMES": [
            ParserConf(
                result_table="kitchen_feed_names",
                parser=parse_kitchen_feed_names,
                schema_str=kitchen_feed_names_schema,
                match_columns=ParserConf.DEFAULT_MATCH_COLUMNS | {"feed_id"},
                enable_change_data_feed=True,
            ),
        ],
        "RATION_NAMES": [
            ParserConf(
                result_table="ration_names",
                parser=parse_ration_names,
                schema_str=ration_names_schema,
                match_columns=ParserConf.DEFAULT_MATCH_COLUMNS | {"ration_id"},
            ),
        ],
    }
