from orchestration.trans_conf import TransConf
from schemas.silver0 import (
    robot_config_schema,
    load_start_schema,
    ration_names_schema,
    kitchen_feed_names_schema,
    load_done_schema,
)
from transformers.parsers import (
    parse_robot_config,
    parse_load_start,
    parse_load_done,
    parse_kitchen_feed_names,
    parse_ration_names,
)


def parser_mapping():
    """Settings for creating silver_layer table of certain message type"""
    return {
        "ROBOT_CONFIG": [
            TransConf(
                result_table="robot_config",
                transformer=parse_robot_config,
                schema_str=robot_config_schema,
            )
        ],
        "LOAD_START": [
            TransConf(
                result_table="load_start",
                transformer=parse_load_start,
                schema_str=load_start_schema,
                match_columns=TransConf.DEFAULT_MATCH_COLUMNS | {"ration_id", "seq_nr"},
            )
        ],
        "LOAD_DONE": [
            TransConf(
                result_table="load_done",
                transformer=parse_load_done,
                schema_str=load_done_schema,
                match_columns=TransConf.DEFAULT_MATCH_COLUMNS | {"ration_id", "seq_nr"},
            ),
        ],
        "KITCHEN_FEED_NAMES": [
            TransConf(
                result_table="kitchen_feed_names",
                transformer=parse_kitchen_feed_names,
                schema_str=kitchen_feed_names_schema,
                match_columns=TransConf.DEFAULT_MATCH_COLUMNS | {"feed_id"},
            ),
        ],
        "RATION_NAMES": [
            TransConf(
                result_table="ration_names",
                transformer=parse_ration_names,
                schema_str=ration_names_schema,
                match_columns=TransConf.DEFAULT_MATCH_COLUMNS | {"ration_id"},
            ),
        ],
    }
