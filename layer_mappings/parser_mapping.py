from orchestration.parser_conf import ParserConf
from schemas.silver0 import (
    mfr_config_schema,
    mfr_load_start_schema,
    t4c_ration_names_schema,
    t4c_kitchen_feed_names_schema,
    mfr_load_done_schema,
    mfr_load_done_result_schema
)
from transformers.parsers import (
    parse_mfr_config,
    parse_mfr_load_start,
    parse_mfr_load_done,
    parse_mfr_load_done_result,
    parse_t4c_kitchen_feed_names,
    parse_t4c_ration_names
)


def parser_mapping():
    """Settings for creating silver_layer table of certain message type"""
    return {
        "MFR_CONFIG": [
            ParserConf(
                result_table="mfr_config",
                parser=parse_mfr_config,
                schema_str=mfr_config_schema,
                enable_change_data_feed=True,
            )
        ],
        "MFR_LOAD_START": [
            ParserConf(
                result_table="mfr_load_start",
                parser=parse_mfr_load_start,
                schema_str=mfr_load_start_schema,
                match_columns=ParserConf.DEFAULT_MATCH_COLUMNS | {"rationId", "seqNr"},
                enable_change_data_feed=True,
            )
        ],
        "MFR_LOAD_DONE": [
            ParserConf(
                result_table="mfr_load_done",
                parser=parse_mfr_load_done,
                schema_str=mfr_load_done_schema,
                match_columns=ParserConf.DEFAULT_MATCH_COLUMNS | {"rationId", "seqNr", "feedId"},
                enable_change_data_feed=True,
            ),
            ParserConf(
                result_table="mfr_load_done_result",
                parser=parse_mfr_load_done_result,
                schema_str=mfr_load_done_result_schema,
                match_columns=ParserConf.DEFAULT_MATCH_COLUMNS | {"rationId", "seqNr"},
                enable_change_data_feed=True,
            ),
        ],
        "T4C_KITCHEN_FEED_NAMES": [
            ParserConf(
                result_table="t4c_kitchen_feed_names",
                parser=parse_t4c_kitchen_feed_names,
                schema_str=t4c_kitchen_feed_names_schema,
                match_columns=ParserConf.DEFAULT_MATCH_COLUMNS | {"feedId"},
                enable_change_data_feed=True,
            ),
        ],
        "T4C_RATION_NAMES": [
            ParserConf(
                result_table="t4c_ration_names",
                parser=parse_t4c_ration_names,
                schema_str=t4c_ration_names_schema,
                match_columns=ParserConf.DEFAULT_MATCH_COLUMNS | {"rationId"},
            ),
        ],
    }