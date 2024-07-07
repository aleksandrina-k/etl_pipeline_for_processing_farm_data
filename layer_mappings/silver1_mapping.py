from layer_mappings.parser_mapping import parser_mapping
from orchestration.trans_conf import TransConf
from orchestration.trans_mapping import TransMapping
from schemas.silver1 import (
    silver_loading_activity_schema,
    silver_robot_config_dim_schema,
    silver_kitchen_feed_names_dim_schema,
    silver_ration_names_dim_schema,
)
from transformers.silver1 import (
    silver_loading_activity_transformer,
    silver_kitchen_feed_names_dim_transformer,
    silver_robot_config_dim_transformer,
    silver_ration_names_dim_transformer,
)

DEFAULT_ZORDER_COLUMNS_SILVER1 = {"farm_license"}


def silver1_mapping():
    """Settings for creating silver1_layer table"""
    existing_tables = {
        conf.result_table for confs in parser_mapping().values() for conf in confs
    }
    return TransMapping(
        confs=[
            TransConf(
                result_table="silver_loading_activity",
                transformer=silver_loading_activity_transformer,
                schema_str=silver_loading_activity_schema,
                zorder_columns=DEFAULT_ZORDER_COLUMNS_SILVER1,
            ),
            TransConf(
                result_table="silver_robot_config_dim",
                transformer=silver_robot_config_dim_transformer,
                schema_str=silver_robot_config_dim_schema,
                zorder_columns=DEFAULT_ZORDER_COLUMNS_SILVER1,
            ),
            TransConf(
                result_table="silver_kitchen_feed_names_dim",
                transformer=silver_kitchen_feed_names_dim_transformer,
                schema_str=silver_kitchen_feed_names_dim_schema,
                zorder_columns=DEFAULT_ZORDER_COLUMNS_SILVER1,
            ),
            TransConf(
                result_table="silver_ration_names_dim",
                transformer=silver_ration_names_dim_transformer,
                schema_str=silver_ration_names_dim_schema,
                zorder_columns=DEFAULT_ZORDER_COLUMNS_SILVER1,
            ),
        ],
        existing_tables=existing_tables,
    )
