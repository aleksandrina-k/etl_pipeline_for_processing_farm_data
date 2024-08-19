from orchestration.trans_conf import TransConf
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


def silver1_mapping():
    """Settings for creating silver1_layer table"""
    return [
        TransConf(
            result_table="silver_loading_activity",
            transformer=silver_loading_activity_transformer,
            schema_str=silver_loading_activity_schema,
        ),
        TransConf(
            result_table="silver_robot_config_dim",
            transformer=silver_robot_config_dim_transformer,
            schema_str=silver_robot_config_dim_schema,
        ),
        TransConf(
            result_table="silver_kitchen_feed_names_dim",
            transformer=silver_kitchen_feed_names_dim_transformer,
            schema_str=silver_kitchen_feed_names_dim_schema,
        ),
        TransConf(
            result_table="silver_ration_names_dim",
            transformer=silver_ration_names_dim_transformer,
            schema_str=silver_ration_names_dim_schema,
        ),
    ]
