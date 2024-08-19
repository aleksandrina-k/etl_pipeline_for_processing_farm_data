from orchestration.trans_conf import TransConf
from schemas.gold import (
    feed_daily_fact_schema,
    ration_daily_fact_schema,
    farm_daily_fact_schema,
)
from transformers.gold import (
    feed_daily_fact_transformer,
    ration_daily_fact_transformer,
    farm_daily_fact_transformer,
)

DEFAULT_PARTITION_COLUMNS_GOLD = {"farm_license"}


def gold_mapping():
    """Settings for creating gold_layer table"""
    return [
        TransConf(
            result_table="feed_daily_fact",
            transformer=feed_daily_fact_transformer,
            schema_str=feed_daily_fact_schema,
            partition_columns=DEFAULT_PARTITION_COLUMNS_GOLD,
        ),
        TransConf(
            result_table="ration_daily_fact",
            transformer=ration_daily_fact_transformer,
            schema_str=ration_daily_fact_schema,
            partition_columns=DEFAULT_PARTITION_COLUMNS_GOLD,
        ),
        TransConf(
            result_table="farm_daily_fact",
            transformer=farm_daily_fact_transformer,
            schema_str=farm_daily_fact_schema,
            partition_columns=DEFAULT_PARTITION_COLUMNS_GOLD,
        ),
    ]
