from layer_mappings.silver1_mapping import silver1_mapping
from orchestration.trans_conf import TransConf
from orchestration.trans_mapping import TransMapping
from schemas.gold import (
    feed_daily_fact_schema,
    ration_daily_fact_schema,
    mfr_daily_fact_schema,
)
from transformers.gold import (
    feed_daily_fact_transformer,
    ration_daily_fact_transformer,
    mfr_daily_fact_transformer,
)

DEFAULT_PARTITION_COLUMNS_GOLD = {"farm_license", "system_number"}
DEFAULT_ZORDER_COLUMNS_GOLD = {"date"}


def gold_mapping():
    """Settings for creating gold_layer table"""
    return TransMapping(
        confs=[
            TransConf(
                result_table="feed_daily_fact",
                transformer=feed_daily_fact_transformer,
                schema_str=feed_daily_fact_schema,
                partition_columns=DEFAULT_PARTITION_COLUMNS_GOLD,
                zorder_columns=DEFAULT_ZORDER_COLUMNS_GOLD,
            ),
            TransConf(
                result_table="ration_daily_fact",
                transformer=ration_daily_fact_transformer,
                schema_str=ration_daily_fact_schema,
                partition_columns=DEFAULT_PARTITION_COLUMNS_GOLD,
                zorder_columns=DEFAULT_ZORDER_COLUMNS_GOLD,
            ),
            TransConf(
                result_table="mfr_daily_fact",
                transformer=mfr_daily_fact_transformer,
                schema_str=mfr_daily_fact_schema,
                partition_columns=DEFAULT_PARTITION_COLUMNS_GOLD,
                zorder_columns=DEFAULT_ZORDER_COLUMNS_GOLD,
            ),
        ],
        existing_tables=silver1_mapping().get_all_tables(),
    )
