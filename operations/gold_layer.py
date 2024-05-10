from pyspark.sql import SparkSession
from conf.pipeline_config import PipelineConfig
from layer_mappings.gold_mapping import gold_mapping
from operations.helper_functions import perform_transformation


def process_silver1_to_gold(spark: SparkSession, config: PipelineConfig):
    mapping = gold_mapping()
    perform_transformation(
        spark=spark,
        mapping=mapping,
        input_dir_location=config.silver_dir_location,
        result_dir_location=config.gold_dir_location,
    )
