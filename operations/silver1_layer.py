from pyspark.sql import SparkSession
from conf.pipeline_config import PipelineConfig
from layer_mappings.silver1_mapping import silver1_mapping
from operations.helper_functions import perform_transformation


def process_silver0_to_silver1(spark: SparkSession, config: PipelineConfig):
    mapping = silver1_mapping()
    perform_transformation(
        spark=spark,
        mapping=mapping,
        input_dir_location=config.silver_dir_location,
        result_dir_location=config.silver_dir_location
    )
