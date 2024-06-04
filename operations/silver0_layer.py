import logging
from os import path
from pyspark.sql import SparkSession, DataFrame, functions as F
from conf.pipeline_config import PipelineConfig
from layer_mappings.parser_mapping import parser_mapping
from orchestration.parser_conf import ParserConf


def process_silver0_to_bronze(spark: SparkSession, config: PipelineConfig):
    logger = logging.getLogger("CustomLogger")
    logger.info("Read data from bronze table...")
    bronze_data_df = spark.read.load(config.get_bronze_table_location())
    bronze_data_df.cache()

    mapping = parser_mapping()
    for msg_type, confs in mapping.items():
        for conf in confs:
            _process_bronze_to_msg_table(
                spark=spark,
                conf=conf,
                bronze_table_df=bronze_data_df,
                result_dir_location=config.silver_dir_location,
            )


def _process_bronze_to_msg_table(
    spark: SparkSession,
    conf: ParserConf,
    bronze_table_df: DataFrame,
    result_dir_location: str,
):
    logger = logging.getLogger("CustomLogger")
    logger.info(f"Starting to process {conf.result_table}")
    result_table_location = path.join(result_dir_location, conf.result_table)

    silver_df = conf.parser(bronze_table_df).withColumn(
        "year_month", F.date_format(F.col("time"), "yyyy-MM")
    )
    silver_df_deduplicated = silver_df.dropDuplicates(list(conf.match_columns))
    (
        silver_df_deduplicated.write.format("delta")
        .option("schema", conf.schema)
        .mode("append")
        .partitionBy(list(conf.DEFAULT_PARTITION_COLUMNS))
        .save(result_table_location)
    )

    df = spark.read.load(result_table_location)
    df.limit(10).show(truncate=False)

    conf.optimize_result_table(spark, result_dir_location)
