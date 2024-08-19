import logging
from os import path
from pyspark.sql import SparkSession, DataFrame, functions as F
from orchestration.trans_conf import TransConf


def process_bronze_to_msg_table(
    spark: SparkSession,
    conf: TransConf,
    bronze_table_df: DataFrame,
    result_dir_location: str,
):
    logger = logging.getLogger("CustomLogger")
    logger.info(f"Starting to process {conf.result_table}")
    result_table_location = path.join(result_dir_location, conf.result_table)

    silver_df = conf.transformer(bronze_table_df).withColumn(
        "year_month", F.date_format(F.col("time"), "yyyy-MM")
    )
    silver_df_deduplicated = silver_df.dropDuplicates(list(conf.match_columns))
    (
        silver_df_deduplicated.write.format("delta")
        .option("schema", conf.schema)
        .mode("overwrite")
        .partitionBy(list(conf.DEFAULT_PARTITION_COLUMNS))
        .save(result_table_location)
    )

    df = spark.read.load(result_table_location)
    df.sort("farm_license", "time").limit(10).show(truncate=False)
