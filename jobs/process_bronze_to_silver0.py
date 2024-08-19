from jobs.job import Job
from os import path
from layer_mappings.parser_mapping import parser_mapping
from pyspark.sql import functions as F, DataFrame

from orchestration.trans_conf import TransConf


class ProcessBronzeToSilver0(Job):
    def __init__(self, config_file_path, spark=None):
        Job.__init__(self, config_file_path, spark)

    def launch(self):
        self.logger.info("Starting Process Bronze To Silver0 Job")
        config = self.common_initialization()

        self.logger.info("Read data from bronze table...")
        bronze_data_df = self.spark.read.load(config.get_bronze_table_location())
        bronze_data_df.cache()

        mapping = parser_mapping()
        for msg_type, confs in mapping.items():
            for conf in confs:
                self.process_bronze_to_msg_table(
                    conf=conf,
                    bronze_table_df=bronze_data_df,
                    result_dir_location=config.silver_dir_location,
                )

    def process_bronze_to_msg_table(
        self,
        conf: TransConf,
        bronze_table_df: DataFrame,
        result_dir_location: str,
    ):
        self.logger.info(f"Starting to process {conf.result_table}")
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

        df = self.spark.read.load(result_table_location)
        df.sort("farm_license", "time").limit(10).show(truncate=False)
