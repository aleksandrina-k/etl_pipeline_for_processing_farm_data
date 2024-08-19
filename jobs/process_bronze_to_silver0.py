from jobs.job import Job
from layer_mappings.parser_mapping import parser_mapping
from operations.silver0_layer import process_bronze_to_msg_table


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
                process_bronze_to_msg_table(
                    spark=self.spark,
                    conf=conf,
                    bronze_table_df=bronze_data_df,
                    result_dir_location=config.silver_dir_location,
                )
