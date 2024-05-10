from jobs.job import Job
from operations.gold_layer import process_silver1_to_gold


class ProcessSilver1ToGold(Job):

    def __init__(self, config_file_path, spark=None):
        Job.__init__(self, config_file_path, spark)

    def launch(self):
        self.logger.info("Starting Process Silver1 To Gold Job")
        config = self.common_initialization()

        bronze_data_df = self.spark.read.load(config.get_bronze_table_location())

        process_silver1_to_gold(self.spark, config)
