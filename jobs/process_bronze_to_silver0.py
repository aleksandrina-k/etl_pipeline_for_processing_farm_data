from jobs.job import Job
from operations.silver0_layer import process_silver0_to_bronze


class ProcessBronzeToSilver0(Job):
    def __init__(self, config_file_path, spark=None):
        Job.__init__(self, config_file_path, spark)

    def launch(self):
        self.logger.info("Starting Process Bronze To Silver0 Job")
        config = self.common_initialization()

        process_silver0_to_bronze(self.spark, config)
