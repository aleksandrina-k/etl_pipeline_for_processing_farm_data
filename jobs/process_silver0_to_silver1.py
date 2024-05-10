from jobs.job import Job
from operations.silver1_layer import process_silver0_to_silver1


class ProcessSilver0ToSilver1(Job):
    def __init__(self, config_file_path, spark=None):
        Job.__init__(self, config_file_path, spark)

    def launch(self):
        self.logger.info("Starting Process Silver0 To Silver1 Job")
        config = self.common_initialization()

        process_silver0_to_silver1(self.spark, config)
