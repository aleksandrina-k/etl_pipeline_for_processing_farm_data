from jobs.job import Job
from layer_mappings.silver1_mapping import silver1_mapping


class ProcessSilver0ToSilver1(Job):
    def __init__(self, config_file_path, spark=None):
        Job.__init__(self, config_file_path, spark)

    def launch(self):
        self.logger.info("Starting Process Silver0 To Silver1 Job")
        config = self.common_initialization()

        mapping = silver1_mapping()
        for conf in mapping.transformations_in_order():
            conf.perform_transformation(
                spark=self.spark,
                input_dir_name=config.silver_dir_location,
                result_dir_location=config.silver_dir_location,
            )
