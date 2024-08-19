from jobs.job import Job
from layer_mappings.gold_mapping import gold_mapping


class ProcessSilver1ToGold(Job):
    def __init__(self, config_file_path, spark=None):
        Job.__init__(self, config_file_path, spark)

    def launch(self):
        self.logger.info("Starting Process Silver1 To Gold Job")
        config = self.common_initialization()

        mapping = gold_mapping()
        for conf in mapping.transformations_in_order():
            conf.perform_transformation(
                spark=self.spark,
                input_dir_name=config.silver_dir_location,
                result_dir_location=config.gold_dir_location,
            )
