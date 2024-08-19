from conf.pipeline_config import PipelineConfig
from pyspark.sql import functions as F
from jobs.job import Job
from layer_mappings.gold_mapping import gold_mapping
from operations.helper_functions import generate_calendar_table


class TestGoldData(Job):
    def __init__(self, config_file_path, spark=None):
        Job.__init__(self, config_file_path, spark)

    def test_all_gold_tables_not_empty(self, config: PipelineConfig):
        for conf in gold_mapping():
            table_name = conf.result_table
            gold_table_count = self.spark.read.load(
                f"{config.gold_dir_location}\\{table_name}"
            ).count()

            assert gold_table_count > 1, f"Table {table_name} should have some entries"
        self.logger.info("All gold tables have entries.")

    def test_daily_facts_have_one_record_per_day(self, config):
        # table: entity columns
        daily_facts = {
            "feed_daily_fact": ["farm_license", "feed_id"],
            "ration_daily_fact": ["farm_license", "ration_id"],
            "farm_daily_fact": ["farm_license"],
        }

        calendar_df = generate_calendar_table()

        for table_name, entity_columns in daily_facts.items():
            daily_table = self.spark.read.load(
                f"{config.gold_dir_location}\\{table_name}"
            )
            # get the first and the last date of the daily table
            min_date = daily_table.select(F.min("date")).collect()[0][0]
            max_date = daily_table.select(F.max("date")).collect()[0][0]
            calendar_for_table = calendar_df.where(F.col("date") >= min_date).where(
                F.col("date") <= max_date
            )

            joined = calendar_for_table.join(daily_table, on=["date"], how="left")

            # check for missing days
            missing_days_count = joined.where(F.col("date").isNull()).count()
            assert (
                missing_days_count == 0
            ), f"Table {table_name} has {missing_days_count} missing days."
        self.logger.info("None of the gold daily tables have missing days.")

    def launch(self):
        self.logger.info("Starting Test Gold Data Job")
        config = self.common_initialization()

        self.test_all_gold_tables_not_empty(config)
        self.test_daily_facts_have_one_record_per_day(config)
