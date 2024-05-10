from os import path
from pyspark.sql.types import _parse_datatype_string
from generator.data_generator import generate_farm_data
from jobs.job import Job
from operations.helper_functions import merge_table
from schemas.bronze import farm_info_schema_str


class GenerateFarmData(Job):
    def __init__(self, config_file_path, spark=None):
        Job.__init__(self, config_file_path, spark)

    def launch(self):
        self.logger.info("Starting Generate Farm Data Job")
        config = self.common_initialization()

        # generate data
        farm_data = generate_farm_data(10)
        farm_info_df = self.spark.createDataFrame(
            farm_data, schema=_parse_datatype_string(farm_info_schema_str)
        )
        farm_info_df.show(truncate=False)

        table_location = path.join(config.db_location, config.farm_table_name)
        # farm_info_df.write.format("delta").mode("overwrite").save(table_location)

        df_before_merge = self.spark.read.load(table_location)
        df_before_merge.show(truncate=False)
        print(df_before_merge.count())

        # merge new generated data into existing delta table
        merge_table(
            self.spark,
            farm_info_df,
            table_location=table_location,
            match_columns={"farm_license", "system_number"},
        )

        df_after_merge = self.spark.read.load(table_location)
        df_after_merge.show(truncate=False)
        print(df_after_merge.count())
