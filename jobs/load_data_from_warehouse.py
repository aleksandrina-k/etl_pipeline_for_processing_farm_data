from pyspark.sql import functions as F
from pyspark.sql.types import _parse_datatype_string

from jobs.job import Job
from schemas.bronze import bronze_table_str


class LoadDataFromWarehouse(Job):
    def __init__(self, config_file_path, spark=None):
        Job.__init__(self, config_file_path, spark)

    def launch(self):
        self.logger.info("Starting Load Data From Warehouse Job")
        config = self.common_initialization()

        self.logger.info("Process bronze data from warehouse...")
        bronze_data_df = (
            self.spark.read.option("header", True)
            .option("escape", '"')
            .schema(_parse_datatype_string(bronze_table_str))
            .csv(f"{config.csv_data_location}\\bronze_table.csv")
            .withColumn("source_path", F.input_file_name())
            .withColumn("processing_time", F.current_timestamp())
        )
        bronze_data_df.limit(10).show(truncate=False)
        (
            bronze_data_df.write.format("delta")
            .mode("overwrite")
            .partitionBy("msg_type")
            .save(config.get_bronze_table_location())
        )
        self.logger.info("Bronze data stored as Delta table!")
