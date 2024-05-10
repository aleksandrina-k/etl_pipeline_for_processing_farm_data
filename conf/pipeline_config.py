from dataclasses import dataclass
from os import path


@dataclass
class PipelineConfig:
    def __init__(
            self,
            bronze_dir_name: str,
            silver_dir_name: str,
            gold_dir_name: str,
            farm_table_name: str,
            bronze_table_name: str,
            warehouse_folder_name: str,
    ):
        """
        Vector pipeline configuration
        """
        self.bronze_dir_name = bronze_dir_name
        self.silver_dir_name = silver_dir_name
        self.gold_dir_name = gold_dir_name
        self.farm_table_name = farm_table_name
        self.bronze_table_name = bronze_table_name
        self.warehouse_location = path.abspath(warehouse_folder_name)
        self.csv_data_location = path.join(self.warehouse_location, "csv_data")
        self.bronze_dir_location = path.join(self.warehouse_location, self.bronze_dir_name)
        self.silver_dir_location = path.join(self.warehouse_location, self.silver_dir_name)
        self.gold_dir_location = path.join(self.warehouse_location, self.gold_dir_name)

    def get_farm_table_location(self):
        return path.join(self.bronze_dir_location, self.farm_table_name)

    def get_bronze_table_location(self):
        return path.join(self.bronze_dir_location, self.bronze_table_name)

    def get_silver_table_location(self, table_name: str):
        return path.join(self.silver_dir_location, table_name)

    def get_gold_table_location(self, table_name: str):
        return path.join(self.gold_dir_location, table_name)
