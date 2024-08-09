from jobs.create_dashboards import CreateDashboards
from jobs.load_data_from_warehouse import LoadDataFromWarehouse
from jobs.process_bronze_to_silver0 import ProcessBronzeToSilver0
from jobs.process_silver0_to_silver1 import ProcessSilver0ToSilver1
from jobs.process_silver1_to_gold import ProcessSilver1ToGold
from jobs.test_gold_data import TestGoldData

if __name__ == "__main__":
    # TODO: fix KPI labels when visualizing
    # TODO: fix the dimension tables to have records before 2023
    # TODO: figure out how to optimize it
    # https://www.kaggle.com/datasets/nelgiriyewithana/cow-farms-feeding-robots

    config_file_path = r"conf/load_data_from_warehouse.json"

    task1 = LoadDataFromWarehouse(config_file_path)
    task2 = ProcessBronzeToSilver0(config_file_path)
    task3 = ProcessSilver0ToSilver1(config_file_path)
    task4 = ProcessSilver1ToGold(config_file_path)
    task5 = TestGoldData(config_file_path)
    task6 = CreateDashboards(config_file_path)

    task1.launch()
    task2.launch()
    task3.launch()
    task4.launch()
    task5.launch()
    task6.launch()
