import json
import logging
from abc import ABC, abstractmethod
from delta import *

from pyspark.sql import SparkSession
from typing import Dict, Any
import re

from conf.pipeline_config import PipelineConfig
from exceptions import ConfigParamMissingException


def remove_comments(json_with_comments: str) -> str:
    json_without_comments = re.sub(r"(\/\*).*?(\*\/)", "", json_with_comments)
    return json_without_comments


class Job(ABC):
    """
    Abstract class for jobs
    """

    def __init__(self, config_file_path: str, spark=None):
        self.spark = self._prepare_spark(spark)
        self.logger = self._prepare_logger()
        if not config_file_path:
            raise Exception("Config file path must be provided!")
        else:
            self.conf = self._provide_config(config_file_path)
        self._log_conf()

    @staticmethod
    def _prepare_spark(spark) -> SparkSession:
        if not spark:
            builder = (
                SparkSession.builder.master("local[*]").appName("Diplomna")
                    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog",
                            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                            )
                    .config("spark.executor.memory", "8g")
                    .config("spark.driver.memory", "8g")
                    .config("spark.driver.maxResultSize", "8g")
            )
            return configure_spark_with_delta_pip(builder).getOrCreate()
        else:
            return spark

    def _provide_config(self, config_file_path) -> Dict[str, Any]:
        self.logger.info(f"Reading configuration from {config_file_path}")
        raw_content = self._read_config_from_file(config_file_path)
        conf_file_parsed = Job._parse_config(raw_content)

        for key, value in conf_file_parsed.items():
            conf_file_parsed[key] = value

        self.logger.info(f"Resulting config: '{conf_file_parsed}'")

        return conf_file_parsed

    @staticmethod
    def _parse_config(raw_content: str) -> Dict[str, Any]:
        jsonstr = remove_comments(raw_content)
        config = json.loads(jsonstr)
        return config

    @staticmethod
    def _read_config_from_file(conf_file) -> str:
        f = open(conf_file, "r")
        raw_content = f.read()
        f.close()
        return raw_content

    @staticmethod
    def _update_spark_log_level(log_level="info"):
        levels_dict = {
            "info": logging.INFO,
            "warn": logging.WARNING,
            "error": logging.ERROR,
        }
        """
        Update the spark log level

        :param log_level: Log level
        """
        logging.getLogger("py4j").setLevel(levels_dict.get(log_level, "info"))

    def _prepare_logger(self) -> logging.Logger:
        # All messages go to spark log4j logger
        # log4j_logger = self.spark._jvm.org.apache.log4j  # noqa
        # return log4j_logger.LogManager.getLogger(self.__class__.__name__)

        # Set custom logger level
        self._update_spark_log_level("warn")
        # Use standard Python logger
        # Remove basic config, as some modules can set it, making setting it again it impossible
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            format="%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
            datefmt="%Y-%m-%d:%H:%M:%S",
            level=logging.INFO,
        )

        # Disable Azure HTTP Logging
        azure_logger = logging.getLogger(
            "azure.core.pipeline.policies.http_logging_policy"
        )
        azure_logger.setLevel(logging.WARNING)

        logger = logging.getLogger("CustomLogger")
        return logger

    def _log_conf(self):
        # log parameters
        self.logger.info("Launching jobs with configuration parameters:")
        for key, item in self.conf.items():
            self.logger.info("\t Parameter: %-30s with value => %-30s" % (key, item))

    def get_parameter(self, param_name: str):
        try:
            param = self.conf[param_name]
        except KeyError:
            raise ConfigParamMissingException(param_name)
        return param

    def common_initialization(self) -> PipelineConfig:
        return PipelineConfig(
            self.get_parameter("bronze_dir_name"),
            self.get_parameter("silver_dir_name"),
            self.get_parameter("gold_dir_name"),
            self.get_parameter("farm_table_name"),
            self.get_parameter("bronze_table_name"),
            self.get_parameter("warehouse_folder_name")
        )

    @abstractmethod
    def launch(self):
        """
        Main method of the jobs.
        :return:
        """
        pass
