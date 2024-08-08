import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[*]")
        .appName("vector-databricks-data-pipeline-tests")
        .getOrCreate()
    )
