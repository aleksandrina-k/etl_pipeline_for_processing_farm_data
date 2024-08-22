from pyspark.sql import functions as F
from transformers.parsers import parse_load_start
from ..conftest import spark  # noqa: F401, F403


def test_parse_load_start(spark):  # noqa: F811
    # Create sample DataFrame
    data = [
        (
            "LOAD_START",
            '{"rationId": 1, "reqWeight": 100, "startWeight": 0, "seqNr": 1}',
        ),
        ("OTHER_TYPE", '{"some_data": "value"}'),
    ]
    df = spark.createDataFrame(data, ["msg_type", "data"])

    # Call the function
    result_df = parse_load_start(df)

    # Assertions
    assert result_df.count() == 1
    assert result_df.filter(F.col("msg_type") != "LOAD_START").count() == 0
    assert (
        result_df.filter(F.col("msg_type") == "LOAD_START")
        .select("ration_id", "req_weight", "start_weight", "seq_nr")
        .count()
        == 1
    )
