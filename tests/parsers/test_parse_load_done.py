from pyspark.sql import functions as F
from transformers.parsers import parse_load_done, parse_load_done_result
from ..conftest import spark  # noqa: F401, F403


def test_parse_load_done(spark):  # noqa: F811
    # Create sample DataFrame
    data = [
        (
            "LOAD_DONE",
            '{"rationId": 1, "weight": 100, "seqNr": 1, "results": [{"feedId": 1, "reqWeight": 20, "weight": 15, '
            '"completed": true}, {"feedId": 2, "reqWeight": 30, "weight": 25, "completed": false}]}',
        ),
        ("OTHER_TYPE", '{"some_data": "value"}'),
    ]
    df = spark.createDataFrame(data, ["msg_type", "data"])

    # Call the function
    result_df = parse_load_done(df)

    # Assertions
    assert result_df.count() == 2
    assert result_df.filter(F.col("msg_type") == "LOAD_DONE").count() == 2
    assert (
        result_df.filter(F.col("msg_type") == "LOAD_DONE")
        .select("ration_id", "total_weight", "seq_nr")
        .distinct()
        .count()
    ), 1
    assert (
        result_df.filter(F.col("msg_type") == "LOAD_DONE")
        .select("feed_id", "req_weight", "weight", "completed")
        .distinct()
        .count()
    ), 2


def test_parse_load_done_result(spark):  # noqa: F811

    # Create sample DataFrame
    data = [
        (
            "LOAD_DONE",
            '{"rationId": 1, "weight": 100, "seqNr": 1, "results": [{"feedId": 1, "reqWeight": 20, '
            '"weight": 15, "completed": true}, {"feedId": 2, "reqWeight": 30, "weight": 25, "completed": '
            "false}]}",
        ),
        ("OTHER_TYPE", '{"some_data": "value"}'),
    ]
    df = spark.createDataFrame(data, ["msg_type", "data"])

    # Call the function
    result_df = parse_load_done_result(df)

    # Assertions
    assert result_df.count() == 1
    assert result_df.filter(F.col("msg_type") == "LOAD_DONE").count() == 1
    assert (
        result_df.filter(F.col("msg_type") == "LOAD_DONE")
        .select("ration_id", "total_weight", "seq_nr", "results")
        .count()
        == 1
    )
