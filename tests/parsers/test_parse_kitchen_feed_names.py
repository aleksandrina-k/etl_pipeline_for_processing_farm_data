from pyspark.sql import functions as F
from transformers.parsers import parse_kitchen_feed_names
from ..conftest import spark  # noqa: F401, F403


def test_parse_kitchen_feed_names(spark):  # noqa: F811
    # Create sample DataFrame with nested array
    data = [
        (
            "KITCHEN_FEED_NAMES",
            '{"feedNames": [{"feedId": 1, "name": "Feed A"}, {"feedId": 2, "name": "Feed B"}]}',
        ),
        ("OTHER_TYPE", '{"some_data": "value"}'),
    ]
    df = spark.createDataFrame(data, ["msg_type", "data"])

    # Call the function
    result_df = parse_kitchen_feed_names(df)

    # Assertions
    assert result_df.count() == 2
    assert result_df.filter(F.col("msg_type") != "KITCHEN_FEED_NAMES").count() == 0
    assert (
        result_df.filter(F.col("msg_type") == "KITCHEN_FEED_NAMES")
        .select("feed_id", "name")
        .distinct()
        .count()
        == 2
    )
