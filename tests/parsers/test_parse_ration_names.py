from pyspark.sql import functions as F
from transformers.parsers import parse_ration_names
from ..conftest import spark  # noqa: F401, F403


def test_ration_names(spark):  # noqa: F811
    # Create sample DataFrame with nested array
    data = [
        (
            "RATION_NAMES",
            '{"rationNames": [{"rationId": 1, "name": "Feed A"}, {"rationId": 2, "name": "Feed B"}]}',
        ),
        ("OTHER_TYPE", '{"some_data": "value"}'),
    ]
    df = spark.createDataFrame(data, ["msg_type", "data"])

    # Call the function
    result_df = parse_ration_names(df)

    # Assertions
    assert result_df.count() == 2
    assert result_df.filter(F.col("msg_type") == "RATION_NAMES").count() == 2
    assert (
        result_df.filter(F.col("msg_type") == "RATION_NAMES")
        .select("ration_id", "name")
        .count()
        == 2
    )
