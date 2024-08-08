from pyspark.sql import functions as F
from transformers.parsers import parse_robot_config
from ..conftest import spark  # noqa: F401, F403


def test_parse_robot_config(spark):  # noqa: F811
    # Create sample DataFrame
    data = [
        (
            "ROBOT_CONFIG",
            '{"phases": "3", "freqTypeMixer": "A", "freqTypeRoller": "B", "relaysType": "C"}',
        ),
        ("OTHER_TYPE", '{"some_data": "value"}'),
    ]
    df = spark.createDataFrame(data, ["msg_type", "data"])

    # Call the function
    result_df = parse_robot_config(df)

    # Assertions
    assert result_df.count() == 1
    assert result_df.filter(F.col("msg_type") == "ROBOT_CONFIG").count() == 1
    assert (
        result_df.filter(F.col("msg_type") == "ROBOT_CONFIG")
        .select("phases", "freq_type_mixer", "freq_type_roller", "relays_type")
        .count()
        == 1
    )
