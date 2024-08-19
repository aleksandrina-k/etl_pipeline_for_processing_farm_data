from pyspark.sql import functions as F
from chispa import assert_df_equality
from operations.helper_functions import create_dim_table, max_datetime


def test_create_dim_table(spark):
    # Sample data
    data = [
        (1, "2023-01-01", "A", "X"),
        (1, "2023-01-02", "A", "X"),
        (1, "2023-01-03", "B", "X"),
        (1, "2023-01-04", "B", "Y"),
        (2, "2023-01-01", "C", "Z"),
        (2, "2023-01-02", "C", "Z"),
    ]
    df = spark.createDataFrame(
        data, ["farm_license", "time", "col1", "col2"]
    ).withColumn("time", F.col("time").cast("timestamp"))

    # Expected output
    expected_data = [
        (1, "2023-01-01", "2023-01-03", "A", "X", 172800),
        (1, "2023-01-03", "2023-01-04", "B", "X", 86400),
        (1, "2023-01-04", str(max_datetime), "B", "Y", 2429654399),
        (2, "2023-01-01", str(max_datetime), "C", "Z", 2429913599),
    ]
    expected_df = spark.createDataFrame(
        expected_data,
        ["farm_license", "start_time", "end_time", "col1", "col2", "duration_s"],
    )
    expected_df = expected_df.withColumn(
        "end_time", F.col("end_time").cast("timestamp")
    ).withColumn("start_time", F.col("start_time").cast("timestamp"))

    # Call the function
    result_df = create_dim_table(df, ["farm_license"], ["col1", "col2"], "time")

    # Assert results
    assert_df_equality(result_df, expected_df, ignore_column_order=True)
