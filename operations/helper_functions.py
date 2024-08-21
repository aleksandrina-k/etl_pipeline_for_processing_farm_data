from datetime import datetime, date, timedelta
from typing import List
from pyspark.sql import SparkSession, DataFrame, functions as F, Window
from pyspark.sql.types import StructType, StructField, DateType

max_datetime = datetime(2099, 12, 31, 23, 59, 59)


def uuid_udf():
    return F.expr("uuid()")


def generate_calendar_table() -> DataFrame:
    spark = SparkSession.builder.getOrCreate()

    schema = StructType([StructField("date", DateType(), False)])

    start_date = date(2023, 1, 1)
    end_date = date(2023, 12, 31)
    dates = [
        (start_date + timedelta(days=i),)
        for i in range((end_date - start_date).days + 1)
    ]

    return spark.createDataFrame(dates, schema)


def create_dim_table(
    df: DataFrame,
    partition_columns: list,
    column_names: list,
    orderby_col: str = "time",
):
    """
    Usage: the current silver tables have time periods for which all
    the settings are valid. As soon as 1 of the settings changes a new
    time period is started. However, sometimes we are not interested in
    all settings, but just a limited set. It might be convenient to tailor
    the existing time periods to that.

    Args:
        df (DataFrame): dataframe for which the new time periods
        will be determined,
        orderby_col (str): column the Window to be ordered by,
        partition_columns (list): columns used to partition df,
        column_names (list): list of columns we want to determine
        start and end time for,

    Returns: A dataframe with "farm_license", "start_time", "end_time",
    and all specified columns we used to determine the start and end time for.
    """
    farm_window = Window.partitionBy(*partition_columns).orderBy(orderby_col)

    dim_table = (
        df
        # # Only keep information of interest
        # .select(*partition_columns, orderby_col, *column_names)
        .withColumn("data_condensed", F.concat_ws(", ", *column_names))
        .withColumn(
            "changed",
            F.col("data_condensed") != F.lag("data_condensed").over(farm_window),
        )
        .filter(F.col("changed").isNull() | F.col("changed"))
        # when there is no end date, use the end date time thing
        .withColumn(
            "end_time",
            F.lead(orderby_col, default=max_datetime).over(farm_window),
        )
        .withColumnRenamed("time", "start_time")
        .withColumn(
            "duration_s",
            F.col("end_time").cast("long") - F.col("start_time").cast("long"),
        )
        .drop("data_condensed", "changed")
    )
    return dim_table


def split_carryover_items_factory(
    event_start_time_column: str,
    event_end_time_column: str,
    use_posexplode: bool = False,
):
    """Factory function that constructs the pyspark transformation to split items into chunks of
    a certain window duration.
    Args:
        event_start_time_column (str): Name of column holding the events' start times
        event_end_time_column (str): Name of the column holding the events' end times
        use_posexplode (bool): If true the position of the new timestamps will be returned in
        column 'pos'
    """

    def inner(
        input_df: DataFrame, window_duration: str, partition_fields: List[str]
    ) -> DataFrame:
        """Method to split items into chunks of `window_duration` length.
        Function can be used for example during the computation of KPI's on some window,
        while events span over that window.


        Args:
            input_df (DataFrame): Dataframe with events that should be split into chuncks
            window_duration (str): Window duration declaration (e.g. '1 day')
            partition_fields (List[str]): List of fields on which data should be partitioned.
            Setting this field will help massively to parellelize the workload.

        Returns:
            DataFrame: Dataframe with events split into multiple subevents if they span over the
            window duration.
        """
        explode_func = F.posexplode if use_posexplode else F.explode
        alias_fields = ["pos", "boundary"] if use_posexplode else ["boundary"]

        id_window = Window.partitionBy("id", *partition_fields).orderBy("boundary")

        output = (
            input_df.withColumn("id", F.monotonically_increasing_id())
            .withColumn("window", F.window(event_start_time_column, window_duration))
            # Extact the window start and end from the window into columns
            # .withColumn("window_start", F.date_add(F.col("window.start"), 1))
            # .withColumn("window_end", F.date_add(F.col("window.end"), 1))
            # note: these lines doesn't work as expected with pyspark==3.2.0, so they are replaced with the ones above
            .withColumn(
                "window_start", F.date_add(F.col("window.start"), 1).cast("timestamp")
            )
            .withColumn(
                "window_end", F.date_add(F.col("window.end"), 1).cast("timestamp")
            )
            .drop(F.col("window"))
            .select(
                "*",
                # create a row for the given interval between
                # start and end
                explode_func(
                    F.sequence(
                        "window_start",
                        F.coalesce(event_end_time_column, "window_end"),
                        F.expr(f"interval {window_duration}"),
                    )
                ).alias(*alias_fields),
            )
            .withColumn(
                "windowed_start_time",
                F.greatest(F.col(event_start_time_column), F.col("boundary")),
            )
            .withColumn("end_time_cutoff", F.lag(F.col("boundary"), -1).over(id_window))
            .withColumn(
                "windowed_end_time",
                F.coalesce("end_time_cutoff", event_end_time_column),
            )
            .where(F.col("windowed_end_time").isNotNull())
            .withColumn(
                "windowed_end_time",
                F.when(F.col(event_end_time_column).isNull(), None).otherwise(
                    F.col("windowed_end_time")
                ),
            )
            .drop(event_start_time_column, event_end_time_column)
            .withColumnRenamed("windowed_start_time", event_start_time_column)
            .withColumnRenamed("windowed_end_time", event_end_time_column)
            .withColumn(
                "_duration",
                F.col(event_end_time_column).cast("long")
                - F.col(event_start_time_column).cast("long"),
            )
            .withColumn("start_date", F.to_date(F.col(event_start_time_column)))
            .withColumn("end_date", F.to_date(F.col(event_end_time_column)))
            .where((F.col("_duration") > 0) | (F.col(event_end_time_column).isNull()))
            .select(*input_df.columns, "start_date", "end_date", *alias_fields)
            .drop("boundary")
        )
        return output

    return inner


def extract_all_farm_licenses(table_location: str) -> list:
    """
    :param table_location: farm table location in the file system
    :return: Sorted list of all farm licenses
    """
    spark = SparkSession.builder.getOrCreate()
    farms = [
        x[0]
        for x in spark.read.load(table_location)
        .groupBy("farm_license")
        .count()
        .select("farm_license")
        .collect()
    ]
    farms.sort()
    return farms
