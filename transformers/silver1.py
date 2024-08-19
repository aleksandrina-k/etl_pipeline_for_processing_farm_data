from pyspark.sql import DataFrame, functions as F
from helper_functions import uuid_udf, create_dim_table


def silver_loading_activity_transformer(
    load_start: DataFrame, load_done: DataFrame
) -> DataFrame:
    """
    The function combines the load_start and load_done messages into load activities.
    Args:
        load_start: The load_start messages
        load_done: The load_done
    Returns:
        A DataFrame with loading activities with start and end time and a uuid in
        loading_uuid
    """

    join_condition = (
        (F.col("start.farm_license") == F.col("end.farm_license"))
        & (F.col("start.device_number") == F.col("end.device_number"))
        & (F.col("start.ration_id") == F.col("end.ration_id"))
        & (F.col("start.seq_nr") == F.col("end.seq_nr"))
        & (F.col("start.start_time") < F.col("end.end_time"))
        & (
            F.col("end.end_time").cast("long") - F.col("start.start_time").cast("long")
            < 12 * 3600
        )
    )

    start_for_join = (
        load_start.select(
            "farm_license",
            "device_number",
            "time",
            "year_month",
            "ration_id",
            "req_weight",
            "start_weight",
            "seq_nr",
        )
        .withColumnRenamed("time", "start_time")
        .withColumnRenamed("req_weight", "req_weight_ration_g")
        .withColumnRenamed("start_weight", "weight_at_load_start_g")
        .withColumn("loading_uuid", uuid_udf())
    )

    end_for_join = (
        load_done.select(
            "farm_license",
            "device_number",
            "time",
            "ration_id",
            "total_weight",
            "seq_nr",
            "results",
        )
        .withColumnRenamed("time", "end_time")
        .withColumnRenamed("total_weight", "loaded_weight_ration_g")
    )

    loading_activity = (
        start_for_join.alias("start")
        .withWatermark("start_time", "12 HOURS")
        .join(
            end_for_join.alias("end").withWatermark("end_time", "12 HOURS"),
            on=join_condition,
            how="left",
        )
        .select(
            "start.farm_license",
            "start.device_number",
            "start.ration_id",
            "start.seq_nr",
            "start.year_month",
            "loading_uuid",
            "start_time",
            "end_time",
            "req_weight_ration_g",
            "weight_at_load_start_g",
            "loaded_weight_ration_g",
            "results",
        )
        .withColumn(
            "duration_s",
            F.col("end_time").cast("long") - F.col("start_time").cast("long"),
        )
        .withColumn(
            "loading_speed_ration_g_per_s",
            F.col("loaded_weight_ration_g") / F.col("duration_s"),
        )
    )

    return loading_activity


def silver_robot_config_dim_transformer(robot_config) -> DataFrame:
    """
    Create a dim DataFrame with robot configuration data with a uuid

    Args:
        robot_config: DataFrame with robot configuration data
    Returns:
        DataFrame with all new configurations per device, start and end time and
        a uuid in config_uuid
    """

    robot_config = (
        robot_config
        # drop unused columns
        .drop("device_type", "msg_type", "processing_time", "data", "source_path")
        # sometimes there are messages from other devices due to a mismatch between SW versions
        .filter(F.col("device_type") == "robot")
        # remove UNKNOWN relays_type, since they don't give information about the robot type
        .filter(F.col("relays_type") != "UNKNOWN")
    )

    config_dim = create_dim_table(
        robot_config,
        partition_columns=["farm_license", "device_number"],
        column_names=["freq_type_mixer", "freq_type_roller", "relays_type", "phases"],
    )

    return (
        config_dim.withColumn("config_uuid", uuid_udf())
        # Rename columns
        .withColumnRenamed("freq_type_mixer", "freq_controller_type_mixer")
        .withColumnRenamed("freq_type_roller", "freq_controller_type_dosing_roller")
        .withColumnRenamed("relays_type", "relays_type")
        .replace(to_replace={"THREE_PHASE": "3", "ONE_PHASE": "1"}, subset="phases")
    )


def silver_kitchen_feed_names_dim_transformer(kitchen_feed_names) -> DataFrame:
    """
    Args:
        kitchen_feed_names: Dataframe with data about names of different feed
    Returns:
        Enriched dataframe with valid from/to timestamps for each name.
    """

    feed_names = (
        # removing feed_id == 0, because they don't bring any value
        kitchen_feed_names.filter(F.col("feed_id") != 0)
        # fix wrongly encoded strings
        .withColumn(
            "encoded_name", F.decode(F.encode(F.col("name"), "ISO-8859-1"), "utf-8")
        )
        # if the name contains '�' or '?' after encode and decoding,
        # then the name was correct in the first place
        # there are names that originally contain '?',
        # we need to make sure that the '?' is a result of decoding
        .withColumn(
            "name",
            F.when(
                (F.col("encoded_name").contains("�"))
                | (
                    ~(F.col("name").contains("?"))
                    & (F.col("encoded_name").contains("?"))
                ),
                F.col("name"),
            ).otherwise(F.col("encoded_name")),
        ).drop("encoded_name")
        # drop unused columns
        .drop(
            "device_number",
            "device_type",
            "msg_type",
            "processing_time",
            "data",
            "source_path",
        )
    )

    return create_dim_table(
        feed_names,
        partition_columns=["farm_license", "feed_id"],
        column_names=["name"],
    )


def silver_ration_names_dim_transformer(ration_names) -> DataFrame:
    """
    Args:
        ration_names: Dataframe with data about names of different rations
    Returns:
        Enriched dataframe with valid from/to timestamps for each name.
    """

    ration_names = (
        # removing ration_id == 0, because they don't bring any value
        ration_names.filter(F.col("ration_id") != 0)
        # fix wrongly encoded strings
        .withColumn(
            "encoded_name", F.decode(F.encode(F.col("name"), "ISO-8859-1"), "utf-8")
        )
        # if the name contains '�' or '?' after encode and decoding,
        # then the name was correct in the first place
        # there are names that originally contain '?',
        # we need to make sure that the '?' is a result of decoding
        .withColumn(
            "name",
            F.when(
                (F.col("encoded_name").contains("�"))
                | (
                    ~(F.col("name").contains("?"))
                    & (F.col("encoded_name").contains("?"))
                ),
                F.col("name"),
            ).otherwise(F.col("encoded_name")),
        ).drop("encoded_name")
        # drop unused columns
        # drop unused columns
        .drop(
            "device_number",
            "device_type",
            "msg_type",
            "processing_time",
            "data",
            "source_path",
        )
    )

    return create_dim_table(
        ration_names,
        partition_columns=["farm_license", "ration_id"],
        column_names=["name"],
    )
