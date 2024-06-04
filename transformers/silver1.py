from pyspark.sql import DataFrame, functions as F
from operations.helper_functions import uuid_udf, create_dim_table


def silver_mfr_loading_activity_transformer(
    mfr_load_start: DataFrame, mfr_load_done_result: DataFrame
) -> DataFrame:
    """
    The function combines the mfr_load_start and mfr_load_done messages into load activities.
    Args:
        mfr_load_start: The mfr_load_start messages
        mfr_load_done_result: The mfr_load_done_results
    Returns:
        A DataFrame with mfr_loading activities with start and end time and a uuid in
        loading_uuid
    Note:
        Sequence numbers are not unique and can be reused by the system. The same sequence
        number can thus occur on multiple days. As a result, joining on the conditions
        sequence number are equal and start time is smaller than end time would result in
        multiple matches (i.e., all start messages before today will match today's end message
        if they have the same sequence number). As the same sequence number is not (for the MFR)
        reused on the same day, we use a 12hour window.
    """

    join_condition = (
        (F.col("start.farm_license") == F.col("end.farm_license"))
        & (F.col("start.system_number") == F.col("end.system_number"))
        & (F.col("start.dev_number") == F.col("end.dev_number"))
        & (F.col("start.ration_id") == F.col("end.ration_id"))
        & (F.col("start.seq_nr") == F.col("end.seq_nr"))
        & (F.col("start.start_time") < F.col("end.end_time"))
        & (
            F.col("end.end_time").cast("long") - F.col("start.start_time").cast("long")
            < 12 * 3600
        )
    )

    start_for_join = (
        mfr_load_start.select(
            "farm_license",
            "system_number",
            "dev_number",
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
        mfr_load_done_result.select(
            "farm_license",
            "system_number",
            "dev_number",
            "time",
            "ration_id",
            "total_weight",
            "seq_nr",
            "results",
        )
        .withColumnRenamed("time", "end_time")
        .withColumnRenamed("total_weight", "loaded_weight_ration_g")
    )

    mfr_loading_activity = (
        start_for_join.alias("start")
        .withWatermark("start_time", "12 HOURS")
        .join(
            end_for_join.alias("end").withWatermark("end_time", "12 HOURS"),
            on=join_condition,
            how="left",
        )
        .select(
            "start.farm_license",
            "start.system_number",
            "start.dev_number",
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

    return mfr_loading_activity


def silver_mfr_config_dim_transformer(mfr_config) -> DataFrame:
    """
    Create a dim DataFrame with mfr configuration data with a uuid

    Args:
        mfr_config: DataFrame with mfr_config data
    Returns:
        DataFrame with all new configurations per device, start and end time and
        a uuid in mfr_config_uuid
    """

    mfr_config = (
        mfr_config
        # drop unused columns
        .drop("dev_type", "msg_type", "processing_time", "data")
        # sometimes there are messages from other devices due to a mismatch between SW versions
        .filter(F.col("dev_type") == "MFR")
        # remove UNKNOWN relays_type, since they don't give information about MFR type
        .filter(F.col("relays_type") != "UNKNOWN")
    )

    mfr_config_dim = create_dim_table(
        mfr_config,
        partition_columns=["farm_license", "system_number", "dev_number"],
        column_names=["freq_type_mixer", "freq_type_roller", "relays_type", "phases"],
    )

    return (
        mfr_config_dim.withColumn("mfr_config_uuid", uuid_udf())
        # Rename columns
        .withColumnRenamed("freq_type_mixer", "freq_controller_type_mixer")
        .withColumnRenamed("freq_type_roller", "freq_controller_type_dosing_roller")
        .withColumnRenamed("relays_type", "relays_type")
        .replace(to_replace={"THREE_PHASE": "3", "ONE_PHASE": "1"}, subset="phases")
        .withColumn(
            "mfr_type",
            F.when(F.col("relays_type") == "EM773", "M1")
            .when(F.col("relays_type") == "SCHNEIDER", "M2")
            .otherwise("M3"),
        )
    )


def silver_kitchen_feed_names_dim_transformer(t4c_kitchen_feed_names) -> DataFrame:
    """
    Args:
        t4c_kitchen_feed_names: Dataframe with data about names of different feed
    Returns:
        Enriched dataframe with valid from/to timestamps for each name.
    """

    feed_names = (
        # removing feed_id == 0, because they don't bring any value
        t4c_kitchen_feed_names.filter(F.col("feed_id") != 0)
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
        .drop("dev_number", "dev_type", "msg_type", "processing_time", "data")
    )

    return create_dim_table(
        feed_names,
        partition_columns=["farm_license", "system_number", "feed_id"],
        column_names=["name"],
    )


def silver_ration_names_dim_transformer(t4c_ration_names) -> DataFrame:
    """
    Args:
        t4c_ration_names: Dataframe with data about names of different rations
    Returns:
        Enriched dataframe with valid from/to timestamps for each name.
    """

    ration_names = (
        # removing ration_id == 0, because they don't bring any value
        t4c_ration_names.filter(F.col("ration_id") != 0)
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
        .drop("dev_number", "dev_type", "msg_type", "processing_time", "data")
    )

    return create_dim_table(
        ration_names,
        partition_columns=["farm_license", "system_number", "ration_id"],
        column_names=["name"],
    )
