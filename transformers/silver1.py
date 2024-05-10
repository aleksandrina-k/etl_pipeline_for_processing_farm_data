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
        loadingUuid
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
        & (F.col("start.rationId") == F.col("end.rationId"))
        & (F.col("start.seqNr") == F.col("end.seqNr"))
        & (F.col("start.startTime") < F.col("end.endTime"))
        & (
            F.col("end.endTime").cast("long") - F.col("start.startTime").cast("long")
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
            "rationId",
            "reqWeight",
            "startWeight",
            "seqNr",
        )
        .withColumnRenamed("time", "startTime")
        .withColumnRenamed("reqWeight", "reqWeightRationG")
        .withColumnRenamed("startWeight", "weightAtLoadStartG")
        .withColumn("loadingUuid", uuid_udf())
    )

    end_for_join = (
        mfr_load_done_result.select(
            "farm_license",
            "system_number",
            "dev_number",
            "time",
            "rationId",
            "totalWeight",
            "seqNr",
            "results",
        )
        .withColumnRenamed("time", "endTime")
        .withColumnRenamed("totalWeight", "loadedWeightRationG")
    )

    mfr_loading_activity = (
        start_for_join.alias("start")
        .withWatermark("startTime", "12 HOURS")
        .join(
            end_for_join.alias("end").withWatermark("endTime", "12 HOURS"),
            on=join_condition,
            how="left",
        )
        .select(
            "start.farm_license",
            "start.system_number",
            "start.dev_number",
            "start.rationId",
            "start.seqNr",
            "start.year_month",
            "loadingUuid",
            "startTime",
            "endTime",
            "reqWeightRationG",
            "weightAtLoadStartG",
            "loadedWeightRationG",
            "results",
        )
        .withColumn(
            "durationS", F.col("endTime").cast("long") - F.col("startTime").cast("long")
        )
        .withColumn(
            "loadingSpeedRationGPerS", F.col("loadedWeightRationG") / F.col("durationS")
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
        a uuid in mfrConfigUuid
    """

    mfr_config = (
        mfr_config
        # drop unused columns
        .drop("dev_type", "msg_type", "processing_time", "data")
        # sometimes there are messages from other devices due to a mismatch between SW versions
        .filter(F.col("dev_type") == "MFR")
        # remove UNKNOWN relaysType, since they don't give information about MFR type
        .filter(F.col("relaysType") != "UNKNOWN")
    )

    mfr_config_dim = create_dim_table(
        mfr_config,
        partition_columns=["farm_license", "system_number", "dev_number"],
        column_names=["freqTypeMixer", "freqTypeRoller", "relaysType", "phases"],
    )

    return (
        mfr_config_dim.withColumn("mfrConfigUuid", uuid_udf())
        # Rename columns
        .withColumnRenamed("freqTypeMixer", "freqControllerTypeMixer")
        .withColumnRenamed("freqTypeRoller", "freqControllerTypeDosingRoller")
        .withColumnRenamed("relaysType", "relaysType")
        .replace(to_replace={"THREE_PHASE": "3", "ONE_PHASE": "1"}, subset="phases")
        .withColumn(
            "mfr_type",
            F.when(F.col("relaysType") == "EM773", "M1")
            .when(F.col("relaysType") == "SCHNEIDER", "M2")
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
        t4c_kitchen_feed_names
        # drop unused columns
        .drop("dev_number", "dev_type", "msg_type", "processing_time", "data")
        # removing feedId == 0, because they don't bring any value
        .filter(F.col("feedId") != 0)
    )

    return create_dim_table(
        feed_names,
        partition_columns=["farm_license", "system_number", "feedId"],
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
        t4c_ration_names
        # drop unused columns
        .drop("dev_number", "dev_type", "msg_type", "processing_time", "data")
        # removing rationId == 0, because they don't bring any value
        .filter(F.col("rationId") != 0)
    )

    return create_dim_table(
        ration_names,
        partition_columns=["farm_license", "system_number", "rationId"],
        column_names=["name"],
    )
