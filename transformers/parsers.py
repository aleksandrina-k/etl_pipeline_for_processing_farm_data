from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from schemas.structured import (
    kitchen_feed_names_schema,
    ration_names_schema,
    robot_config_schema,
    load_done_schema,
    load_start_schema,
)


# ROBOT CONFIG
def parse_robot_config(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("msg_type") == "ROBOT_CONFIG")
        .withColumn(
            "data",
            F.from_json(F.col("data").cast("string"), schema=robot_config_schema),
        )
        .withColumn("phases", F.col("data.phases"))
        .withColumn("freq_type_mixer", F.col("data.freqTypeMixer"))
        .withColumn("freq_type_roller", F.col("data.freqTypeRoller"))
        .withColumn("relays_type", F.col("data.relaysType"))
    )


# LOAD_START:
def parse_load_start(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("msg_type") == "LOAD_START")
        .withColumn(
            "data",
            F.from_json(F.col("data").cast("string"), schema=load_start_schema),
        )
        .withColumn("ration_id", F.col("data.rationId"))
        .withColumn("req_weight", F.col("data.reqWeight"))
        .withColumn("start_weight", F.col("data.startWeight"))
        .withColumn("seq_nr", F.col("data.seqNr"))
    )


# LOAD_DONE:
def parse_load_done(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("msg_type") == "LOAD_DONE")
        .withColumn(
            "data",
            F.from_json(F.col("data").cast("string"), schema=load_done_schema),
        )
        .withColumn("ration_id", F.col("data.rationId"))
        .withColumn("total_weight", F.col("data.weight"))
        .withColumn("results", F.col("data.results"))
        .withColumn("seq_nr", F.col("data.seqNr"))
    )


# KITCHEN_FEED_NAMES:
def parse_kitchen_feed_names(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("msg_type") == "KITCHEN_FEED_NAMES")
        .withColumn(
            "data",
            F.from_json(F.col("data").cast("string"), schema=kitchen_feed_names_schema),
        )
        .withColumn("feedNames", F.explode(F.col("data.feedNames")))
        .withColumn("feed_id", F.col("feedNames.feedId"))
        .withColumn("name", F.col("feedNames.name"))
        .drop("feedNames")
    )


# RATION_NAMES:
def parse_ration_names(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("msg_type") == "RATION_NAMES")
        .withColumn(
            "data",
            F.from_json(F.col("data").cast("string"), schema=ration_names_schema),
        )
        .withColumn("rationNames", F.explode(F.col("data.rationNames")))
        .withColumn("ration_id", F.col("rationNames.rationId"))
        .withColumn("name", F.col("rationNames.name"))
        .drop("rationNames")
    )
