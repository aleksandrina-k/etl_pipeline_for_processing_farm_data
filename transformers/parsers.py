from pyspark.sql import functions as F
from pyspark.sql import DataFrame


# MFR_CONFIG:
from schemas.mfr import mfr_config_schema, mfr_load_start_schema, mfr_load_done_schema
from schemas.t4c import t4c_kitchen_feed_names_schema, t4c_ration_names_schema


def parse_mfr_config(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("msg_type") == "MFR_CONFIG")
        .withColumn(
            "data", F.from_json(F.col("data").cast("string"), schema=mfr_config_schema)
        )
        .withColumn("phases", F.col("data.phases"))
        .withColumn("freqTypeMixer", F.col("data.freqTypeMixer"))
        .withColumn("freqTypeRoller", F.col("data.freqTypeRoller"))
        .withColumn("relaysType", F.col("data.relaysType"))
    )


# MFR_LOAD_START:
def parse_mfr_load_start(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("msg_type") == "MFR_LOAD_START")
        .withColumn(
            "data",
            F.from_json(F.col("data").cast("string"), schema=mfr_load_start_schema),
        )
        .withColumn("rationId", F.col("data.rationId"))
        .withColumn("reqWeight", F.col("data.reqWeight"))
        .withColumn("startWeight", F.col("data.startWeight"))
        .withColumn("seqNr", F.col("data.seqNr"))
    )


# MFR_LOAD_DONE:
def parse_mfr_load_done(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("msg_type") == "MFR_LOAD_DONE")
        .withColumn(
            "data",
            F.from_json(F.col("data").cast("string"), schema=mfr_load_done_schema),
        )
        .withColumn("rationId", F.col("data.rationId"))
        .withColumn("totalWeight", F.col("data.weight"))
        .withColumn("seqNr", F.col("data.seqNr"))
        .select("*", F.posexplode("data.results").alias("result_position", "result"))
        .withColumn("feedId", F.col("result.feedId"))
        .withColumn("reqWeight", F.col("result.reqWeight"))
        .withColumn("weight", F.col("result.weight"))
        .withColumn("completed", F.col("result.completed"))
        .drop("result", "result_position")
    )


def parse_mfr_load_done_result(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("msg_type") == "MFR_LOAD_DONE")
        .withColumn(
            "data",
            F.from_json(F.col("data").cast("string"), schema=mfr_load_done_schema),
        )
        .withColumn("rationId", F.col("data.rationId"))
        .withColumn("totalWeight", F.col("data.weight"))
        .withColumn("results", F.col("data.results"))
        .withColumn("seqNr", F.col("data.seqNr"))
    )


# T4C_KITCHEN_FEED_NAMES:
def parse_t4c_kitchen_feed_names(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("msg_type") == "T4C_KITCHEN_FEED_NAMES")
        .withColumn(
            "data",
            F.from_json(
                F.col("data").cast("string"), schema=t4c_kitchen_feed_names_schema
            ),
        )
        .withColumn("feedNames", F.explode(F.col("data.feedNames")))
        .withColumn("feedId", F.col("feedNames.feedId"))
        .withColumn("name", F.col("feedNames.name"))
        .drop("feedNames")
    )


# T4C_RATION_NAMES:
def parse_t4c_ration_names(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("msg_type") == "T4C_RATION_NAMES")
        .withColumn(
            "data",
            F.from_json(F.col("data").cast("string"), schema=t4c_ration_names_schema),
        )
        .withColumn("rationNames", F.explode(F.col("data.rationNames")))
        .withColumn("rationId", F.col("rationNames.rationId"))
        .withColumn("name", F.col("rationNames.name"))
        .drop("rationNames")
    )
