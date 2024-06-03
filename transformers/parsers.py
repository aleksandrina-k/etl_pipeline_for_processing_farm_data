from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from schemas.mfr import mfr_config_schema, mfr_load_start_schema, mfr_load_done_schema
from schemas.t4c import t4c_kitchen_feed_names_schema, t4c_ration_names_schema


def parse_mfr_config(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("msg_type") == "MFR_CONFIG")
        .withColumn(
            "data", F.from_json(F.col("data").cast("string"), schema=mfr_config_schema)
        )
        .withColumn("phases", F.col("data.phases"))
        .withColumn("freq_type_mixer", F.col("data.freq_type_mixer"))
        .withColumn("freq_type_roller", F.col("data.freq_type_roller"))
        .withColumn("relays_type", F.col("data.relays_type"))
    )


# MFR_LOAD_START:
def parse_mfr_load_start(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("msg_type") == "MFR_LOAD_START")
        .withColumn(
            "data",
            F.from_json(F.col("data").cast("string"), schema=mfr_load_start_schema),
        )
        .withColumn("ration_id", F.col("data.rationId"))
        .withColumn("req_weight", F.col("data.reqWeight"))
        .withColumn("start_weight", F.col("data.startWeight"))
        .withColumn("seq_nr", F.col("data.seqNr"))
    )


# MFR_LOAD_DONE:
def parse_mfr_load_done(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("msg_type") == "MFR_LOAD_DONE")
        .withColumn(
            "data",
            F.from_json(F.col("data").cast("string"), schema=mfr_load_done_schema),
        )
        .withColumn("ration_id", F.col("data.rationId"))
        .withColumn("total_weight", F.col("data.weight"))
        .withColumn("seq_nr", F.col("data.seqNr"))
        .select("*", F.posexplode("data.results").alias("result_position", "result"))
        .withColumn("feed_id", F.col("result.feed_id"))
        .withColumn("req_weight", F.col("result.reqWeight"))
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
        .withColumn("ration_id", F.col("data.rationId"))
        .withColumn("total_weight", F.col("data.weight"))
        .withColumn("results", F.col("data.results"))
        .withColumn("seq_nr", F.col("data.seqNr"))
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
        .withColumn("feed_id", F.col("feedNames.feed_id"))
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
        .withColumn("ration_id", F.col("rationNames.rationId"))
        .withColumn("name", F.col("rationNames.name"))
        .drop("rationNames")
    )
