from pyspark.sql import types as T

kitchen_feed_names_schema = T.StructType(
    [
        T.StructField(
            "feedNames",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("feedId", T.IntegerType(), False),
                        T.StructField("name", T.StringType(), False),
                    ]
                )
            ),
        )
    ]
)

ration_names_schema = T.StructType(
    [
        T.StructField(
            "rationNames",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("rationId", T.IntegerType(), False),
                        T.StructField("name", T.StringType(), False),
                    ]
                )
            ),
        )
    ]
)
robot_config_schema = T.StructType(
    [
        T.StructField("phases", T.StringType(), False),
        T.StructField("freqTypeMixer", T.StringType(), False),
        T.StructField("freqTypeRoller", T.StringType(), False),
        T.StructField("relaysType", T.StringType(), False),
    ]
)
load_done_schema = T.StructType(
    [
        T.StructField("rationId", T.IntegerType(), False),
        T.StructField("weight", T.IntegerType(), False),
        T.StructField(
            "results",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("feedId", T.IntegerType(), False),
                        T.StructField("reqWeight", T.IntegerType(), False),
                        T.StructField("weight", T.IntegerType(), False),
                        T.StructField("completed", T.BooleanType(), False),
                    ]
                )
            ),
        ),
        T.StructField("seqNr", T.IntegerType(), False),
    ]
)
load_start_schema = T.StructType(
    [
        T.StructField("rationId", T.IntegerType(), False),
        T.StructField("reqWeight", T.IntegerType(), False),
        T.StructField("startWeight", T.IntegerType(), False),
        T.StructField("seqNr", T.IntegerType(), False),
    ]
)
