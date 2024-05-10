import pyspark.sql.types as T

mfr_config_schema = T.StructType(
    [
        T.StructField("phases", T.StringType(), False),
        T.StructField("freqTypeMixer", T.StringType(), False),
        T.StructField("freqTypeRoller", T.StringType(), False),
        T.StructField("relaysType", T.StringType(), False),
    ]
)

mfr_load_done_schema = T.StructType(
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

mfr_load_start_schema = T.StructType(
    [
        T.StructField("rationId", T.IntegerType(), False),
        T.StructField("reqWeight", T.IntegerType(), False),
        T.StructField("startWeight", T.IntegerType(), False),
        T.StructField("seqNr", T.IntegerType(), False),
    ]
)