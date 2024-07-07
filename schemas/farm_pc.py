import pyspark.sql.types as T

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
