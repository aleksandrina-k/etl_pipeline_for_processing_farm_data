from pyspark.sql import DataFrame, functions as F, Window

from operations.helper_functions import create_dim_table, split_carryover_items_factory

MFR1_DEV_LDN = [71, 118]
MFR2_DEV_LDN = [72, 119]


def _explode_mfr_loading_activity(
    silver_mfr_loading_activity: DataFrame,
) -> DataFrame:
    loading_details = (
        silver_mfr_loading_activity.select(
            # By exploding results we get a row for every feedId in the ration
            "*",
            F.explode("results").alias("loadResult"),
        )
        # Next, we need to unnest the feed specific information so that the info becomes 'queryable'
        .withColumn("feedId", F.col("loadResult.feedId"))
        .withColumn("reqWeightFeedtypeG", F.col("loadResult.reqWeight"))
        .withColumn("loadedWeightFeedtypeG", F.col("loadResult.weight"))
        .withColumn("feedtypeCompleted", F.col("loadResult.completed"))
        .drop("loadResult")
        .withColumn(
            "feedtypeLoadingDeviationG",
            F.col("loadedWeightFeedtypeG") - F.col("reqWeightFeedtypeG"),
        )
        # removing feedstuff that are 0 in the ration. They're not loaded
        .where(F.col("reqWeightFeedtypeG") > 0)
        .withColumn(
            "loadingAccuracyPercentage",
            F.least(F.col("reqWeightFeedtypeG"), F.col("loadedWeightFeedtypeG"))
            / F.greatest(F.col("reqWeightFeedtypeG"), F.col("loadedWeightFeedtypeG"))
            * 100,
        )
    )
    return loading_details


def feed_loading_daily_fact_transformer(
    silver_mfr_loading_activity: DataFrame,
    silver_kitchen_feed_names_dim: DataFrame,
) -> DataFrame:

    feed_exploded = _explode_mfr_loading_activity(silver_mfr_loading_activity)

    feed_window = Window.partitionBy("farm_license", "system_number", "feedId").orderBy(
        "date"
    )
    join_condition_dim = (
        (F.col("f.farm_license") == F.col("dim.farm_license"))
        & (F.col("f.system_number") == F.col("dim.system_number"))
        & (F.col("f.feedId") == F.col("dim.feedId"))
        & (F.col("f.date") > F.col("dim.startTime"))
        & (F.col("f.date") <= F.col("dim.endTime"))
    )

    feed_loading_daily_fact = (
        feed_exploded.withColumn("date", F.to_date(F.col("startTime"), "MM-dd-yyyy"))
        .groupBy("farm_license", "system_number", "date", "feedId")
        .agg(
            F.sum("reqWeightFeedtypeG").alias("totalRequestedWeightG"),
            F.sum("loadedWeightFeedtypeG").alias("totalLoadedWeightG"),
            F.avg("reqWeightFeedtypeG").alias("avgRequestedWeightG"),
            F.avg("loadedWeightFeedtypeG").alias("avgLoadedWeightG"),
            # if loading accuracy perc is <= 0 we set its accuracy to 0
            # it will be included in the calculation
            F.avg(
                F.when(F.col("loadingAccuracyPercentage") <= 0, 0).otherwise(
                    F.col("loadingAccuracyPercentage")
                )
            ).alias("loadingAccuracyPerc"),
        )
        .withColumn("totalRequestedWeightKg", F.col("totalRequestedWeightG") / 1000)
        .withColumn("totalLoadedWeightKg", F.col("totalLoadedWeightG") / 1000)
        .withColumn("avgRequestedWeightKg", F.col("avgRequestedWeightG") / 1000)
        .withColumn("avgLoadedWeightKg", F.col("avgLoadedWeightG") / 1000)
        .withColumn("nextDate", F.lead(F.col("date")).over(feed_window))
        .drop(
            "totalRequestedWeightG",
            "totalLoadedWeightG",
            "avgRequestedWeightG",
            "avgLoadedWeightG",
        )
    )

    add_missing_days_func = split_carryover_items_factory(
        "date", "nextDate", use_posexplode=True
    )

    with_kitchen_names = (
        feed_loading_daily_fact.alias("f")
        .join(
            silver_kitchen_feed_names_dim.alias("dim"),
            on=join_condition_dim,
            how="left",
        )
        .select("f.*", F.col("name").alias("feedName"))
    )

    with_missing_days = (
        add_missing_days_func(
            with_kitchen_names, "1 days", ["farm_license", "system_number", "feedId"]
        )
        .drop("nextDate", "startDate", "endDate", "pos")
        .withColumn("date", F.to_date("date"))
    )

    return with_missing_days


def ration_loading_daily_fact_transformer(
    silver_mfr_loading_activity: DataFrame,
    silver_ration_names_dim: DataFrame,
) -> DataFrame:

    feed_exploded = _explode_mfr_loading_activity(silver_mfr_loading_activity)

    ration_window = Window.partitionBy(
        "farm_license", "system_number", "rationId"
    ).orderBy("date")

    join_condition_dim = (
        (F.col("f.farm_license") == F.col("dim.farm_license"))
        & (F.col("f.system_number") == F.col("dim.system_number"))
        & (F.col("f.rationId") == F.col("dim.rationId"))
        & (F.col("f.date") > F.col("dim.startTime"))
        & (F.col("f.date") <= F.col("dim.endTime"))
    )

    ration_loading_daily_fact = (
        feed_exploded.withColumn("date", F.to_date(F.col("startTime"), "MM-dd-yyyy"))
        .groupBy("farm_license", "system_number", "date", "loadingUuid")
        # The table has a row per feed, but also contains ration general information
        # Hence, we aggregate (sum/avg) the feed specific info but take first of the
        # ration specific information (after all it will be the same for all rows).
        .agg(
            F.first("durationS").alias("durationS"),
            F.first("loadingSpeedRationGPerS").alias("loadingSpeedRationGPerS"),
            F.first("rationId").alias("rationId"),
            F.sum("reqWeightFeedtypeG").alias("reqWeightPerLoadGSummed"),
            F.sum("loadedWeightFeedtypeG").alias("loadedWeightPerLoadGSummed"),
            F.sum("feedtypeLoadingDeviationG").alias("feedtypeLoadingDeviationGSummed"),
            # if loading accuracy perc is <= 0 we set its accuracy to 0
            # it will be included in the calculation
            F.sum(
                F.when(F.col("loadingAccuracyPercentage") <= 0, 0).otherwise(
                    F.col("loadingAccuracyPercentage")
                )
            ).alias("loadingAccuracyPercentageSummed"),
            F.count("feedId").alias("nrOfFeedPerLoad"),
        )
        .groupBy("farm_license", "system_number", "date", "rationId")
        .agg(
            F.avg(F.col("durationS")).alias("avgDurationS"),
            F.avg("loadingSpeedRationGPerS").alias("avgLoadingSpeedGperS"),
            F.avg("reqWeightPerLoadGSummed").alias("avgRequestedWeightG"),
            F.avg("loadedWeightPerLoadGSummed").alias("avgLoadedWeightG"),
            F.avg("feedtypeLoadingDeviationGSummed").alias("avgLoadingDeviationG"),
            F.sum("loadingAccuracyPercentageSummed").alias("loadingAccuracyPercSummed"),
            F.sum("nrOfFeedPerLoad").alias("nrOfFeedPerRationPerDay"),
        )
        .withColumn(
            "loadingAccuracyPerc",
            F.round(
                F.col("loadingAccuracyPercSummed") / F.col("nrOfFeedPerRationPerDay"), 4
            ),
        )
        .drop(
            "loadingAccuracyPercSummed",
            "nrOfFeedPerRationPerDay",
        )
        .withColumn("nextDate", F.lead(F.col("date")).over(ration_window))
    )

    with_ration_names = (
        ration_loading_daily_fact.alias("f")
        .join(silver_ration_names_dim.alias("dim"), on=join_condition_dim, how="left")
        .select("f.*", F.col("name").alias("rationName"))
    )

    add_missing_days_func = split_carryover_items_factory(
        "date", "nextDate", use_posexplode=True
    )

    with_missing_days = (
        add_missing_days_func(
            with_ration_names, "1 days", ["farm_license", "system_number", "rationId"]
        )
        .drop("nextDate", "startDate", "endDate", "pos")
        .withColumn("date", F.to_date("date"))
    )

    return with_missing_days


def mfr_daily_fact_transformer(
    silver_mfr_loading_activity: DataFrame,
    silver_mfr_config_dim: DataFrame,
) -> DataFrame:
    MFR1_DEV_LDN = [71, 118]
    MFR2_DEV_LDN = [72, 119]

    mfr_window = Window.partitionBy("farm_license", "system_number").orderBy("date")

    join_condition_dim = (
        (F.col("f.farm_license") == F.col("dim.farm_license"))
        & (F.col("f.system_number") == F.col("dim.system_number"))
        & (F.col("f.date") > F.col("dim.startTime"))
        & (F.col("f.date") <= F.col("dim.endTime"))
    )

    feed_exploded = _explode_mfr_loading_activity(silver_mfr_loading_activity)

    # aggregating daily loading facts per vector system
    vector_daily = (
        feed_exploded.withColumn("date", F.to_date(F.col("startTime")))
        .groupBy("farm_license", "system_number", "date", "dev_number", "loadingUuid")
        .agg(
            F.sum("reqWeightFeedtypeG").alias("reqWeightPerLoadGSummed"),
            F.sum("loadedWeightFeedtypeG").alias("loadedWeightPerLoadGSummed"),
            # F.sum("feedtypeLoadingDeviationG").alias("deviationPerLoadGSummed"),
            # if loading accuracy perc is <= 0 we set its accuracy to 0
            # it will be included in the calculation
            F.sum(
                F.when(F.col("loadingAccuracyPercentage") <= 0, 0).otherwise(
                    F.col("loadingAccuracyPercentage")
                )
            ).alias("loadingAccuracyPercentageSummed"),
            F.count("feedId").alias("nrOfFeedPerLoad"),
        )
        .groupBy("farm_license", "system_number", "date")
        .agg(
            F.sum("reqWeightPerLoadGSummed").alias("totalRequestedWeightG"),
            F.sum("loadedWeightPerLoadGSummed").alias("totalLoadedWeightG"),
            F.sum("loadingAccuracyPercentageSummed").alias("loadingAccuracyPercSummed"),
            F.sum("nrOfFeedPerLoad").alias("nrOfFeedPerDay"),
        )
        .withColumn("totalRequestedWeightKg", F.col("totalRequestedWeightG") / 1000)
        .withColumn("totalLoadedWeightKg", F.col("totalLoadedWeightG") / 1000)
        .withColumn(
            "loadingAccuracyPerc",
            F.col("loadingAccuracyPercSummed") / F.col("nrOfFeedPerDay"),
        )
        .drop(
            "totalRequestedWeightG",
            "totalLoadedWeightG",
            "loadingAccuracyPercSummed",
            "nrOfFeedPerDay",
        )
        .withColumn("nextDate", F.lead(F.col("date")).over(mfr_window))
    )

    silver_mfr_config_unique_ss = create_dim_table(
        silver_mfr_config_dim,
        partition_columns=["farm_license", "system_number", "dev_number"],
        column_names=["freqControllerTypeMixer", "freqControllerTypeDosingRoller"],
        orderby_col="startTime",
    )

    mfr_config_count = (
        silver_mfr_config_unique_ss
        # Calculate number of Schneiders active within MFR
        .withColumn(
            "mfrNrSchneider",
            (F.col("freqControllerTypeMixer") == "SCHNEIDER").cast("int")
            + (F.col("freqControllerTypeDosingRoller") == "SCHNEIDER").cast("int"),
        )
        # Calculate number of COMMSK active within MFR
        .withColumn(
            "mfrNrCommsk",
            (F.col("freqControllerTypeMixer") == "COMMSK").cast("int")
            + (F.col("freqControllerTypeDosingRoller") == "COMMSK").cast("int"),
        )
    )

    mfr1_config_count = mfr_config_count.filter(F.col("dev_number").isin(MFR1_DEV_LDN))
    mfr2_config_count = mfr_config_count.filter(F.col("dev_number").isin(MFR2_DEV_LDN))

    vector_daily_fact = (
        vector_daily.alias("f")
        # add count for MFRs
        .join(mfr1_config_count.alias("dim"), on=join_condition_dim, how="left")
        .select(
            "f.*",
            F.col("mfrNrSchneider").alias("mfr1NrSchneider"),
            F.col("mfrNrCommsk").alias("mfr1NrCommsk"),
        )
        .alias("f")
        .join(mfr2_config_count.alias("dim"), on=join_condition_dim, how="left")
        .select(
            "f.*",
            F.col("mfrNrSchneider").alias("mfr2NrSchneider"),
            F.col("mfrNrCommsk").alias("mfr2NrCommsk"),
        )
        .alias("f")
        # sum all info we have for schneider
        .selectExpr(
            "*",
            """
            if(isnull(mfr1NrSchneider)
            and isnull(mfr2NrSchneider)
            , null,
            coalesce(mfr1NrSchneider, 0)
            + coalesce(mfr2NrSchneider, 0)
            )
            as nrSchneiderFreqControl
            """,
        )
        # sum all info we have for commsk
        .selectExpr(
            "*",
            """
            if(isnull(mfr1NrCommsk)
            and isnull(mfr2NrCommsk)
            , null,
            coalesce(mfr1NrCommsk, 0)
            + coalesce(mfr2NrCommsk, 0)
            )
            as nrCommskFreqControl
            """,
        )
        # remove the columns we no longer need
        .drop(
            "mfr1NrSchneider",
            "mfr1NrCommsk",
            "mfr2NrSchneider",
            "mfr2NrCommsk",
        )
    )

    add_missing_days_func = split_carryover_items_factory(
        "date", "nextDate", use_posexplode=True
    )

    with_missing_days = (
        add_missing_days_func(
            vector_daily_fact, "1 days", ["farm_license", "system_number"]
        )
        .drop("nextDate", "startDate", "endDate", "pos")
        .withColumn("date", F.to_date("date"))
    )

    return with_missing_days
