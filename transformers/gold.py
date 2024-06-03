from pyspark.sql import DataFrame, functions as F, Window

from operations.helper_functions import create_dim_table, split_carryover_items_factory

MFR1_DEV_LDN = [71, 118]
MFR2_DEV_LDN = [72, 119]
SECONDS_IN_HOUR = 3600
GRAMS_IN_KILOGRAM = 1000


def _explode_mfr_loading_activity(
    silver_mfr_loading_activity: DataFrame,
) -> DataFrame:
    loading_details = (
        silver_mfr_loading_activity.select(
            # By exploding results we get a row for every feed_id in the ration
            "*",
            F.explode("results").alias("load_result"),
        )
        # Next, we need to unnest the feed specific information so that the info becomes 'queryable'
        .withColumn("feed_id", F.col("load_result.feedId"))
        .withColumn("req_weight_feedtype_g", F.col("load_result.reqWeight"))
        .withColumn("loaded_weight_feedtype_g", F.col("load_result.weight"))
        .withColumn("feedtype_completed", F.col("load_result.completed"))
        .drop("load_result")
        .withColumn(
            "feedtype_loading_deviation_g",
            F.col("loaded_weight_feedtype_g") - F.col("req_weight_feedtype_g"),
        )
        # removing feedstuff that are 0 in the ration. They're not loaded
        .where(F.col("req_weight_feedtype_g") > 0)
        .withColumn(
            "loading_accuracy_percentage",
            F.least(F.col("req_weight_feedtype_g"), F.col("loaded_weight_feedtype_g"))
            / F.greatest(
                F.col("req_weight_feedtype_g"), F.col("loaded_weight_feedtype_g")
            )
            * 100,
        )
    )
    return loading_details


def feed_daily_fact_transformer(
    silver_mfr_loading_activity: DataFrame,
    silver_kitchen_feed_names_dim: DataFrame,
) -> DataFrame:

    feed_exploded = _explode_mfr_loading_activity(silver_mfr_loading_activity)

    feed_window = Window.partitionBy(
        "farm_license", "system_number", "feed_id"
    ).orderBy("date")
    join_condition_dim = (
        (F.col("f.farm_license") == F.col("dim.farm_license"))
        & (F.col("f.system_number") == F.col("dim.system_number"))
        & (F.col("f.feed_id") == F.col("dim.feed_id"))
        & (F.col("f.date") > F.col("dim.start_time"))
        & (F.col("f.date") <= F.col("dim.end_time"))
    )

    feed_daily_fact = (
        feed_exploded.withColumn("date", F.to_date(F.col("start_time"), "MM-dd-yyyy"))
        .groupBy("farm_license", "system_number", "date", "feed_id")
        .agg(
            F.sum("req_weight_feedtype_g").alias("total_requested_weight_g"),
            F.sum("loaded_weight_feedtype_g").alias("total_loaded_weight_g"),
            F.avg("req_weight_feedtype_g").alias("avg_requested_weight_g"),
            F.avg("loaded_weight_feedtype_g").alias("avg_loaded_weight_g"),
            # if loading accuracy perc is <= 0 we set its accuracy to 0
            # it will be included in the calculation
            F.avg(
                F.when(F.col("loading_accuracy_percentage") <= 0, 0).otherwise(
                    F.col("loading_accuracy_percentage")
                )
            ).alias("loading_accuracy_perc"),
        )
        .withColumn(
            "total_requested_weight_kg",
            F.col("total_requested_weight_g") / GRAMS_IN_KILOGRAM,
        )
        .withColumn(
            "total_loaded_weight_kg", F.col("total_loaded_weight_g") / GRAMS_IN_KILOGRAM
        )
        .withColumn(
            "avg_requested_weight_kg",
            F.col("avg_requested_weight_g") / GRAMS_IN_KILOGRAM,
        )
        .withColumn(
            "avg_loaded_weight_kg", F.col("avg_loaded_weight_g") / GRAMS_IN_KILOGRAM
        )
        .withColumn("next_date", F.lead(F.col("date")).over(feed_window))
        .drop(
            "total_requested_weight_g",
            "total_loaded_weight_g",
            "avg_requested_weight_g",
            "avg_loaded_weight_g",
        )
    )

    add_missing_days_func = split_carryover_items_factory(
        "date", "next_date", use_posexplode=True
    )

    with_kitchen_names = (
        feed_daily_fact.alias("f")
        .join(
            silver_kitchen_feed_names_dim.alias("dim"),
            on=join_condition_dim,
            how="left",
        )
        .select("f.*", F.col("name").alias("feed_name"))
    )

    with_missing_days = (
        add_missing_days_func(
            with_kitchen_names, "1 days", ["farm_license", "system_number", "feed_id"]
        )
        .drop("next_date", "start_date", "end_date", "pos")
        .withColumn("date", F.to_date("date"))
    )

    return with_missing_days


def ration_daily_fact_transformer(
    silver_mfr_loading_activity: DataFrame,
    silver_ration_names_dim: DataFrame,
) -> DataFrame:

    feed_exploded = _explode_mfr_loading_activity(silver_mfr_loading_activity)

    ration_window_date = Window.partitionBy(
        "farm_license", "system_number", "ration_id"
    ).orderBy("date")

    join_condition_dim = (
        (F.col("r.farm_license") == F.col("dim.farm_license"))
        & (F.col("r.system_number") == F.col("dim.system_number"))
        & (F.col("r.ration_id") == F.col("dim.ration_id"))
        & (F.col("r.date") > F.col("dim.start_time"))
        & (F.col("r.date") <= F.col("dim.end_time"))
    )

    columns_with_daily_values = [
        "total_loading_speed_kg_per_h",
        "total_requested_weight_kg",
        "total_loaded_weight_kg",
        "avg_nr_of_feed_in_ration",
        "total_nr_of_feed_per_load",
        "total_nr_bins_loaded",
        "loading_accuracy_perc",
    ]

    ration_daily_fact = (
        feed_exploded.withColumn("date", F.to_date(F.col("start_time"), "MM-dd-yyyy"))
        # if duration is <= 1 there is a problem and the record should be excluded from the calculations
        .where(F.col("duration_s") > 1)
        # if loaded weight is <= 0 or >= 1200000 there is a problem
        # and the record should be excluded from the calculations
        .where(
            (F.col("loaded_weight_ration_g") > 0)
            & (F.col("loaded_weight_ration_g") < 1200000)
        )
        .groupBy("farm_license", "system_number", "date", "loading_uuid")
        # The table has a row per feed, but also contains ration general information
        # Hence, we aggregate (sum/avg) the feed specific info but take first of the
        # ration specific information (after all it will be the same for all rows).
        .agg(
            F.first(F.col("duration_s") / SECONDS_IN_HOUR).alias("duration_h"),
            (F.first("loading_speed_ration_g_per_s") / 3.6).alias(
                "loading_speed_ration_kg_per_h"
            ),
            F.first("ration_id").alias("ration_id"),
            (F.sum("req_weight_feedtype_g") / GRAMS_IN_KILOGRAM).alias(
                "total_requested_weight_per_load_kg"
            ),
            F.sum((F.col("loaded_weight_feedtype_g") / GRAMS_IN_KILOGRAM)).alias(
                "total_loaded_weight_per_load_kg"
            ),
            # if loading accuracy perc is <= 0 we set its accuracy to 0
            # it will be included in the calculation
            F.sum(
                F.when(F.col("loading_accuracy_percentage") <= 0, 0).otherwise(
                    F.col("loading_accuracy_percentage")
                )
            ).alias("loading_accuracy_perc"),
            F.count("feed_id").alias("nr_of_feed_per_load"),
            F.countDistinct("loading_uuid").alias("bins_loaded"),
        )
        .groupBy("farm_license", "system_number", "date", "ration_id")
        .agg(
            F.sum("loading_speed_ration_kg_per_h").alias(
                "total_loading_speed_kg_per_h"
            ),
            F.sum("total_requested_weight_per_load_kg").alias(
                "total_requested_weight_kg"
            ),
            F.sum("total_loaded_weight_per_load_kg").alias("total_loaded_weight_kg"),
            F.sum("loading_accuracy_perc").alias("loading_accuracy_perc"),
            F.avg("nr_of_feed_per_load").alias("avg_nr_of_feed_in_ration"),
            F.sum("nr_of_feed_per_load").alias("total_nr_of_feed_per_load"),
            F.sum("bins_loaded").alias("total_nr_bins_loaded"),
        )
        .withColumn(
            "loading_accuracy_perc",
            F.round(
                F.col("loading_accuracy_perc") / F.col("total_nr_of_feed_per_load"), 2
            ),
        )
    )

    add_missing_days_func = split_carryover_items_factory(
        "date", "next_date", use_posexplode=True
    )

    with_kitchen_names = (
        ration_daily_fact.alias("r")
        .join(
            silver_ration_names_dim.alias("dim"),
            on=join_condition_dim,
            how="left",
        )
        .select("r.*", F.col("name").alias("ration_name"))
    )

    with_missing_days = add_missing_days_func(
        with_kitchen_names.withColumn(
            "next_date", F.lead(F.col("date")).over(ration_window_date)
        ),
        "1 days",
        ["farm_license", "system_number", "ration_id"],
    ).select(
        "farm_license",
        "system_number",
        "ration_id",
        F.col("date").cast("date"),
        *[
            F.when(F.col("pos") > 0, F.lit(None)).otherwise(F.col(c)).alias(c)
            for c in columns_with_daily_values
        ],
        "ration_name"
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
        & (F.col("f.date") > F.col("dim.start_time"))
        & (F.col("f.date") <= F.col("dim.end_time"))
    )

    feed_exploded = _explode_mfr_loading_activity(silver_mfr_loading_activity)

    # aggregating daily loading facts per vector system
    vector_daily = (
        feed_exploded.withColumn("date", F.to_date(F.col("start_time")))
        .groupBy("farm_license", "system_number", "date", "dev_number", "loading_uuid")
        .agg(
            F.sum("req_weight_feedtype_g").alias("req_weight_per_load_g_summed"),
            F.sum("loaded_weight_feedtype_g").alias("loaded_weight_per_load_g_summed"),
            # if loading accuracy perc is <= 0 we set its accuracy to 0
            # it will be included in the calculation
            F.sum(
                F.when(F.col("loading_accuracy_percentage") <= 0, 0).otherwise(
                    F.col("loading_accuracy_percentage")
                )
            ).alias("loading_accuracy_percentage_summed"),
            F.count("feed_id").alias("nr_of_feed_per_load"),
        )
        .groupBy("farm_license", "system_number", "date")
        .agg(
            F.sum("req_weight_per_load_g_summed").alias("total_requested_weight_g"),
            F.sum("loaded_weight_per_load_g_summed").alias("total_loaded_weight_g"),
            F.sum("loading_accuracy_percentage_summed").alias(
                "loading_accuracy_perc_summed"
            ),
            F.sum("nr_of_feed_per_load").alias("nr_of_feed_per_day"),
        )
        .withColumn(
            "total_requested_weight_kg",
            F.col("total_requested_weight_g") / GRAMS_IN_KILOGRAM,
        )
        .withColumn(
            "total_loaded_weight_kg", F.col("total_loaded_weight_g") / GRAMS_IN_KILOGRAM
        )
        .withColumn(
            "loading_accuracy_perc",
            F.col("loading_accuracy_perc_summed") / F.col("nr_of_feed_per_day"),
        )
        .drop(
            "total_requested_weight_g",
            "total_loaded_weight_g",
            "loading_accuracy_perc_summed",
            "nr_of_feed_per_day",
        )
        .withColumn("next_date", F.lead(F.col("date")).over(mfr_window))
    )

    silver_mfr_config_unique_ss = create_dim_table(
        silver_mfr_config_dim,
        partition_columns=["farm_license", "system_number", "dev_number"],
        column_names=[
            "freq_controller_type_mixer",
            "freq_controller_type_dosing_roller",
        ],
        orderby_col="start_time",
    )

    mfr_config_count = (
        silver_mfr_config_unique_ss
        # Calculate number of Schneiders active within MFR
        .withColumn(
            "mfr_nr_schneider",
            (F.col("freq_controller_type_mixer") == "SCHNEIDER").cast("int")
            + (F.col("freq_controller_type_dosing_roller") == "SCHNEIDER").cast("int"),
        )
        # Calculate number of COMMSK active within MFR
        .withColumn(
            "mfr_nr_commsk",
            (F.col("freq_controller_type_mixer") == "COMMSK").cast("int")
            + (F.col("freq_controller_type_dosing_roller") == "COMMSK").cast("int"),
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
            F.col("mfr_nr_schneider").alias("mfr_1_nr_schneider"),
            F.col("mfr_nr_commsk").alias("mfr_1_nr_commsk"),
        )
        .alias("f")
        .join(mfr2_config_count.alias("dim"), on=join_condition_dim, how="left")
        .select(
            "f.*",
            F.col("mfr_nr_schneider").alias("mfr_2_nr_schneider"),
            F.col("mfr_nr_commsk").alias("mfr_2_nr_commsk"),
        )
        .alias("f")
        # sum all info we have for schneider
        .selectExpr(
            "*",
            """
            if(isnull(mfr_1_nr_schneider)
            and isnull(mfr_2_nr_schneider)
            , null,
            coalesce(mfr_1_nr_schneider, 0)
            + coalesce(mfr_2_nr_schneider, 0)
            )
            as nr_schneider_freq_control
            """,
        )
        # sum all info we have for commsk
        .selectExpr(
            "*",
            """
            if(isnull(mfr_1_nr_commsk)
            and isnull(mfr_2_nr_commsk)
            , null,
            coalesce(mfr_1_nr_commsk, 0)
            + coalesce(mfr_2_nr_commsk, 0)
            )
            as nr_commsk_freq_control
            """,
        )
        # remove the columns we no longer need
        .drop(
            "mfr_1_nr_schneider",
            "mfr_1_nr_commsk",
            "mfr_2_nr_schneider",
            "mfr_2_nr_commsk",
        )
    )

    add_missing_days_func = split_carryover_items_factory(
        "date", "next_date", use_posexplode=True
    )

    with_missing_days = (
        add_missing_days_func(
            vector_daily_fact, "1 days", ["farm_license", "system_number"]
        )
        .drop("next_date", "start_date", "end_date", "pos")
        .withColumn("date", F.to_date("date"))
    )

    return with_missing_days
