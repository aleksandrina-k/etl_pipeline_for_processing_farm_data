from pyspark.sql import DataFrame, functions as F, Window
from operations.helper_functions import create_dim_table, split_carryover_items_factory

SECONDS_IN_HOUR = 3600
GRAMS_IN_KILOGRAM = 1000


def _explode_loading_activity(
    silver_loading_activity: DataFrame,
) -> DataFrame:
    loading_details = (
        silver_loading_activity.select(
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
    silver_loading_activity: DataFrame,
    silver_kitchen_feed_names_dim: DataFrame,
) -> DataFrame:

    feed_exploded = _explode_loading_activity(silver_loading_activity)
    feed_window = Window.partitionBy("farm_license", "feed_id").orderBy("date")

    add_missing_days_func = split_carryover_items_factory(
        "date", "next_date", use_posexplode=True
    )

    columns_with_daily_values = [
        "nr_times_loaded",
        "avg_loading_speed_kg_per_h",
        "requested_weight_kg",
        "loaded_weight_kg",
        "avg_loading_deviation_kg",
        "total_loading_deviation_kg",
        "loading_accuracy_perc",
    ]

    join_condition_dim = (
        (F.col("fact.farm_license") == F.col("dim.farm_license"))
        & (F.col("fact.date") >= F.col("dim.start_time"))
        & (F.col("fact.date") < F.col("dim.end_time"))
    )

    feed_daily_fact = (
        feed_exploded.withColumn("date", F.to_date(F.col("start_time")))
        .where(F.col("loading_speed_ration_g_per_s") > 0)
        .where(F.col("duration_s") > 1)
        .groupBy("farm_license", "date", "feed_id")
        .agg(
            F.count("farm_license").alias("nr_times_loaded"),
            (F.avg("loading_speed_ration_g_per_s") / 3.6).alias(
                "avg_loading_speed_kg_per_h"
            ),
            F.sum(F.col("req_weight_feedtype_g") / GRAMS_IN_KILOGRAM).alias(
                "requested_weight_kg"
            ),
            F.sum(F.col("loaded_weight_feedtype_g") / GRAMS_IN_KILOGRAM).alias(
                "loaded_weight_kg"
            ),
            F.sum(F.col("feedtype_loading_deviation_g") / GRAMS_IN_KILOGRAM).alias(
                "total_loading_deviation_kg"
            ),
            F.avg(F.col("feedtype_loading_deviation_g") / GRAMS_IN_KILOGRAM).alias(
                "avg_loading_deviation_kg"
            ),
            # if loading accuracy perc is <= 0 we set its accuracy to 0
            # it will be included in the calculation
            F.avg(
                F.when(
                    F.col("feedtype_completed"),
                    F.when(F.col("loading_accuracy_percentage") <= 0, 0).otherwise(
                        F.col("loading_accuracy_percentage")
                    ),
                )
            ).alias("loading_accuracy_perc"),
        )
        # .where((F.col("requested_weight_kg") / F.col("nr_times_loaded")) < 1500)
        # .where(F.col("avg_loading_deviation_kg") < 11000)
    )

    with_kitchen_names = (
        feed_daily_fact.alias("fact")
        .join(
            silver_kitchen_feed_names_dim.alias("dim"),
            on=join_condition_dim,
            how="left",
        )
        .select("fact.*", F.col("name").alias("feed_name"))
    )

    with_missing_days = (
        add_missing_days_func(
            with_kitchen_names.withColumn(
                "next_date", F.lead(F.col("date")).over(feed_window)
            ),
            "1 days",
            ["farm_license", "feed_id"],
        )
        .select(
            "farm_license",
            "feed_id",
            "date",
            *[
                F.when(F.col("pos") > 0, F.lit(None))
                .otherwise(F.round(F.col(c), 2))
                .alias(c)
                for c in columns_with_daily_values
            ],
            "feed_name",
        )
        .withColumn("date", F.to_date(F.col("date")))
        .withColumnRenamed("start_date", "date")
    )

    return with_missing_days


def ration_daily_fact_transformer(
    silver_loading_activity: DataFrame,
    silver_ration_names_dim: DataFrame,
) -> DataFrame:

    feed_exploded = _explode_loading_activity(silver_loading_activity)

    ration_window_date = Window.partitionBy("farm_license", "ration_id").orderBy("date")

    join_condition_dim = (
        (F.col("r.farm_license") == F.col("dim.farm_license"))
        & (F.col("r.ration_id") == F.col("dim.ration_id"))
        & (F.col("r.date") > F.col("dim.start_time"))
        & (F.col("r.date") <= F.col("dim.end_time"))
    )

    columns_with_daily_values = [
        "avg_loading_speed_kg_per_h",
        "requested_weight_kg",
        "loaded_weight_kg",
        "avg_loading_deviation_kg",
        "total_loading_deviation_kg",
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
        .groupBy("farm_license", "date", "loading_uuid")
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
            F.avg(F.col("feedtype_loading_deviation_g") / GRAMS_IN_KILOGRAM).alias(
                "avg_loading_deviation_kg"
            ),
            F.sum(F.col("feedtype_loading_deviation_g") / GRAMS_IN_KILOGRAM).alias(
                "sum_loading_deviation_kg"
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
        .groupBy("farm_license", "date", "ration_id")
        .agg(
            F.avg("loading_speed_ration_kg_per_h").alias("avg_loading_speed_kg_per_h"),
            F.sum("total_requested_weight_per_load_kg").alias("requested_weight_kg"),
            F.sum("total_loaded_weight_per_load_kg").alias("loaded_weight_kg"),
            F.avg("avg_loading_deviation_kg").alias("avg_loading_deviation_kg"),
            F.sum("sum_loading_deviation_kg").alias("total_loading_deviation_kg"),
            F.sum("loading_accuracy_perc").alias("loading_accuracy_perc"),
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
        ["farm_license", "ration_id"],
    ).select(
        "farm_license",
        "ration_id",
        F.col("date").cast("date"),
        *[
            F.when(F.col("pos") > 0, F.lit(None))
            .otherwise(F.round(F.col(c), 2))
            .alias(c)
            for c in columns_with_daily_values
        ],
        "ration_name",
    )

    return with_missing_days


def farm_daily_fact_transformer(
    silver_loading_activity: DataFrame,
    silver_robot_config_dim: DataFrame,
) -> DataFrame:
    farm_window = Window.partitionBy("farm_license").orderBy("date")

    join_condition_dim = (
        (F.col("f.farm_license") == F.col("dim.farm_license"))
        & (F.col("f.date") > F.col("dim.start_time"))
        & (F.col("f.date") <= F.col("dim.end_time"))
    )

    columns_with_daily_values = [
        "avg_loading_speed_kg_per_h",
        "requested_weight_kg",
        "loaded_weight_kg",
        "avg_loading_deviation_kg",
        "total_loading_deviation_kg",
        "nr_of_loading_activities_per_day",
        "loading_accuracy_perc",
        "nr_schneider_freq_control",
        "nr_commsk_freq_control",
    ]

    feed_exploded = _explode_loading_activity(silver_loading_activity)

    # aggregating daily loading facts per vector system
    farm_daily = (
        feed_exploded.withColumn("date", F.to_date(F.col("start_time")))
        .groupBy("farm_license", "date")
        .agg(
            (F.avg("loading_speed_ration_g_per_s") / 3.6).alias(
                "avg_loading_speed_kg_per_h"
            ),
            F.sum(F.col("req_weight_feedtype_g") / GRAMS_IN_KILOGRAM).alias(
                "requested_weight_kg"
            ),
            F.sum(F.col("loaded_weight_feedtype_g") / GRAMS_IN_KILOGRAM).alias(
                "loaded_weight_kg"
            ),
            F.avg(F.col("feedtype_loading_deviation_g") / GRAMS_IN_KILOGRAM).alias(
                "avg_loading_deviation_kg"
            ),
            F.avg(F.col("feedtype_loading_deviation_g") / GRAMS_IN_KILOGRAM).alias(
                "total_loading_deviation_kg"
            ),
            # if loading accuracy perc is <= 0 we set its accuracy to 0
            # it will be included in the calculation
            F.sum(
                F.when(F.col("loading_accuracy_percentage") <= 0, 0).otherwise(
                    F.col("loading_accuracy_percentage")
                )
            ).alias("loading_accuracy_perc_summed"),
            F.count("feed_id").alias("nr_of_feed_per_day"),
            F.count("loading_uuid").alias("nr_of_loading_activities_per_day"),
        )
        .withColumn(
            "loading_accuracy_perc",
            F.col("loading_accuracy_perc_summed") / F.col("nr_of_feed_per_day"),
        )
        .drop(
            "loading_accuracy_perc_summed",
            "nr_of_feed_per_day",
        )
    )

    robot_config_unique_ss = create_dim_table(
        silver_robot_config_dim,
        partition_columns=["farm_license", "device_number"],
        column_names=[
            "freq_controller_type_mixer",
            "freq_controller_type_dosing_roller",
        ],
        orderby_col="start_time",
    )

    robot_config_count = (
        robot_config_unique_ss
        # Calculate number of Schneiders active within the robot
        .withColumn(
            "nr_schneider",
            (F.col("freq_controller_type_mixer") == "SCHNEIDER").cast("int")
            + (F.col("freq_controller_type_dosing_roller") == "SCHNEIDER").cast("int"),
        )
        # Calculate number of COMMSK active within the robor
        .withColumn(
            "nr_commsk",
            (F.col("freq_controller_type_mixer") == "COMMSK").cast("int")
            + (F.col("freq_controller_type_dosing_roller") == "COMMSK").cast("int"),
        )
    )

    farm_daily_fact = (
        farm_daily.alias("f")
        .join(robot_config_count.alias("dim"), on=join_condition_dim, how="left")
        .select(
            "f.*",
            F.col("nr_schneider").alias("nr_schneider_freq_control"),
            F.col("nr_commsk").alias("nr_commsk_freq_control"),
        )
    )

    add_missing_days_func = split_carryover_items_factory(
        "date", "next_date", use_posexplode=True
    )

    with_missing_days = add_missing_days_func(
        farm_daily_fact.withColumn(
            "next_date", F.lead(F.col("date")).over(farm_window)
        ),
        "1 days",
        ["farm_license"],
    ).select(
        "farm_license",
        F.col("date").cast("date"),
        *[
            F.when(F.col("pos") > 0, F.lit(None))
            .otherwise(F.round(F.col(c), 2))
            .alias(c)
            for c in columns_with_daily_values
        ],
    )

    return with_missing_days
