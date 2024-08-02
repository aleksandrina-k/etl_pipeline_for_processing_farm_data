feed_daily_fact_schema = """
    farm_license STRING,
    feed_id INTEGER,
    date DATE NOT NULL,
    nr_times_loaded LONG,
    avg_loading_speed_kg_per_h DOUBLE,
    requested_weight_kg DOUBLE,
    loaded_weight_kg DOUBLE,
    avg_loading_deviation_kg DOUBLE,
    total_loading_deviation_kg DOUBLE,
    loading_accuracy_perc DOUBLE,
    feed_name STRING
"""

ration_daily_fact_schema = """
    farm_license STRING,
    ration_id INTEGER,
    date DATE NOT NULL,
    avg_loading_speed_kg_per_h DOUBLE,
    requested_weight_kg DOUBLE,
    loaded_weight_kg DOUBLE,
    avg_loading_deviation_kg DOUBLE,
    total_loading_deviation_kg DOUBLE,
    total_nr_of_feed_per_load LONG,
    total_nr_bins_loaded LONG,
    loading_accuracy_perc DOUBLE,
    ration_name STRING
"""

farm_daily_fact_schema = """
    farm_license STRING,
    date DATE NOT NULL,
    avg_loading_speed_kg_per_h DOUBLE,
    requested_weight_kg DOUBLE,
    loaded_weight_kg DOUBLE,
    avg_loading_deviation_kg DOUBLE,
    total_loading_deviation_kg DOUBLE,
    nr_of_loading_activities_per_day LONG,
    loading_accuracy_perc DOUBLE,
    nr_schneider_freq_control INTEGER,
    nr_commsk_freq_control INTEGER
"""
