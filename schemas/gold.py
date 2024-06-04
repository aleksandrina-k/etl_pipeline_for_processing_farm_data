feed_daily_fact_schema = """
    farm_license STRING,
    feed_id INTEGER,
    date DATE NOT NULL,
    nr_times_loaded LONG,
    requested_weight_kg DOUBLE,
    loaded_weight_kg DOUBLE,
    avg_loading_deviation_kg DOUBLE,
    loading_accuracy_perc DOUBLE,
    feed_name STRING
"""

ration_daily_fact_schema = """
    farm_license STRING,
    ration_id INTEGER,
    date DATE NOT NULL,
    total_loading_speed_kg_per_h DOUBLE,
    total_requested_weight_kg DOUBLE,
    total_loaded_weight_kg DOUBLE,
    avg_nr_of_feed_in_ration DOUBLE,
    total_nr_of_feed_per_load LONG,
    total_nr_bins_loaded LONG,
    loading_accuracy_perc DOUBLE,
    ration_name STRING
"""

mfr_daily_fact_schema = """
    farm_license STRING,
    date DATE NOT NULL,
    total_requested_weight_kg DOUBLE,
    total_loaded_weight_kg DOUBLE,
    loading_accuracy_perc DOUBLE,
    nr_schneider_freq_control INTEGER,
    nr_commsk_freq_control INTEGER
"""
