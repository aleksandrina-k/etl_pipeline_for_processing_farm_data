feed_daily_fact_schema = """
    farm_license STRING,
    system_number INTEGER,
    date DATE NOT NULL,
    feed_id INTEGER,
    loading_accuracy_perc DOUBLE,
    total_requested_weight_kg DOUBLE,
    total_loaded_weight_kg DOUBLE,
    avg_requested_weight_kg DOUBLE,
    avg_loaded_weight_kg DOUBLE,
    feed_name STRING
"""

ration_daily_fact_schema = """
    farm_license STRING,
    system_number INTEGER,
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
    system_number INTEGER,
    date DATE NOT NULL,
    total_requested_weight_kg DOUBLE,
    total_loaded_weight_kg DOUBLE,
    loading_accuracy_perc DOUBLE,
    nr_schneider_freq_control INTEGER,
    nr_commsk_freq_control INTEGER
"""
