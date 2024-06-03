silver_mfr_loading_activity_schema = """
    farm_license STRING,
    system_number INTEGER,
    dev_number INTEGER,
    ration_id INTEGER,
    seq_nr INTEGER,
    year_month STRING,
    loading_uuid STRING NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    req_weight_ration_g INTEGER,
    weight_at_load_start_g INTEGER,
    loaded_weight_ration_g INTEGER,
    results ARRAY<STRUCT<
        feedId: INTEGER,
        reqWeight: INTEGER,
        weight: INTEGER,
        completed: BOOLEAN
    >>,
    duration_s LONG,
    loading_speed_ration_g_per_s DOUBLE
"""

silver_mfr_config_dim_schema = """
    farm_license STRING,
    system_number INTEGER,
    dev_number INTEGER,
    start_time TIMESTAMP,
    phases STRING,
    freq_controller_type_mixer STRING,
    freq_controller_type_dosing_roller STRING,
    relays_type STRING,
    year_month STRING,
    end_time TIMESTAMP,
    duration_s LONG,
    mfr_config_uuid STRING NOT NULL,
    mfr_type STRING NOT NULL
"""

silver_kitchen_feed_names_dim_schema = """
    farm_license STRING,
    system_number INT,
    start_time TIMESTAMP,
    feed_id INTEGER,
    name STRING,
    year_month STRING,
    end_time TIMESTAMP,
    duration_s LONG
"""

silver_ration_names_dim_schema = """
    farm_license STRING,
    system_number INT,
    start_time TIMESTAMP,
    ration_id INTEGER,
    name STRING,
    year_month STRING,
    end_time TIMESTAMP,
    duration_s LONG
"""
