from schemas.common import BASE_COLUMNS

mfr_config_schema = f"""
    {BASE_COLUMNS},
    phases STRING,
    freq_type_mixer STRING,
    freq_type_roller STRING,
    relays_type STRING
"""

mfr_load_done_schema = f"""
    {BASE_COLUMNS},
    ration_id INTEGER,
    total_weight INTEGER,
    feed_id INTEGER,
    req_weight INTEGER,
    weight INTEGER,
    completed BOOLEAN,
    seq_nr INTEGER
"""

mfr_load_done_result_schema = f"""
    {BASE_COLUMNS},
    ration_id INTEGER,
    total_weight INTEGER,
    results ARRAY<STRUCT<
        feed_id: INTEGER,
        req_weight: INTEGER,
        weight: INTEGER,
        completed: BOOLEAN
    >>,
    seq_nr INTEGER
"""

mfr_load_start_schema = f"""
    {BASE_COLUMNS},
    ration_id INTEGER,
    req_weight INTEGER,
    start_weight INTEGER,
    seq_nr INTEGER
"""

t4c_ration_names_schema = f"""
    {BASE_COLUMNS},
    ration_id INTEGER,
    name STRING
"""

t4c_kitchen_feed_names_schema = f"""
    {BASE_COLUMNS},
    feed_id INTEGER,
    name STRING
"""
