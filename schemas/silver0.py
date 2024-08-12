from schemas.common import BASE_COLUMNS

robot_config_schema = f"""
    {BASE_COLUMNS},
    phases STRING,
    freq_type_mixer STRING,
    freq_type_roller STRING,
    relays_type STRING
"""

load_done_result_schema = f"""
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

load_start_schema = f"""
    {BASE_COLUMNS},
    ration_id INTEGER,
    req_weight INTEGER,
    start_weight INTEGER,
    seq_nr INTEGER
"""

ration_names_schema = f"""
    {BASE_COLUMNS},
    ration_id INTEGER,
    name STRING
"""

kitchen_feed_names_schema = f"""
    {BASE_COLUMNS},
    feed_id INTEGER,
    name STRING
"""
