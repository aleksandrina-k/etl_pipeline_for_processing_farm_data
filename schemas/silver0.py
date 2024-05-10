from schemas.common import BASE_COLUMNS

mfr_config_schema = f"""
    {BASE_COLUMNS},
    phases STRING,
    freqTypeMixer STRING,
    freqTypeRoller STRING,
    relaysType STRING
"""

mfr_load_done_schema = f"""
    {BASE_COLUMNS},
    rationId INTEGER,
    totalWeight INTEGER,
    feedId INTEGER,
    reqWeight INTEGER,
    weight INTEGER,
    completed BOOLEAN,
    seqNr INTEGER
"""

mfr_load_done_result_schema = f"""
    {BASE_COLUMNS},
    rationId INTEGER,
    totalWeight INTEGER,
    results ARRAY<STRUCT<
        feedId: INTEGER,
        reqWeight: INTEGER,
        weight: INTEGER,
        completed: BOOLEAN
    >>,
    seqNr INTEGER
"""

mfr_load_start_schema = f"""
    {BASE_COLUMNS},
    rationId INTEGER,
    reqWeight INTEGER,
    startWeight INTEGER,
    seqNr INTEGER
"""

t4c_ration_names_schema = f"""
    {BASE_COLUMNS},
    rationId INTEGER,
    name STRING
"""

t4c_kitchen_feed_names_schema = f"""
    {BASE_COLUMNS},
    feedId INTEGER,
    name STRING
"""
