silver_mfr_loading_activity_schema = """
    farm_license STRING,
    system_number INTEGER,
    dev_number INTEGER,
    rationId INTEGER,
    seqNr INTEGER,
    year_month STRING,
    loadingUuid STRING NOT NULL,
    startTime TIMESTAMP,
    endTime TIMESTAMP,
    reqWeightRationG INTEGER,
    weightAtLoadStartG INTEGER,
    loadedWeightRationG INTEGER,
    results ARRAY<STRUCT<
        feedId: INTEGER,
        reqWeight: INTEGER,
        weight: INTEGER,
        completed: BOOLEAN
    >>,
    durationS LONG,
    loadingSpeedRationGPerS DOUBLE
"""

silver_mfr_config_dim_schema = """
    farm_license STRING,
    system_number INTEGER,
    dev_number INTEGER,
    startTime TIMESTAMP,
    phases STRING,
    freqControllerTypeMixer STRING,
    freqControllerTypeDosingRoller STRING,
    relaysType STRING,
    year_month STRING,
    endTime TIMESTAMP,
    durationS LONG,
    mfrConfigUuid STRING NOT NULL,
    mfr_type STRING NOT NULL
"""

silver_kitchen_feed_names_dim_schema = """
    farm_license STRING,
    system_number INT,
    startTime TIMESTAMP,
    feedId INTEGER,
    name STRING,
    year_month STRING,
    endTime TIMESTAMP,
    durationS LONG
"""

silver_ration_names_dim_schema = """
    farm_license STRING,
    system_number INT,
    startTime TIMESTAMP,
    rationId INTEGER,
    name STRING,
    year_month STRING,
    endTime TIMESTAMP,
    durationS LONG
"""