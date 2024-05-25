feed_daily_fact_schema = """
    farm_license STRING,
    system_number INTEGER,
    date DATE NOT NULL,
    feedId INTEGER,
    loadingAccuracyPerc DOUBLE,
    totalRequestedWeightKg DOUBLE,
    totalLoadedWeightKg DOUBLE,
    avgRequestedWeightKg DOUBLE,
    avgLoadedWeightKg DOUBLE,
    feedName STRING
"""

ration_loading_daily_fact_schema = """
    farm_license STRING,
    system_number INTEGER,
    date DATE NOT NULL,
    rationId INTEGER,
    avgDurationS DOUBLE,
    avgLoadingSpeedGperS DOUBLE,
    avgRequestedWeightG DOUBLE,
    avgLoadedWeightG DOUBLE,
    avgLoadingDeviationG DOUBLE,
    loadingAccuracyPerc DOUBLE,
    rationName STRING
"""

mfr_daily_fact_schema = """
    farm_license STRING,
    system_number INTEGER,
    date DATE NOT NULL,
    totalRequestedWeightKg DOUBLE,
    totalLoadedWeightKg DOUBLE,
    loadingAccuracyPerc DOUBLE,
    nrSchneiderFreqControl INTEGER,
    nrCommskFreqControl INTEGER
"""
