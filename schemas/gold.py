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

ration_daily_fact_schema = """
    farm_license STRING,
    system_number INTEGER,
    rationId INTEGER,
    date DATE NOT NULL,
    totalLoadingSpeedKgPerH DOUBLE,
    totalRequestedWeightKg DOUBLE,
    totalLoadedWeightKg DOUBLE,
    avgNrOfFeedInRation DOUBLE,
    totalNrOfFeedPerLoad LONG,
    totalNrBinsLoaded LONG,
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
