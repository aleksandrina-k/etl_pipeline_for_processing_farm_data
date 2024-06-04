farm_info_schema_str = """
    farm_license STRING,
    country STRING,
    city STRING,
    creation_timestamp TIMESTAMP
"""

bronze_table_str = """
    farm_license STRING,
    device_type STRING,
    device_number INTEGER,
    msg_type STRING,
    time TIMESTAMP,
    data STRING,
    processing_time TIMESTAMP
"""
