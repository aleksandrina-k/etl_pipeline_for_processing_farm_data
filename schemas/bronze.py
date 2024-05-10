farm_info_schema_str = """
    farm_license STRING,
    system_number INTEGER,
    country STRING,
    city STRING,
    creation_timestamp TIMESTAMP
"""

bronze_table_str = f"""
    farm_license STRING,
    system_number INTEGER,
    dev_type STRING,
    dev_number INTEGER,
    msg_type STRING,
    time TIMESTAMP,
    data STRING,
    processing_time TIMESTAMP
"""
