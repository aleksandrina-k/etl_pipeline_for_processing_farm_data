class ConfigParamMissingException(Exception):
    def __init__(self, param_name: str):
        self.param_name = param_name
        super().__init__(f"Missing required config parameter: {param_name}")


class SchemaDoesNotMatchExpectedException(Exception):
    def __init__(self, result_table: str, expected_schema, actual_schema):
        self.result_table = result_table
        self.expected_schema = expected_schema
        self.actual_schema = actual_schema
        super().__init__(
            f"Results from transformation for {result_table} table"
            f" does NOT match expected schema. "
            f"Expected schema: {expected_schema}. "
            f"Actual schema: {actual_schema}"
        )
