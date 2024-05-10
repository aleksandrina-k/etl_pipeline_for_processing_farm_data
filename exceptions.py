from typing import Set, List


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


class DuplicatesTablesException(Exception):
    def __init__(self, tables: Set[str]):
        self.tables = tables
        super().__init__(f"There are duplicate tables! Duplicates: {tables}")


class NotAllInputTablesAreDeclaredException(Exception):
    def __init__(self, tables: Set[str]):
        self.tables = tables
        super().__init__(
            f"Not all input tables are declared. Not declared tables: {tables}"
        )


class UnknownInputTableException(Exception):
    def __init__(self, input_table: str, result_table: str):
        self.input_table = input_table
        self.result_table = result_table
        super().__init__(
            f"Unknown input table {input_table} for result table {result_table}"
        )


class CyclicDependenciesException(Exception):
    def __init__(self, cycles: List[tuple]):
        self.cycles = cycles
        super().__init__(f"Cyclic dependencies found! Cycles: {cycles}")
