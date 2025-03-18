#!/usr/bin/env python3
"""
Module that defines conversion tasks for Make-it-Parquet! using a direct factory function.
"""

import uuid

TYPE_CHECKING = False
if TYPE_CHECKING:
    from pathlib import Path


class ConversionData:
    # map extensions to duckdb read statements.
    extension_to_sql_read_statement_map: dict[str, str] = {
        ".csv": "read_csv",
        ".tsv": "read_csv",
        ".txt": "read_csv",
        ".json": "read_json",
        ".parquet": "read_parquet",
        ".xlsx": "read_xlsx",
    }

    # map extensions to default arguments
    # default arguments must start with ', '
    extension_to_default_arguments_map: dict[str, str] = {
        ".csv": "",
        ".tsv": "",
        ".txt": "",
        ".json": "",
        ".parquet": "",
        ".xlsx": "",
    }

    def __init__(self, input_ext: str, file_path: Path) -> None:
        """Initializes ConversionData dataclass."""

        # Initialize input data.
        self.input_ext: str = input_ext
        self.file_path: Path = file_path
        self.read_function: str = ConversionData.extension_to_sql_read_statement_map[
            self.input_ext
        ]
        self.default_arguments: str = ConversionData.extension_to_default_arguments_map[
            self.input_ext
        ]

        # Create table name.
        self.table_name: str = self._create_unique_table_name()

        # Create import statement.
        self.import_query: str = self._generate_import_query()

        # Initialize output data.
        self.output_ext: str | None = None
        self.output_path: Path | None = None

    # function to generate import statements
    def _generate_import_query(self) -> str:
        return f"CREATE TABLE {self.table_name} AS FROM {self.read_function}('{self.file_path}'{self.default_arguments});"

    def _create_unique_table_name(self) -> str:
        name: str = self.file_path.name
        return f"{name}_{uuid.uuid4().hex[:8]}"
