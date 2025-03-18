#!/usr/bin/env python3
"""
Module that defines conversion tasks for Make-it-Parquet! using a direct factory function.
"""

import re
from typing import NamedTuple
import uuid
from pathlib import Path


class _SQLReadStatementMapping(NamedTuple):
    csv: str = "read_csv"
    tsv: str = "read_csv"
    txt: str = "read_csv"
    json: str = "read_json"
    parquet: str = "read_parquet"
    xlsx: str = "read_xlsx"


class _DefaultArgumentMapping(NamedTuple):
    csv: str = ""
    tsv: str = ""
    txt: str = ""
    json: str = ""
    parquet: str = ""
    xlsx: str = ""


class ConversionData:
    # Store mapping dataclasses as Class Attributes.
    read_statement_mapping: _SQLReadStatementMapping = _SQLReadStatementMapping()
    default_argument_mapping: _DefaultArgumentMapping = _DefaultArgumentMapping()

    def __init__(self, input_ext: str, file_path: Path) -> None:
        """Initializes a ConversionData instance."""

        # Initialize input data.
        self.input_ext: str = input_ext
        self.file_path: Path = file_path

        # Normalize the extension: remove a leading dot.
        ext_key: str = re.sub(r"^\.", "", self.input_ext)

        # Get read_function and degault argument strings from dataclasses.
        self.read_function: str = getattr(
            ConversionData.read_statement_mapping, ext_key
        )
        self.default_arguments: str = getattr(
            ConversionData.default_argument_mapping, ext_key
        )

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
