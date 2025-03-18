#!/usr/bin/env python3
"""
Module that contains dataclasses related to the conversion process in MakeItParquet!
"""

from dataclasses import dataclass
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
    tsv: str = r", delim = \t"
    txt: str = r", delim = \t"
    json: str = ""
    parquet: str = ""
    xlsx: str = ""


class ConversionInputAttributes(NamedTuple):
    input_ext: str
    file_path: Path
    read_function: str
    default_arguments: str
    table_name: str
    import_query: str


@dataclass
class ConversionOutputAttributes:
    output_ext: str | None
    output_directory_name: str | None
    output_file_name: str | None
    output_path: Path | None


class ConversionData:
    # Store mapping dataclasses as Class Attributes.
    read_statement_mapping: _SQLReadStatementMapping = _SQLReadStatementMapping()
    default_argument_mapping: _DefaultArgumentMapping = _DefaultArgumentMapping()

    def __init__(self, input_ext: str, file_path: Path) -> None:
        """Initializes a ConversionData instance."""

        # Normalize the extension: remove a leading dot.
        ext_key: str = re.sub(r"^\.", "", input_ext)
        read_function: str = getattr(ConversionData.read_statement_mapping, ext_key)
        default_arguments: str = getattr(
            ConversionData.default_argument_mapping, ext_key
        )
        table_name = self._create_unique_table_name(file_path)
        import_query = self.generate_import_query(table_name, file_path)

        self.input_attributes: ConversionInputAttributes = ConversionInputAttributes(
            input_ext=input_ext,
            file_path=file_path,
            read_function=read_function,
            default_arguments=default_arguments,
            table_name=table_name,
            import_query=import_query,
        )

        # Initialize output data.
        self.output_ext: str | None = None
        self.output_path: Path | None = None

    def _create_unique_table_name(self, file_path: Path) -> str:
        name: str = file_path.name
        return f"{name}_{uuid.uuid4().hex[:8]}"

    def generate_import_query(self, table_name: str, file_path: Path) -> str:
        return f"EXECUTE import_statement(table_name := {table_name}, file_path := {file_path});"

    def generate_prepared_import_statement(self) -> str:
        return f"PREPARE import_statement AS CREATE TABLE $table_name AS FROM {
            self.input_attributes.read_function
        }('$file_path'{self.input_attributes.default_arguments});"
