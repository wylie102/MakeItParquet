#!/usr/bin/env python3
"""
Module that defines conversion tasks for Make-it-Parquet! using a direct factory function.
"""

import duckdb
from duckdb.typing import DuckDBPyConnection

TYPE_CHECKING = False
if TYPE_CHECKING:
    from pathlib import Path

# TODO: consider replacing entire thing with a function to generate the import statements.
# TODO: check whether Path object can be used in f strings/sql/duckdb.

# === Input Classes ===


class BaseInputConnection:
    """
    Base class for input connections to DuckDB.

    Provides the foundation for all input classes with a DuckDB connection
    and basic import functionality.

    Attributes:
        conn (duckdb.DuckDBPyConnection): DuckDB connection instance
    """

    def __init__(self, conn:DuckDBPyConnection):
        self.conn:DuckDBPyConnection = conn
        self.read_function: str = ""
        self.query: str = ""

    def _create_query(self, table_name: str, file_path: Path) -> str:
        return f"CREATE TABLE {table_name} AS FROM {self.read_function}('{file_path}')"

    def _create_import_statement(self) -> duckdb.DuckDBPyRelation:
        return duckdb.sql(f"{self.query}")

class CSVInput(BaseInputConnection):
    """
    Handles importing CSV files into DuckDB.

    Provides specialized functionality for reading and processing CSV format files
    using DuckDB's CSV reader.
    """
    def __init__(self, conn: DuckDBPyConnection, table_name: str, file_path: Path):
        super().__init__(conn)
        self.read_function: str = "read_csv"
        self.query: str = self._create_query(table_name, file_path)
        self.import_statement: duckdb.DuckDBPyRelation = self._create_import_statement()

class JSONInput(BaseInputConnection):
    """
    Handles importing JSON files into DuckDB.

    Provides specialized functionality for reading and processing JSON format files,
    handling nested structures and arrays.
    """
    def __init__(self, conn: DuckDBPyConnection, table_name: str, file_path: Path):
        super().__init__(conn)
        self.read_function: str = "read_json"
        self.query: str = self._create_query(table_name, file_path)
        self.import_statement: duckdb.DuckDBPyRelation = self._create_import_statement()



class ParquetInput(BaseInputConnection):
    """
    Handles importing Parquet files into DuckDB.

    Provides specialized functionality for reading Apache Parquet format files,
    preserving column types and handling compression.
    """
    def __init__(self, conn: DuckDBPyConnection, table_name: str, file_path: Path):
        super().__init__(conn)
        self.read_function: str = "read_parquet"
        self.query: str = self._create_query(table_name, file_path)
        self.import_statement: duckdb.DuckDBPyRelation = self._create_import_statement()



class TsvInput(CSVInput):
    """
    Handles importing TSV (Tab-Separated Values) files into DuckDB.

    Extends CSVInput to handle tab-delimited files specifically, using
    tab character as the default delimiter.
    """
    def __init__(self, conn: DuckDBPyConnection, table_name: str, file_path: Path):
        super().__init__(conn, table_name, file_path)



class TxtInput(CSVInput):
    """
    Handles importing generic text files into DuckDB.

    Extends CSVInput to handle text files with configurable delimiters,
    supporting both tab and comma-separated formats.
    """

    
    def __init__(self, conn: DuckDBPyConnection, table_name: str, file_path: Path):
        super().__init__(conn, table_name, file_path)



class ExcelInputUntyped(BaseInputConnection):
    """
    Handles importing Excel files into DuckDB with string typing.

    Reads Excel files treating all columns as strings (VARCHAR),
    suitable for text-based output formats.
    """

    
    def __init__(self, conn: DuckDBPyConnection, table_name: str, file_path: Path):
        super().__init__(conn)
        self.import_statement: duckdb.DuckDBPyRelation = duckdb.sql(f"CREATE TABLE {table_name} AS FROM read_csv('{file_path}')")



class ExcelInputTyped(ExcelInputUntyped):
    """
    Handles importing Excel files into DuckDB with type inference.

    Extends ExcelInputUntyped to infer and preserve column data types,
    suitable for Parquet and JSON output formats.
    """

    
    def __init__(self, conn: DuckDBPyConnection, table_name: str, file_path: Path):
        super().__init__(conn)
        self.import_statement: duckdb.DuckDBPyRelation = duckdb.sql(f"CREATE TABLE {table_name} AS FROM read_csv('{file_path}')")



# === Output Classes ===


class BaseOutputConnection:
    """
    Base class for output connections from DuckDB.

    Provides the foundation for all output classes with basic
    export functionality.
    """

    def __init__(self):
        pass


class ParquetOutput(BaseOutputConnection):
    """
    Handles exporting data to Parquet files.

    Provides functionality for writing data to Apache Parquet format,
    preserving column types and applying compression.
    """

    def __init__(self):
        super().__init__()
        self.export_statement = write_parquet(f"file_info_dict["path"]")


class JSONOutput(BaseOutputConnection):
    """
    Handles exporting data to JSON files.

    Provides functionality for writing data to JSON format,
    handling nested structures and arrays.
    """

    def __init__(self):
        super().__init__()
        self.export_statement = write_json(f"file_info_dict["path"]")


class CSVOutput(BaseOutputConnection):
    """
    Handles exporting data to CSV files.

    Provides functionality for writing data to comma-separated format,
    with options for custom delimiters and quoting.
    """

    def __init__(self):
        super().__init__()
        self.export_statement = write_csv(f"file_info_dict["path"]")


class TsvOutput(CSVOutput):
    """
    Handles exporting data to TSV files.

    Extends CSVOutput to write tab-separated files specifically,
    using tab character as the delimiter.
    """

    def __init__(self):
        super().__init__()
        self.export_statement = write_csv(f"file_info_dict["path"]", sep="\t")


class TxtOutput(CSVOutput):
    """
    Handles exporting data to text files.

    Extends CSVOutput to write delimited text files with configurable
    separators, supporting both tab and comma-separated formats.
    """

    def __init__(self):
        super().__init__()
        self.export_statement = write_csv(f"file_info_dict["path"]", sep=",")


class ExcelOutput(BaseOutputConnection):
    """
    Handles exporting data to Excel files.

    Provides functionality for writing data to Excel format (.xlsx),
    handling multiple sheets and basic formatting.
    """

    def __init__(self):
        super().__init__()
        pass # TODO check excel_utils.py for export statements.


# === Import class map  ===

import_class_map = {
    ".csv": CSVInput,
    ".tsv": TsvInput,
    ".txt": TxtInput,
    ".json": JSONInput,
    ".parquet": ParquetInput,
}
