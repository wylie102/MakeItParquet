#!/usr/bin/env python3
"""
Module for Excel-specific conversion functions for Make-it-Parquet!.

This module provides the ExcelUtils class which contains helper methods
to build Excel options clauses, build read queries, export Excel files with
inferred column types, perform paginated Excel exports, and load the Excel extension.
"""

import duckdb
import uuid
import tempfile
import os
from pathlib import Path
from user_interface import logger
import argparse
from typing import List

from user_interface.settings import Settings


class ExcelUtils:
    @staticmethod
    def build_excel_options(sheet, range_):
        """
        Build an Excel options clause for the read query.

        Constructs an options clause based on the provided sheet and range. If the sheet is
        an integer, it is converted to the format "Sheet{number}".

        Args:
            sheet (Union[int, str, None]): The Excel sheet (number or name).
            range_ (str or None): Cell range (e.g. "A1:C10").

        Returns:
            str: Options clause to be appended to the read query, or an empty string if no options.
        """
        parts = []
        if sheet is not None:
            sheet_val = f"Sheet{sheet}" if isinstance(sheet, int) else sheet
            parts.append(f"sheet = '{sheet_val}'")
        if range_ is not None:
            parts.append(f"range = '{range_}'")
        return f", {', '.join(parts)}" if parts else ""

    @staticmethod
    def build_excel_query(file: Path, sheet, range_) -> str:
        """
        Build a SQL query to read an Excel file.

        Resolves the file path and builds the options clause before generating a query that
        reads the file with all columns as varchar.

        Args:
            file (Path): Path object pointing to the Excel file.
            sheet (Union[int, str, None]): Excel sheet number or name.
            range_ (str or None): Excel range string.

        Returns:
            str: SQL query for reading the Excel file.
        """
        file_path = str(file.resolve())
        options = ExcelUtils.build_excel_options(sheet, range_)
        return f"SELECT * FROM read_xlsx('{file_path}', all_varchar = 'true'{options})"

    @staticmethod
    def export_with_inferred_types(
        conn: duckdb.DuckDBPyConnection,
        file: Path,
        out_file: Path,
        sheet=None,
        range_=None,
        fmt: str = "json",
        **kwargs,
    ) -> None:
        """
        Export an Excel file to another format using inferred column types.

        Writes a temporary CSV sample to infer table schema, then creates a temporary table,
        and finally exports the data in the desired format (JSON or Parquet).

        Args:
            conn (duckdb.DuckDBPyConnection): DuckDB connection instance.
            file (Path): Excel file to be converted.
            out_file (Path): Destination output file path.
            sheet (Union[int, str, None]): Excel sheet specifier.
            range_ (str or None): Cell range to read.
            fmt (str): Output format; either "json" or "parquet".
            **kwargs: Additional keyword arguments.

        Raises:
            ValueError: If an unsupported output format is specified.
            Exception: Propagates any exceptions encountered during export.
        """
        excel_options = ExcelUtils.build_excel_options(sheet, range_)
        file_path = str(file.resolve())
        out_file_path = str(out_file.resolve())

        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            sample_csv = tmp.name

        sample_query = ExcelUtils.build_excel_query(file, sheet, range_)
        table_name = None
        try:
            conn.sql(sample_query).write_csv(sample_csv, header=True)
            table_name = f"tmp_conv_{uuid.uuid4().hex}"
            try:
                create_table_query = f"""
                    CREATE TEMPORARY TABLE {table_name} AS 
                    SELECT * FROM read_csv_auto('{sample_csv}') LIMIT 0
                    """
                conn.execute(create_table_query)

                copy_cmd = (
                    f"COPY {table_name} FROM '{file_path}' "
                    f"WITH (FORMAT 'xlsx', HEADER{excel_options})"
                )
                conn.execute(copy_cmd)

                if fmt == "json":
                    final_copy_cmd = (
                        f"COPY {table_name} TO '{out_file_path}' WITH (FORMAT 'json')"
                    )
                elif fmt == "parquet":
                    final_copy_cmd = f"COPY {table_name} TO '{out_file_path}' WITH (FORMAT 'parquet')"
                else:
                    raise ValueError("Unsupported format in export_with_inferred_types")
                conn.execute(final_copy_cmd)
            except Exception as e:
                logger.error(f"Error during Excel export: {e}")
                raise
            finally:
                if table_name is not None:
                    try:
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    except Exception as drop_error:
                        logger.warning(
                            f"Failed to drop temporary table {table_name}: {drop_error}"
                        )
                try:
                    if os.path.exists(sample_csv):
                        os.remove(sample_csv)
                except Exception as remove_error:
                    logger.warning(
                        f"Failed to remove temporary file {sample_csv}: {remove_error}"
                    )
        except Exception as e:
            logger.error(f"Error during Excel export: {e}")
            raise

    @staticmethod
    def export_excel(
        conn: duckdb.DuckDBPyConnection,
        base_query: str,
        out_file: Path,
        row_limit: int = 1048576,
        margin: int = 100,
    ) -> None:
        """
        Export query results to an Excel file, splitting the export if necessary.

        Executes a query and, if the number of rows exceeds the effective limit (row_limit minus margin),
        splits the export into multiple parts. Each part is exported as a separate Excel file.

        Args:
            conn (duckdb.DuckDBPyConnection): DuckDB connection instance.
            base_query (str): SQL query to retrieve data for export.
            out_file (Path): Destination output file path.
            row_limit (int, optional): Maximum allowed rows (default is 1,048,576).
            margin (int, optional): Margin subtracted from row_limit (default is 100).

        Raises:
            ValueError: If no result is returned from the count query or if any part exceeds the effective limit.
        """
        out_file_path = str(out_file.resolve())
        count_query = f"SELECT COUNT(*) FROM ({base_query}) AS t"
        result = conn.execute(count_query).fetchone()
        if result is None:
            raise ValueError("No result returned from count query")
        row_count = result[0]
        effective_limit = row_limit - margin

        if row_count <= effective_limit:
            conn.sql(
                f"COPY ({base_query}) TO '{out_file_path}' WITH (FORMAT 'xlsx', HEADER)"
            )
            return

        parts = (row_count + effective_limit - 1) // effective_limit
        logger.info(
            f"Excel export: {row_count} rows exceed the effective limit of {effective_limit}. "
            f"Splitting into {parts} parts labelled as '{out_file.stem}_1' to '{out_file.stem}_{parts}'."
        )

        for part in range(parts):
            offset = part * effective_limit
            part_query = f"{base_query} LIMIT {effective_limit} OFFSET {offset}"

            verify_query = f"SELECT COUNT(*) FROM ({part_query}) AS t"
            result = conn.execute(verify_query).fetchone()
            if result is None:
                raise ValueError("No result returned from count query")
            part_count = result[0]
            if part_count > effective_limit:
                raise ValueError(
                    f"Part {part + 1} exceeds effective limit ({part_count} > {effective_limit})"
                )
            if part_count == 0:
                break

            logger.info(
                f"Exporting part {part + 1} with {part_count} rows (offset {offset})."
            )
            out_file_part = (
                out_file.parent / f"{out_file.stem}_{part + 1}{out_file.suffix}"
            )
            unique_out_file = out_file_part
            counter = 1
            while unique_out_file.exists():
                unique_out_file = (
                    out_file.parent
                    / f"{out_file.stem}_{part + 1}_{counter}{out_file.suffix}"
                )
                counter += 1
            conn.sql(
                f"COPY ({part_query}) TO '{unique_out_file.resolve()}' WITH (FORMAT 'xlsx', HEADER)"
            )

    @staticmethod
    def load_extension(conn: duckdb.DuckDBPyConnection) -> bool:
        """
        Load the Excel extension into the DuckDB connection.

        Attempts to install and load the "excel" extension.

        Args:
            conn (duckdb.DuckDBPyConnection): DuckDB connection instance.

        Returns:
            bool: True if the extension was successfully loaded, False otherwise.
        """
        try:
            conn.install_extension("excel")
            conn.load_extension("excel")
            return True
        except Exception as e:
            logger.error(f"Failed to install/load excel extension: {e}")
            return False

    @staticmethod
    def excel_check_and_load(
        conn: duckdb.DuckDBPyConnection,
        out_type: str,
        args: argparse.Namespace,
        files_to_process: List[Path],
    ) -> bool:
        """
        Check and load the Excel extension if required.

        Determines if the Excel extension is needed based on the output type, input arguments,
        or the file types present in the list. If required, attempts to load the extension.

        Args:
            conn (duckdb.DuckDBPyConnection): DuckDB connection instance.
            out_type (str): Desired output format.
            args (argparse.Namespace): CLI arguments.
            files_to_process (List[Path]): List of file paths to process.

        Returns:
            bool: True if the Excel extension is successfully loaded or not needed, False otherwise.
        """
        requires_excel = (
            (out_type == "excel")
            or (
                args.input_type
                and Settings.ALIAS_TO_EXTENSION_MAP[args.input_type.lower()] == "excel"
            )
            or any(
                Settings.ALIAS_TO_EXTENSION_MAP[f.suffix.lower()] == "excel"
                for f in files_to_process
            )
        )
        if requires_excel:
            if not ExcelUtils.load_extension(conn):
                return False
        try:
            conn.install_extension("excel")
            conn.load_extension("excel")
            return True
        except Exception as e:
            logger.error(f"Failed to install/load excel extension: {e}")
            return False
