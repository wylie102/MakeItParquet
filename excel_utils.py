#!/usr/bin/env python3
"""
Module for Excel-specific conversion functions for DuckConvert.

This module provides the ExcelUtils class which contains helper methods
to build Excel options clauses, build read queries, export Excel files with
inferred column types, perform paginated Excel exports, and load the Excel extension.
"""

import duckdb
import uuid
import tempfile
import os
from pathlib import Path
import logging
import argparse
from typing import List

from cli_interface import FILE_TYPE_ALIASES, get_file_type_by_extension


class ExcelUtils:
    @staticmethod
    def build_excel_options(sheet, range_):
        parts = []
        if sheet is not None:
            sheet_val = f"Sheet{sheet}" if isinstance(sheet, int) else sheet
            parts.append(f"sheet = '{sheet_val}'")
        if range_ is not None:
            parts.append(f"range = '{range_}'")
        return f", {', '.join(parts)}" if parts else ""

    @staticmethod
    def build_excel_query(file: Path, sheet, range_) -> str:
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
                logging.error(f"Error during Excel export: {e}")
                raise
            finally:
                if table_name is not None:
                    try:
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    except Exception as drop_error:
                        logging.warning(
                            f"Failed to drop temporary table {table_name}: {drop_error}"
                        )
                try:
                    if os.path.exists(sample_csv):
                        os.remove(sample_csv)
                except Exception as remove_error:
                    logging.warning(
                        f"Failed to remove temporary file {sample_csv}: {remove_error}"
                    )
        except Exception as e:
            logging.error(f"Error during Excel export: {e}")
            raise

    @staticmethod
    def export_excel(
        conn: duckdb.DuckDBPyConnection,
        base_query: str,
        out_file: Path,
        row_limit: int = 1048576,
        margin: int = 100,
    ) -> None:
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
        logging.info(
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
                    f"Part {part+1} exceeds effective limit ({part_count} > {effective_limit})"
                )
            if part_count == 0:
                break

            logging.info(
                f"Exporting part {part+1} with {part_count} rows (offset {offset})."
            )
            out_file_part = (
                out_file.parent / f"{out_file.stem}_{part+1}{out_file.suffix}"
            )
            unique_out_file = out_file_part
            counter = 1
            while unique_out_file.exists():
                unique_out_file = (
                    out_file.parent
                    / f"{out_file.stem}_{part+1}_{counter}{out_file.suffix}"
                )
                counter += 1
            conn.sql(
                f"COPY ({part_query}) TO '{unique_out_file.resolve()}' WITH (FORMAT 'xlsx', HEADER)"
            )

    @staticmethod
    def load_extension(conn: duckdb.DuckDBPyConnection) -> bool:
        try:
            conn.install_extension("excel")
            conn.load_extension("excel")
            return True
        except Exception as e:
            logging.error(f"Failed to install/load excel extension: {e}")
            return False

    @staticmethod
    def excel_check_and_load(
        conn: duckdb.DuckDBPyConnection,
        out_type: str,
        args: argparse.Namespace,
        files_to_process: List[Path],
    ) -> bool:
        """
        Check if the Excel extension is required and load it if necessary.
        """
        requires_excel = (
            (out_type == "excel")
            or (
                args.input_type
                and FILE_TYPE_ALIASES[args.input_type.lower()] == "excel"
            )
            or any(get_file_type_by_extension(f) == "excel" for f in files_to_process)
        )
        if requires_excel:
            if not ExcelUtils.load_extension(conn):
                return False
        try:
            conn.install_extension("excel")
            conn.load_extension("excel")
            return True
        except Exception as e:
            logging.error(f"Failed to install/load excel extension: {e}")
            return False
