#!/usr/bin/env python3
"""
Module for Excel-specific conversion functions for DuckConvert.

This module provides helper functions to build Excel options clauses and
to export Excel files with conversion logic that infers column types when needed.
"""

import duckdb
import uuid
import tempfile
import os
from pathlib import Path
import logging


def _build_excel_options(sheet, range_):
    """
    Build Excel options clause for read_xlsx queries.
    If sheet and/or range are provided, returns a clause such as:
      ", sheet = 'Sheet1', range = 'A1:B2'"
    """
    parts = []
    if sheet is not None:
        sheet_val = f"Sheet{sheet}" if isinstance(sheet, int) else sheet
        parts.append(f"sheet = '{sheet_val}'")
    if range_ is not None:
        parts.append(f"range = '{range_}'")
    return f", {', '.join(parts)}" if parts else ""


def _build_excel_query(file: Path, sheet, range_) -> str:
    # Build a query string for reading Excel files
    file_path = str(file.resolve())
    options = _build_excel_options(sheet, range_)
    return f"SELECT * FROM read_xlsx('{file_path}', all_varchar = 'true'{options})"


def export_excel_with_inferred_types(
    conn: duckdb.DuckDBPyConnection,
    file: Path,
    out_file: Path,
    sheet=None,
    range_=None,
    fmt: str = "json",
    **kwargs,
) -> None:
    """
    Export an Excel file to JSON or Parquet by inferring column types.

    Steps:
      1. Create a temporary CSV sample using read_xlsx.
      2. Create a temporary table with inferred schema using read_csv_auto.
      3. COPY data from the original Excel file into the temporary table.
      4. Export the table to the requested format.
      5. Clean up temporary artifacts.
    """
    excel_options = _build_excel_options(sheet, range_)
    file_path = str(file.resolve())
    out_file_path = str(out_file.resolve())

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        sample_csv = tmp.name

    sample_query = _build_excel_query(file, sheet, range_)
    table_name = None  # initialize here for later cleanup
    try:
        conn.sql(sample_query).write_csv(sample_csv, header=True)
        table_name = f"tmp_conv_{uuid.uuid4().hex}"
        try:
            create_table_query = f"""
            CREATE TEMPORARY TABLE {table_name} AS 
            SELECT * FROM read_csv_auto('{sample_csv}') LIMIT 0
            """
            conn.execute(create_table_query)

            copy_cmd = f"COPY {table_name} FROM '{file_path}' WITH (FORMAT 'xlsx', HEADER{excel_options})"
            conn.execute(copy_cmd)

            if fmt == "json":
                final_copy_cmd = (
                    f"COPY {table_name} TO '{out_file_path}' WITH (FORMAT 'json')"
                )
            elif fmt == "parquet":
                final_copy_cmd = (
                    f"COPY {table_name} TO '{out_file_path}' WITH (FORMAT 'parquet')"
                )
            else:
                raise ValueError(
                    "Unsupported format in export_excel_with_inferred_types"
                )
            conn.execute(final_copy_cmd)
        except Exception as e:
            logging.error(f"Error during Excel export: {e}")
            raise
        finally:
            if table_name is not None:
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                except Exception as drop_error:
                    logging.warning(f"Failed to drop temporary table {table_name}: {drop_error}")
            try:
                if os.path.exists(sample_csv):
                    os.remove(sample_csv)
            except Exception as remove_error:
                logging.warning(f"Failed to remove temporary file {sample_csv}: {remove_error}")
    except Exception as e:
        logging.error(f"Error during Excel export: {e}")
        raise


def export_excel(
    conn: duckdb.DuckDBPyConnection,
    base_query: str,
    out_file: Path,
    row_limit: int = 1048576,
    margin: int = 100,
) -> None:
    """
    Export query results to an Excel file using a COPY command.
    If the row count is within the effective limit, perform a single export;
    otherwise, split the export into multiple parts.
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

        out_file_part = out_file.parent / f"{out_file.stem}_{part+1}{out_file.suffix}"
        unique_out_file = out_file_part
        counter = 1
        while unique_out_file.exists():
            unique_out_file = (
                out_file.parent / f"{out_file.stem}_{part+1}_{counter}{out_file.suffix}"
            )
            counter += 1

        conn.sql(
            f"COPY ({part_query}) TO '{unique_out_file.resolve()}' WITH (FORMAT 'xlsx', HEADER)"
        )
