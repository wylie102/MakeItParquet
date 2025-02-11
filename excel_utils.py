#!/usr/bin/env python3
"""
Module for Excel-specific conversion functions for DuckConvert.

This module contains helper functions to build Excel options clauses and to export
Excel files with various conversion logic.
"""


import duckdb
import uuid
import tempfile
import os
from pathlib import Path
import logging


def _build_excel_options(sheet, range_):
    """
    Build the optional clause for an Excel query based on sheet and range.
    For an integer sheet number, it uses 'Sheet<number>'; otherwise, it uses the provided sheet name.
    """
    parts = []
    if sheet is not None:
        parts.append(
            f"sheet = '{'Sheet' + str(sheet) if isinstance(sheet, int) else sheet}'"
        )
    if range_ is not None:
        parts.append(f"range = '{range_}'")
    return f", {', '.join(parts)}" if parts else ""


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
    Export an Excel file to JSON or Parquet by inferring column types from a temporary CSV sample.

    Steps:
      1. Use read_xlsx (with all_varchar = 'true' and a LIMIT of 1000) to export a sample CSV.
      2. Create a temporary table by inferring the correct schema via read_csv_auto.
      3. Insert full Excel data into the temporary table using a COPY command (using native Excel input).
      4. Export the temporary table using a COPY command to the requested format.
      5. Clean up the temporary table and the temporary CSV sample.
    """
    excel_options = _build_excel_options(sheet, range_)
    file_path = str(file.resolve())
    out_file_path = str(out_file.resolve())

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        sample_csv = tmp.name

    sample_query = f"SELECT * FROM read_xlsx('{file_path}', all_varchar = 'true'{excel_options} ) LIMIT 1000"
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
            raise ValueError("Unsupported format in export_excel_with_inferred_types")
        conn.execute(final_copy_cmd)
    finally:
        try:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass
        os.remove(sample_csv)


def export_excel(
    conn: duckdb.DuckDBPyConnection,
    base_query: str,
    out_file: Path,
    row_limit: int = 1048576,
    margin: int = 100,  # Breathing room for header row etc.
) -> None:
    """
    Export query results to an Excel file using a COPY command.
    If the row count is within the effective limit (row_limit - margin), a single export is performed;
    otherwise, the export is split into multiple files.
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

        # Verify the row count for this batch.
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
