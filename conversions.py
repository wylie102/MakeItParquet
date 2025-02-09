#!/usr/bin/env python3
"""
Module that defines conversion functions for DuckConvert.

This module provides a dictionary mapping (input_type, output_type) pairs
to lambda functions that perform file conversions using DuckDB's Python API.
It also leverages the Excel extension when needed.
"""

from typing import Callable, Any, Dict, Tuple
from pathlib import Path
import duckdb


def export_excel(
    conn: duckdb.DuckDBPyConnection,
    base_query: str,
    out_file: Path,
    row_limit: int = 1048576,
    margin: int = 100,  # Extra breathing room for header row etc.
) -> None:
    """
    Export query results to an Excel file.
    Instead of performing a full export first, the function counts the rows.
    If the row count is within the effective limit (row_limit - margin), a single export is performed.
    Otherwise, the export is split into multiple files, each with up to (row_limit - margin) data rows.
    """
    # Fetch the total row count.
    count_query = f"SELECT COUNT(*) FROM ({base_query}) AS t"
    result = conn.execute(count_query).fetchone()
    if result is None:
        raise ValueError("No result returned from count query")
    row_count = result[0]

    # Use a reduced limit to account for the header and any potential extra rows.
    effective_limit = row_limit - margin

    # Single export if within limit.
    if row_count <= effective_limit:
        conn.sql(
            f"COPY ({base_query}) TO '{out_file.resolve()}' WITH (FORMAT 'xlsx', header 'true')"
        )
        return

    # Paginate the export if row_count exceeds the effective limit.
    parts = (row_count + effective_limit - 1) // effective_limit
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

        # Generate a unique file name for this partition.
        out_file_part = out_file.parent / f"{out_file.stem}_{part+1}{out_file.suffix}"
        unique_out_file = out_file_part
        counter = 1
        while unique_out_file.exists():
            unique_out_file = (
                out_file.parent / f"{out_file.stem}_{part+1}_{counter}{out_file.suffix}"
            )
            counter += 1

        conn.sql(
            f"COPY ({part_query}) TO '{unique_out_file.resolve()}' WITH (FORMAT 'xlsx', header 'true')"
        )


def export_txt_generic(
    conn: duckdb.DuckDBPyConnection,
    read_func: Callable[[str], Any],
    file: Path,
    out_file: Path,
    **kwargs,
) -> None:
    """
    Generic exporter for TXT files.
    Prompts the user (if not provided via kwargs) to choose whether to use
    tab or comma as the delimiter, then writes using the same CSV functions.
    """
    delimiter = kwargs.get("delimiter")
    if delimiter is None:
        answer = (
            input(
                "For TXT export, choose T for tab separated or C for comma separated: "
            )
            .strip()
            .lower()
        )
        delimiter = "\t" if answer == "t" else ","
    read_func(str(file.resolve())).write_csv(
        str(out_file.resolve()), delimiter=delimiter
    )


def export_tsv_generic(
    conn: duckdb.DuckDBPyConnection,
    read_func: Callable[[str], Any],
    file: Path,
    out_file: Path,
    **kwargs,
) -> None:
    """
    Generic exporter for TSV files.
    Uses tab as the delimiter.
    """
    read_func(str(file.resolve())).write_csv(str(out_file.resolve()), delimiter="\t")


# Conversion lookup dictionary mapping (input_type, output_type) to conversion lambdas.
# The type annotation tells the linter each value is a callable.
CONVERSION_FUNCTIONS: Dict[Tuple[str, str], Callable[..., Any]] = {
    # CSV conversions
    ("csv", "parquet"): lambda conn, file, out_file, **kwargs: conn.read_csv(
        str(file.resolve())
    ).write_parquet(str(out_file.resolve())),
    ("csv", "json"): lambda conn, file, out_file, **kwargs: conn.read_csv(
        str(file.resolve())
    ).sql(
        f"COPY (SELECT * FROM read_csv_auto('{file.resolve()}')) TO '{out_file.resolve()}'"
    ),
    ("csv", "excel"): lambda conn, file, out_file, **kwargs: export_excel(
        conn, f"SELECT * FROM read_csv_auto('{file.resolve()}')", out_file
    ),
    ("csv", "tsv"): lambda conn, file, out_file, **kwargs: export_tsv_generic(
        conn, conn.read_csv, file, out_file, **kwargs
    ),
    ("csv", "txt"): lambda conn, file, out_file, **kwargs: export_txt_generic(
        conn, conn.read_csv, file, out_file, **kwargs
    ),
    # JSON conversions
    ("json", "csv"): lambda conn, file, out_file, **kwargs: conn.read_json(
        str(file.resolve())
    ).write_csv(str(out_file.resolve())),
    ("json", "parquet"): lambda conn, file, out_file, **kwargs: conn.read_json(
        str(file.resolve())
    ).write_parquet(str(out_file.resolve())),
    ("json", "excel"): lambda conn, file, out_file, **kwargs: export_excel(
        conn, f"SELECT * FROM read_json('{file.resolve()}')", out_file
    ),
    ("json", "tsv"): lambda conn, file, out_file, **kwargs: export_tsv_generic(
        conn, conn.read_json, file, out_file, **kwargs
    ),
    ("json", "txt"): lambda conn, file, out_file, **kwargs: export_txt_generic(
        conn, conn.read_json, file, out_file, **kwargs
    ),
    # Parquet conversions
    ("parquet", "csv"): lambda conn, file, out_file, **kwargs: conn.from_parquet(
        str(file.resolve())
    ).write_csv(str(out_file.resolve())),
    ("parquet", "json"): lambda conn, file, out_file, **kwargs: conn.from_parquet(
        str(file.resolve())
    ).sql(
        f"COPY (SELECT * FROM read_parquet('{file.resolve()}')) TO '{out_file.resolve()}'"
    ),
    ("parquet", "excel"): lambda conn, file, out_file, **kwargs: export_excel(
        conn, f"SELECT * FROM read_parquet('{file.resolve()}')", out_file
    ),
    ("parquet", "tsv"): lambda conn, file, out_file, **kwargs: export_tsv_generic(
        conn, conn.from_parquet, file, out_file, **kwargs
    ),
    ("parquet", "txt"): lambda conn, file, out_file, **kwargs: export_txt_generic(
        conn, conn.from_parquet, file, out_file, **kwargs
    ),
    # Excel conversions (sheet and range parameters are optional)
    (
        "excel",
        "csv",
    ): lambda conn, file, out_file, sheet=None, range_=None, **kwargs: conn.sql(
        "SELECT * FROM read_xlsx('{}'{}{})".format(
            str(file.resolve()),
            (
                ", sheet = '{}'".format(
                    f"Sheet{sheet}" if isinstance(sheet, int) else sheet
                )
                if sheet is not None
                else ""
            ),
            (", range = '{}'".format(range_) if range_ is not None else ""),
        )
    ).write_csv(
        str(out_file.resolve())
    ),
    (
        "excel",
        "parquet",
    ): lambda conn, file, out_file, sheet=None, range_=None, **kwargs: conn.sql(
        "SELECT * FROM read_xlsx('{}'{}{})".format(
            str(file.resolve()),
            (
                ", sheet = '{}'".format(
                    f"Sheet{sheet}" if isinstance(sheet, int) else sheet
                )
                if sheet is not None
                else ""
            ),
            (", range = '{}'".format(range_) if range_ is not None else ""),
        )
    ).write_parquet(
        str(out_file.resolve())
    ),
    ("excel", "json"): lambda conn, file, out_file, sheet=None, range_=None, **kwargs: (
        lambda rel: conn.execute(f"COPY ({rel.sql_query()}) TO '{out_file.resolve()}'")
    )(
        conn.sql(
            "SELECT * FROM read_xlsx('{}'{}{})".format(
                str(file.resolve()),
                (
                    ", sheet = '{}'".format(
                        f"Sheet{sheet}" if isinstance(sheet, int) else sheet
                    )
                    if sheet is not None
                    else ""
                ),
                (", range = '{}'".format(range_) if range_ is not None else ""),
            )
        )
    ),
    (
        "excel",
        "tsv",
    ): lambda conn, file, out_file, sheet=None, range_=None, **kwargs: conn.sql(
        "SELECT * FROM read_xlsx('{}'{}{})".format(
            str(file.resolve()),
            (
                ", sheet = '{}'".format(
                    f"Sheet{sheet}" if isinstance(sheet, int) else sheet
                )
                if sheet is not None
                else ""
            ),
            (", range = '{}'".format(range_) if range_ is not None else ""),
        )
    ).write_csv(
        str(out_file.resolve()), delimiter="\t"
    ),
    ("excel", "txt"): lambda conn, file, out_file, sheet=None, range_=None, **kwargs: (
        (
            lambda dt: conn.sql(
                "SELECT * FROM read_xlsx('{}'{}{})".format(
                    str(file.resolve()),
                    (
                        ", sheet = '{}'".format(
                            f"Sheet{sheet}" if isinstance(sheet, int) else sheet
                        )
                        if sheet is not None
                        else ""
                    ),
                    (", range = '{}'".format(range_) if range_ is not None else ""),
                )
            ).write_csv(str(out_file.resolve()), delimiter=dt)
        )(
            input(
                "For TXT export, choose T for tab separated or C for comma separated: "
            )
            .strip()
            .lower()
            == "t"
            and "\t"
            or ","
        )
    ),
}
