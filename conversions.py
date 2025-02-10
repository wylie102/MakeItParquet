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
import excel_utils as ex


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
                "For TXT export, choose t for tab separated or c for comma separated: "
            )
            .strip()
            .lower()
        )
        delimiter = "\t" if answer == "t" else ","
    read_func(str(file.resolve())).write_csv(str(out_file.resolve()), sep=delimiter)


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
    read_func(str(file.resolve())).write_csv(str(out_file.resolve()), sep="\t")


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
    ("csv", "excel"): lambda conn, file, out_file, **kwargs: ex.export_excel(
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
    ("json", "excel"): lambda conn, file, out_file, **kwargs: ex.export_excel(
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
    ("parquet", "excel"): lambda conn, file, out_file, **kwargs: ex.export_excel(
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
        f"SELECT * FROM read_xlsx('{str(file.resolve())}', all_varchar = 'true'{ex._build_excel_options(sheet, range_)})"
    ).write_csv(
        str(out_file.resolve())
    ),
    (
        "excel",
        "parquet",
    ): lambda conn, file, out_file, sheet=None, range_=None, **kwargs: ex.export_excel_with_inferred_types(
        conn, file, out_file, sheet=sheet, range_=range_, fmt="parquet", **kwargs
    ),
    (
        "excel",
        "json",
    ): lambda conn, file, out_file, sheet=None, range_=None, **kwargs: ex.export_excel_with_inferred_types(
        conn, file, out_file, sheet=sheet, range_=range_, fmt="json", **kwargs
    ),
    (
        "excel",
        "tsv",
    ): lambda conn, file, out_file, sheet=None, range_=None, **kwargs: conn.sql(
        f"SELECT * FROM read_xlsx('{str(file.resolve())}', all_varchar = 'true'{ex._build_excel_options(sheet, range_)})"
    ).write_csv(
        str(out_file.resolve()), sep="\t"
    ),
    ("excel", "txt"): lambda conn, file, out_file, sheet=None, range_=None, **kwargs: (
        lambda dt: conn.sql(
            f"SELECT * FROM read_xlsx('{str(file.resolve())}', all_varchar = 'true'{ex._build_excel_options(sheet, range_)})"
        ).write_csv(str(out_file.resolve()), sep=dt)
    )(
        input("For TXT export, choose T for tab separated or C for comma separated: ")
        .strip()
        .lower()
        == "t"
        and "\t"
        or ","
    ),
}
