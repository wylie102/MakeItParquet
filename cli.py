#!/usr/bin/env python3
"""
CLI module for DuckConvert.
This module handles argument parsing and CLI‐related helper functions.
"""

import argparse
from pathlib import Path
from typing import Optional, Union, Tuple

# Mapping file extensions to input type names.
ALLOWED_EXT_MAP = {
    ".csv": "csv",
    ".txt": "csv",  # treat .txt exactly like CSV
    ".json": "json",
    ".parquet": "parquet",
    ".parq": "parquet",
    ".pq": "parquet",
    ".xlsx": "excel",
    # .xls are not supported by DuckDB by default.
}

# Mapping of file type aliases (both input and output) to their full names.
FILE_TYPE_ALIASES = {
    "csv": "csv",
    "txt": "csv",
    "parquet": "parquet",
    "pq": "parquet",
    "json": "json",
    "js": "json",
    "excel": "excel",
    "ex": "excel",
}


def get_file_type_by_extension(file: Path) -> Optional[str]:
    """Infer the file type from the file extension."""
    return ALLOWED_EXT_MAP.get(file.suffix.lower())


def prompt_excel_options(file: Path) -> Tuple[Optional[Union[int, str]], Optional[str]]:
    """
    Ask the user whether they want to provide Excel options (sheet and/or range).

    Returns:
        tuple: A tuple (sheet, excel_range) where 'sheet' is either an integer or a string (or None)
               and 'excel_range' is a string (or None).
    """
    sheet: Optional[Union[int, str]] = None
    excel_range: Optional[str] = None
    sheet_num: Optional[int] = None
    sheet_name: Optional[str] = None

    answer = (
        input(
            f"For Excel file '{file.name}', would you like to specify a sheet? (y/n): "
        )
        .strip()
        .lower()
    )
    if answer.startswith("y"):
        sheet_input = input(
            "Enter the sheet number (for example, 1 for Sheet1) or sheet name: "
        ).strip()
        try:
            sheet_num = int(sheet_input)
        except ValueError:
            sheet_name = sheet_input

    answer = input("Would you like to specify an import range? (y/n): ").strip().lower()
    if answer.startswith("y"):
        excel_range = input("Enter the range (for example, A1:B2): ").strip()

    if sheet_num is not None:
        sheet = sheet_num
    elif sheet_name is not None:
        sheet = sheet_name

    return sheet, excel_range


def parse_cli_arguments() -> argparse.Namespace:
    """
    Parse command line arguments using argparse.

    Returns:
        argparse.Namespace: The parsed arguments.
    """
    valid_types = list(FILE_TYPE_ALIASES.keys())
    parser = argparse.ArgumentParser(
        description="Convert between popular data storage file types using DuckDB."
    )
    parser.add_argument(
        "input_path",
        type=str,
        help="Path to a file or directory containing files.",
    )
    parser.add_argument(
        "-i",
        "--input-type",
        type=str,
        choices=valid_types,
        default=None,
        help="Override auto-detection of input file type. (Allowed: csv, txt, json, js, parquet, pq, excel, ex)",
    )
    parser.add_argument(
        "-o",
        "--output-type",
        type=str,
        choices=valid_types,
        default=None,
        help="Desired output file type. (Allowed: csv, json, js, parquet, pq, excel, ex)",
    )
    parser.add_argument(
        "-op",
        "--output-path",
        type=str,
        default=None,
        help="Output file (if input is a single file) or directory (if input is a folder).",
    )
    parser.add_argument(
        "-s",
        "--sheet",
        type=str,
        default=None,
        help="For Excel input: sheet number or sheet name to import (e.g. 1 or 'Sheet1').",
    )
    parser.add_argument(
        "-c",
        "--range",
        type=str,
        default=None,
        help="For Excel input: cell range to import (e.g. A1:B2).",
    )
    args = parser.parse_args()
    return args
