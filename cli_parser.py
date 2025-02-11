#!/usr/bin/env python3
"""
CLI Parser module for DuckConvert.

Provides functions to parse command-line arguments, file type aliases,
and helper functions to detect file types and prompt for Excel options.
"""

import argparse
from pathlib import Path
from typing import Optional

# Map common file type names/aliases to canonical names.
FILE_TYPE_ALIASES = {
    "csv": "csv",
    "txt": "txt",
    "tsv": "tsv",
    "json": "json",
    "excel": "excel",
    "xlsx": "excel",
    "xls": "excel",
    "parquet": "parquet",
    "pq": "parquet",
}


def parse_cli_arguments():
    parser = argparse.ArgumentParser(
        description="DuckConvert: Convert data files using DuckDB"
    )
    parser.add_argument("input_path", help="Path to the input file or directory")
    parser.add_argument("-i", "--input_type", help="Specify input file type")
    parser.add_argument("-o", "--output_type", help="Specify output file type")
    parser.add_argument("-s", "--sheet", help="Excel sheet (name or number)", type=str)
    parser.add_argument("-c", "--range", help="Excel range (e.g., A2:E7)", type=str)
    parser.add_argument("-d", "--delimiter", help="Delimiter for TXT export", type=str)
    return parser.parse_args()


def get_file_type_by_extension(file_path: Path) -> Optional[str]:
    ext = file_path.suffix.lower()
    if ext == ".csv":
        return "csv"
    elif ext in [".tsv", ".txt"]:
        return "txt"
    elif ext == ".json":
        return "json"
    elif ext in [".parquet", ".pq"]:
        return "parquet"
    elif ext in [".xlsx", ".xls"]:
        return "excel"
    else:
        return None


def prompt_excel_options(file: Path):
    sheet = (
        input(
            f"Enter Excel sheet for file {file.name} (default: first sheet): "
        ).strip()
        or None
    )
    range_ = (
        input(f"Enter Excel cell range for file {file.name} (or leave blank): ").strip()
        or None
    )
    return sheet, range_
