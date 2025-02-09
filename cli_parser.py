#!/usr/bin/env python3
# /// script
# dependencies = [
#     "duckdb",
# ]
# ///

"""
CLI Parser module for DuckConvert.

Provides command-line argument parsing and helper functions.
"""

import argparse
from pathlib import Path
from typing import Optional, Dict

# Mapping for file type aliases.
FILE_TYPE_ALIASES = {
    "csv": "csv",
    "txt": "csv",  # Process .txt as CSV.
    "tsv": "csv",  # Process .tsv as CSV.
    "json": "json",
    "js": "json",  # Added alias for JSON.
    "parquet": "parquet",
    "parq": "parquet",
    "pq": "parquet",
    "excel": "excel",
    "xlsx": "excel",
    "ex": "excel",  # Added alias "ex" for Excel.
}


def parse_cli_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert between data file types using DuckDB."
    )
    parser.add_argument("input_path", help="Path to the input file or directory.")
    parser.add_argument(
        "-o",
        "--output_type",
        help="Output file type (csv, parquet, json, excel).",
        default=None,
    )
    parser.add_argument(
        "-i",
        "--input_type",
        help="Input file type (csv, json, parquet, excel).",
        default=None,
    )
    parser.add_argument(
        "-s",
        "--sheet",
        help="Excel sheet number or name (for Excel input).",
        default=None,
    )
    parser.add_argument(
        "-c",
        "--range",
        help="Excel cell range (for Excel input, e.g., A1:D100).",
        default=None,
    )
    parser.add_argument(
        "-d",
        "--delimiter",
        help="Delimiter to use for TXT export (use 't' for tab, 'c' for comma or provide a literal value)",
        default=None,
    )
    return parser.parse_args()


def get_file_type_by_extension(path: Path) -> Optional[str]:
    """
    Determine file type based on file extension.

    Returns the canonical file type (e.g., 'csv', 'json', etc.) or None if unsupported.
    """
    ext = path.suffix.lower()
    extension_map: Dict[str, str] = {
        ".csv": "csv",
        ".txt": "csv",
        ".tsv": "csv",  # Added for TSV files.
        ".json": "json",
        ".parquet": "parquet",
        ".parq": "parquet",
        ".pq": "parquet",
        ".xlsx": "excel",
    }
    return extension_map.get(ext)


def prompt_excel_options(file_path: Path):
    """
    Prompt the user for Excel options if not provided via CLI.

    Returns:
        A tuple of (sheet, range) options.
    """
    print(f"Excel options required for file: {file_path.name}")
    sheet = (
        input("Enter sheet name or number (default is first sheet): ").strip() or None
    )
    cell_range = (
        input("Enter cell range (e.g., A1:D100) or leave blank for all data: ").strip()
        or None
    )
    return sheet, cell_range
