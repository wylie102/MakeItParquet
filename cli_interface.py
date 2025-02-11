#!/usr/bin/env python3
"""
CLI Interface module for DuckConvert.

Provides functions to parse command-line arguments, handle file type aliases,
and common user interactions (prompts for output type, Excel options, and TXT delimiters).
"""

import argparse
from pathlib import Path
from typing import Optional
import logging

# --- CLI Argument Parsing and File Type Helpers ---

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
    parser.add_argument(
        "--log-level",
        help="Set the logging level (e.g., DEBUG, INFO, WARNING)",
        default="INFO",
    )

    args = parser.parse_args()

    # Configure logging based on arguments
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    logging.basicConfig(
        level=numeric_level, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    return args


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


def prepare_cli_options(args):
    """
    Validate and prepare CLI options, handling prompts if needed.
    Returns a tuple of (input_path, output_type).
    """
    input_path = Path(args.input_path).resolve()
    if not (input_path.is_file() or input_path.is_dir()):
        logging.error(f"Input path '{input_path}' is neither a file nor a directory.")
        raise ValueError("Invalid input path")

    out_type = (
        args.output_type.strip().lower()
        if args.output_type
        else prompt_for_output_type()
    )
    try:
        out_type = FILE_TYPE_ALIASES[out_type]
    except KeyError:
        logging.error(f"Unsupported output format: {out_type}")
        raise ValueError("Invalid output type specified")

    return input_path, out_type


# --- User Interaction Functions ---


def get_delimiter(
    existing: str | None = None,
    prompt_text: str = "Enter delimiter (t for tab, c for comma): ",
) -> str:
    """
    If an existing delimiter is provided, returns it.
    Otherwise, prompts the user and returns "\t" for 't' or "," for other responses.
    """
    if existing is not None:
        return existing
    answer = input(prompt_text).strip().lower()
    return "\t" if answer == "t" else ","


def prompt_for_output_type() -> str:
    """
    Prompt the user for the desired output format.
    """
    return (
        input("Enter desired output format (csv, parquet, json, excel, tsv, txt): ")
        .strip()
        .lower()
    )


def prompt_for_txt_delimiter() -> dict:
    """
    Prompt the user for the delimiter used for TXT export.
    Returns a dict with the key 'delimiter'.
    """
    answer = (
        input("For TXT export, choose t for tab separated or c for comma separated: ")
        .strip()
        .lower()
    )
    return {"delimiter": "\t" if answer == "t" else ","}


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
