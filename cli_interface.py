#!/usr/bin/env python3
"""
CLI Interface module for DuckConvert.

Provides functions to parse command-line arguments, handle file type aliases,
and common user interactions (prompts for output type, Excel options, and TXT delimiters).
"""

import argparse
from pathlib import Path
from typing import Optional, List, Tuple
import logging

# --- CLI Argument Parsing and File Type Helpers ---


def get_delimiter(
    existing: Optional[str] = None,
    prompt_text: str = "Enter delimiter (t for tab, c for comma): ",
) -> str:
    if existing is not None:
        return existing
    answer = input(prompt_text).strip().lower()
    return "\t" if answer == "t" else ","


def prompt_for_output_type() -> str:
    return (
        input("Enter desired output format (csv, parquet, json, excel, tsv, txt): ")
        .strip()
        .lower()
    )


def prompt_for_txt_delimiter() -> dict:
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


def get_excel_options_for_files(
    files: List[Path],
) -> Tuple[Optional[str], Optional[str]]:
    """
    If one or more Excel files are present, prompt the user for Excel sheet and range based on
    the first Excel file found. Returns a tuple (sheet, range) or (None, None) if no Excel file exists.
    """
    # Filter files that are detected as Excel.
    excel_files = [f for f in files if get_file_type_by_extension(f) == "excel"]
    if not excel_files:
        return None, None
    logging.info("Excel options required for processing Excel files, prompting once.")
    return prompt_excel_options(excel_files[0])


#############################
# Settings Class
#############################


class Settings:
    def __init__(self):
        self.args = self._parse_cli_arguments()
        self.input_path = self._resolve_and_validate(Path(self.args.input_path))
        self.file_or_dir = self._determine_file_or_dir()

    def _parse_cli_arguments(self):
        parser = argparse.ArgumentParser(
            description="DuckConvert: Convert data files using DuckDB"
        )
        parser.add_argument("input_path", help="Path to the input file or directory")
        parser.add_argument("-i", "--input_type", help="Specify input file type")
        parser.add_argument("-o", "--output_type", help="Specify output file type")
        parser.add_argument(
            "-s", "--sheet", help="Excel sheet (name or number)", type=str
        )
        parser.add_argument("-c", "--range", help="Excel range (e.g., A2:E7)", type=str)
        parser.add_argument(
            "-d", "--delimiter", help="Delimiter for TXT export", type=str
        )
        parser.add_argument(
            "--log-level",
            help="Set the logging level (e.g., DEBUG, INFO, WARNING)",
            default="INFO",
        )
        args = parser.parse_args()

        # Configure logging based on arguments.
        numeric_level = getattr(logging, args.log_level.upper(), None)
        if not isinstance(numeric_level, int):
            numeric_level = logging.INFO
        logging.basicConfig(
            level=numeric_level, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        return args

    def prompt_for_output_type(self):
        return (
            input("Enter desired output format (csv, parquet, json, excel, tsv, txt): ")
            .strip()
            .lower()
        )

    def _resolve_and_validate(self, path: Path) -> Path:
        resolved_path = path.resolve()
        if not (resolved_path.is_file() or resolved_path.is_dir()):
            logging.error(
                f"Input path '{resolved_path}' is neither a file nor a directory."
            )
            raise ValueError("Invalid input path")
        return resolved_path

    def _determine_file_or_dir(self) -> str:
        return "file" if self.input_path.is_file() else "dir"

    def _determine_output_type(self):
        raw_out = (
            self.args.output_type.strip().lower()
            if self.args.output_type
            else self.prompt_for_output_type()
        )
        try:
            self.output_ext = FILE_TYPE_ALIASES[raw_out]
        except KeyError:
            logging.error(f"Unsupported output format: {raw_out}")
            raise ValueError("Invalid output type specified")

    def _determine_excel_options(self):
        # If Excel options are not provided, prompt for them.
        if self.args.sheet is None and self.args.range is None:
            if self.input_path.is_file():
                excel_files = [self.input_path]
            else:
                excel_files = [f for f in self.input_path.glob("*") if f.is_file()]
            self.args.sheet, self.args.range = get_excel_options_for_files(excel_files)

        # For TXT output, if no delimiter provided, prompt for it.
        if self.out_type == "txt" and (
            self.args.delimiter is None or self.args.delimiter.strip() == ""
        ):
            txt_kwargs = prompt_for_txt_delimiter()
            self.args.delimiter = txt_kwargs["delimiter"]
