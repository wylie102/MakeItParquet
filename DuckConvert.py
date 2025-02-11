#!/usr/bin/env python3
# /// script
# dependencies = [
#     "duckdb",
# ]
# ///

"""
DuckConvert: A conversion tool to convert between popular data storage file types
(CSV/TXT, JSON, Parquet, Excel) using DuckDB's Python API.

Usage example (assuming the script is aliased as "conv"):
    conv path/to/folder -i excel -s 1 -c A2:E7 -o pq

If the input type is not provided, the file extension is used to auto-detect.
For Excel files, if no sheet or range has been provided via the command line,
the tool will prompt for options.
"""

import logging
from pathlib import Path
import duckdb
from typing import Optional

from cli_interface import (
    parse_cli_arguments,
    FILE_TYPE_ALIASES,
    get_file_type_by_extension,
    prompt_excel_options,
    prompt_for_output_type,  # new import
    prompt_for_txt_delimiter,  # new import
)
from path_manager import create_path_manager
from conversions import CONVERSION_FUNCTIONS
from excel_utils import load_excel_extension

# Mapping of output file extensions
EXTENSIONS = {
    "csv": ".csv",
    "tsv": ".tsv",
    "txt": ".txt",
    "parquet": ".parquet",
    "json": ".json",
    "excel": ".xlsx",
}


# --- Helper functions for CLI & DuckDB connection ---


def process_file(
    file: Path,
    in_type: str,
    out_type: str,
    conn: duckdb.DuckDBPyConnection,
    excel_sheet=None,
    excel_range=None,
    output_dir: Optional[Path] = None,
    **kwargs,
) -> None:
    """
    Process a single file conversion:
      1. Construct the output file (ensuring no overwrite).
      2. Call the conversion function mapped to (in_type, out_type).
    """
    logging.info(f"Processing file: {file.name}")

    # Determine the output file
    if output_dir is not None:
        output_file = output_dir / (file.stem + EXTENSIONS[out_type])
    else:
        output_file = file.with_suffix(EXTENSIONS[out_type])

    # Avoid overwriting an existing file.
    if output_file.exists():
        base = output_file.stem
        ext = output_file.suffix
        counter = 1
        candidate = output_file.parent / f"{base}_{counter}{ext}"
        while candidate.exists():
            counter += 1
            candidate = output_file.parent / f"{base}_{counter}{ext}"
        output_file = candidate

    conversion_key = (in_type, out_type)
    if conversion_key not in CONVERSION_FUNCTIONS:
        logging.error(f"Unsupported conversion: {in_type} to {out_type}")
        return

    try:
        # Call conversion function with Excel-specific options if needed.
        if in_type == "excel":
            CONVERSION_FUNCTIONS[conversion_key](
                conn, file, output_file, sheet=excel_sheet, range_=excel_range, **kwargs
            )
        else:
            CONVERSION_FUNCTIONS[conversion_key](conn, file, output_file, **kwargs)
        logging.info(f"Converted to {output_file.name}")
    except Exception as e:
        logging.error(f"Error processing file {file.name}: {e}")


# --- Main orchestration wrapped in a class ---


class DuckConvert:
    def __init__(self):
        self.args = parse_cli_arguments()
        self.input_path = Path(self.args.input_path).resolve()
        if not (self.input_path.is_file() or self.input_path.is_dir()):
            logging.error(
                f"Input path '{self.input_path}' is neither a file nor a directory."
            )
            raise ValueError("Invalid input path")
        self.out_type = (
            self.args.output_type.strip().lower()
            if self.args.output_type
            else prompt_for_output_type()
        )
        try:
            self.out_type = FILE_TYPE_ALIASES[self.out_type]
        except KeyError:
            logging.error(f"Unsupported output format: {self.out_type}")
            raise ValueError("Invalid output type specified")
        
        # New, simplified conversion parameter preparation:
        if self.input_path.is_file():
            self.files_to_process = [self.input_path]
            self.output_dest = None
            self.source_type = self.input_path.suffix.lstrip(".").lower()
        else:
            pm = create_path_manager(self.input_path, self.out_type)
            self.files_to_process = pm.get_files(pm.input_alias)
            self.output_dest = pm.output_path
            self.output_dest.mkdir(parents=True, exist_ok=True)
            self.source_type = pm.input_alias

        # Determine Excel options once if needed.
        self.common_excel_sheet = self.args.sheet
        self.common_excel_range = self.args.range
        if self.args.sheet is None and self.args.range is None:
            excel_files = [
                f
                for f in self.files_to_process
                if get_file_type_by_extension(f) == "excel"
            ]
            if excel_files:
                logging.info(
                    "Excel options required for processing Excel files, prompting once."
                )
                self.common_excel_sheet, self.common_excel_range = prompt_excel_options(
                    excel_files[0]
                )

        # Determine extra kwargs for TXT export.
        self.extra_kwargs = {}
        if self.out_type == "txt":
            self.extra_kwargs = prompt_for_txt_delimiter()

    def run(self):
        """
        Open a DuckDB connection (with the Excel extension loaded) and process each file.
        """
        with duckdb.connect(database=":memory:") as conn:
            if not load_excel_extension(conn):
                return
            for file in self.files_to_process:
                detected = get_file_type_by_extension(file)
                if detected is None:
                    logging.info(
                        f"Skipping file with unsupported extension: {file.name}"
                    )
                    continue
                in_type = (
                    FILE_TYPE_ALIASES[self.args.input_type.lower()]
                    if self.args.input_type
                    else detected
                )
                if in_type == "excel":
                    excel_sheet, excel_range = (
                        self.common_excel_sheet,
                        self.common_excel_range,
                    )
                else:
                    excel_sheet, excel_range = self.args.sheet, self.args.range
                process_file(
                    file,
                    in_type,
                    self.out_type,
                    conn,
                    excel_sheet,
                    excel_range,
                    output_dir=self.output_dest,
                    **self.extra_kwargs,
                )


def main():
    try:
        args = parse_cli_arguments()
        # configure logging based on user input
        numeric_level = getattr(logging, args.log_level.upper(), None)
        if not isinstance(numeric_level, int):
            numeric_level = logging.INFO
        logging.basicConfig(
            level=numeric_level, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        converter = DuckConvert()
        converter.run()
    except Exception as e:
        logging.error(f"Conversion failed: {e}")


if __name__ == "__main__":
    main()
