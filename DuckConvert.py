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

from cli_parser import (
    parse_cli_arguments,
    FILE_TYPE_ALIASES,
    get_file_type_by_extension,
    prompt_excel_options,
)
from path_manager import (
    create_path_manager,
)  # returns a manager with get_files() and output_path attributes
from conversions import CONVERSION_FUNCTIONS

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

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


def determine_output_type(args) -> Optional[str]:
    """
    Determine the desired output type from CLI args or by prompting the user.
    """
    if args.output_type:
        out_lower = args.output_type.lower()
        if out_lower in ["tsv", "txt"]:
            return out_lower
        else:
            try:
                return FILE_TYPE_ALIASES[out_lower]
            except KeyError:
                logging.error(f"Unsupported output format: {args.output_type}")
                return None
    else:
        user_output = (
            input("Enter desired output format (csv, parquet, json, excel, tsv, txt): ")
            .strip()
            .lower()
        )
        if user_output in ["tsv", "txt"]:
            return user_output
        else:
            try:
                return FILE_TYPE_ALIASES[user_output]
            except KeyError:
                logging.error(f"Unsupported output format: {user_output}")
                return None


def determine_txt_delimiter(args) -> dict:
    """
    Determine delimiter settings for TXT export.
    """
    extra_kwargs = {}
    if args.delimiter is not None:
        d = args.delimiter.strip().lower()
        extra_kwargs["delimiter"] = (
            "\t" if d == "t" else "," if d == "c" else args.delimiter
        )
    else:
        answer = (
            input(
                "For TXT export, choose t for tab separated or c for comma separated: "
            )
            .strip()
            .lower()
        )
        extra_kwargs["delimiter"] = "\t" if answer == "t" else ","
    return extra_kwargs


def load_excel_extension(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Install and load the Excel extension on the given DuckDB connection.
    """
    try:
        conn.install_extension("excel")
        conn.load_extension("excel")
        return True
    except Exception as e:
        logging.error(f"Failed to install/load excel extension: {e}")
        return False


def prepare_conversion_parameters(input_path: Path, args, out_type: str):
    """
    Return a tuple (files_to_process, output_dest, source_type) based on the input path.
    For a file, output_dest is None; for a directory, it is provided by the path manager.
    """
    if input_path.is_file():
        if args.input_type:
            try:
                source_type = FILE_TYPE_ALIASES[args.input_type.lower()]
            except KeyError:
                logging.error(f"Unsupported input format: {args.input_type}")
                return None
            logging.info("Number of files to be converted: 1")
            return ([input_path], None, source_type)
        else:
            source_type_optional = get_file_type_by_extension(input_path)
            if source_type_optional is None:
                logging.error("Could not determine input type from file extension.")
                return None
            logging.info("Number of files to be converted: 1")
            return ([input_path], None, source_type_optional)
    elif input_path.is_dir():
        pm = create_path_manager(input_path, out_type)
        if pm is None or not (hasattr(pm, "get_files") and hasattr(pm, "output_path")):
            logging.error("Invalid path manager for directory input.")
            return None
        if args.input_type:
            try:
                source_type = FILE_TYPE_ALIASES[args.input_type.lower()]
            except KeyError:
                logging.error(f"Unsupported input format: {args.input_type}")
                return None
            logging.info(f"Input type provided: {source_type}")
        else:
            source_type_optional = pm.input_alias
            if source_type_optional is None:
                logging.error(
                    "Could not infer the input type via the path manager for the input directory."
                )
                return None
            source_type = source_type_optional
            logging.info(f"Detected input extension as: {source_type}")
        files = pm.get_files(source_type)
        output_dest = pm.output_path
        output_dest.mkdir(parents=True, exist_ok=True)
        logging.info(f"Number of files to be converted: {len(files)}")
        return (files, output_dest, source_type)
    else:
        logging.error(f"Input path '{input_path}' is neither a file nor a directory.")
        return None


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
        self.out_type = determine_output_type(self.args)
        if self.out_type is None:
            raise ValueError("Invalid output type specified")
        conversion_params = prepare_conversion_parameters(
            self.input_path, self.args, self.out_type
        )
        if conversion_params is None:
            raise ValueError("Failed to prepare conversion parameters")
        self.files_to_process, self.output_dest, self.source_type = conversion_params

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
            self.extra_kwargs = determine_txt_delimiter(self.args)

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
        converter = DuckConvert()
        converter.run()
    except Exception as e:
        logging.error(f"Conversion failed: {e}")


if __name__ == "__main__":
    main()
