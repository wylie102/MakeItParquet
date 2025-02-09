#!/usr/bin/env python3
# /// script
# dependencies = [
#     "duckdb",
# ]
# ///

"""
DuckConvert: A conversion tool to convert between popular data storage file types (CSV/TXT, JSON, Parquet, Excel)
using DuckDB's Python API.

Usage example on the command line (assuming the script is aliased as "conv"):
    conv path/to/folder -i excel -s 1 -c A2:E7 -o pq

If the input type is not provided, the file extension is used to auto-detect.
For Excel files, if no sheet or range has been provided through the command line,
the tool will prompt for options.

DuckDB reference:
- CSV: https://duckdb.org/docs/guides/file_formats/csv_import
- Excel: https://duckdb.org/docs/guides/file_formats/excel_import
- JSON: https://duckdb.org/docs/guides/file_formats/json_export
- Parquet: https://duckdb.org/docs/guides/file_formats/parquet_import
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
from path_manager import create_path_manager
from conversions import CONVERSION_FUNCTIONS

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Mapping of output file extensions
EXTENSIONS = {
    "csv": ".csv",
    "parquet": ".parquet",
    "json": ".json",
    "excel": ".xlsx",
}


def process_file(
    file: Path,
    in_type: str,
    out_type: str,
    conn: duckdb.DuckDBPyConnection,
    excel_sheet=None,
    excel_range=None,
    output_dir: Optional[Path] = None,
) -> None:
    """Process a single file conversion using the conversion lookup."""
    logging.info(f"Processing file: {file.name}")

    # Determine output file
    if output_dir is not None:
        output_file = output_dir / (file.stem + EXTENSIONS[out_type])
    else:
        output_file = file.with_suffix(EXTENSIONS[out_type])

    # Ensure we do not overwrite an existing file.
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
        raise ValueError(f"Unsupported conversion: {in_type} to {out_type}")

    try:
        # Use appropriate conversion function (passing sheet/range for Excel input)
        if in_type == "excel":
            CONVERSION_FUNCTIONS[conversion_key](
                conn, file, output_file, sheet=excel_sheet, range_=excel_range
            )
        else:
            CONVERSION_FUNCTIONS[conversion_key](conn, file, output_file)
        logging.info(f"Written {out_type} file: {output_file.resolve()}")
    except Exception as e:
        logging.error(f"Error processing file {file.name}: {e}")


def main():
    args = parse_cli_arguments()
    input_path = Path(args.input_path).resolve()

    # The -o option is now used as the output type alias (e.g., "pq" for parquet)
    if args.output_type:
        out_type = FILE_TYPE_ALIASES[args.output_type.lower()]
    else:
        # If not provided via -o, prompt the user.
        user_output = (
            input("Enter desired output format (csv, parquet, json, excel): ")
            .strip()
            .lower()
        )
        try:
            out_type = FILE_TYPE_ALIASES[user_output]
        except KeyError:
            logging.error(f"Unsupported output format: {user_output}")
            return

    with duckdb.connect(database=":memory:") as conn:
        # Install and load the excel extension using the Python API.
        try:
            conn.install_extension("excel")
            conn.load_extension("excel")
        except Exception as e:
            logging.error(f"Failed to install/load excel extension: {e}")

        if input_path.is_file():
            # Determine input type (CLI override or auto-detect)
            if args.input_type:
                in_type = FILE_TYPE_ALIASES[args.input_type.lower()]
            else:
                detected = get_file_type_by_extension(input_path)
                if detected is None:
                    logging.error(
                        f"Could not automatically detect file type for {input_path}"
                    )
                    return
                in_type = detected

            excel_sheet = args.sheet
            excel_range = args.range
            if in_type == "excel" and (excel_sheet is None and excel_range is None):
                excel_sheet, excel_range = prompt_excel_options(input_path)

            process_file(input_path, in_type, out_type, conn, excel_sheet, excel_range)
        elif input_path.is_dir():
            # Create output destination directory using the path manager.
            pm = create_path_manager(input_path, out_type)
            output_dest = pm.output_path
            output_dest.mkdir(parents=True, exist_ok=True)

            # Process each file in the directory using the output type alias (-o)
            for file in input_path.iterdir():
                if file.is_file():
                    detected = get_file_type_by_extension(file)
                    if detected is None:
                        logging.info(
                            f"Skipping file with unsupported extension: {file.name}"
                        )
                        continue
                    in_type = (
                        FILE_TYPE_ALIASES[args.input_type.lower()]
                        if args.input_type
                        else detected
                    )
                    excel_sheet = args.sheet
                    excel_range = args.range
                    if in_type == "excel" and (
                        excel_sheet is None and excel_range is None
                    ):
                        logging.info(f"For Excel file '{file.name}':")
                        excel_sheet, excel_range = prompt_excel_options(file)
                    process_file(
                        file,
                        in_type,
                        out_type,
                        conn,
                        excel_sheet,
                        excel_range,
                        output_dir=output_dest,
                    )
        else:
            logging.error(
                f"Input path '{input_path}' is neither a file nor a directory."
            )


if __name__ == "__main__":
    main()
