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
from path_manager import create_path_manager, FilePathManager
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


def process_file(
    file: Path,
    in_type: str,
    out_type: str,
    conn: duckdb.DuckDBPyConnection,
    excel_sheet=None,
    excel_range=None,
    output_dir: Optional[Path] = None,
    **kwargs,  # Accept extra keyword arguments
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
                conn, file, output_file, sheet=excel_sheet, range_=excel_range, **kwargs
            )
        else:
            CONVERSION_FUNCTIONS[conversion_key](conn, file, output_file, **kwargs)
        logging.info(f"File {file.name} converted to {output_file.name}")
    except Exception as e:
        logging.error(f"Error processing file {file.name}: {e}")


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
                if source_type_optional is None:
                    logging.error("Could not determine input type from file extension.")
                    return None
                source_type = source_type_optional
                logging.info("Number of files to be converted: 1")
                return ([input_path], None, source_type)
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
            # Retrieve the input type using the path manager.
            source_type_optional = pm.input_alias
            if source_type_optional is None:
                logging.error(
                    "Could not infer the input type via the path manager for the input directory."
                )
                return None
            source_type = source_type_optional  # now source_type is a str
            logging.info(
                f"Detected majority input type from path manager: {source_type}"
            )
        files = pm.get_files(source_type)
        output_dest = pm.output_path
        output_dest.mkdir(parents=True, exist_ok=True)
        logging.info(f"Number of files to be converted: {len(files)}")
        return (files, output_dest, source_type)
    else:
        logging.error(f"Input path '{input_path}' is neither a file nor a directory.")
        return None


def main():
    args = parse_cli_arguments()
    input_path = Path(args.input_path).resolve()
    if input_path.is_file():
        logging.info("Input is a file.")
    elif input_path.is_dir():
        logging.info("Input is a directory.")
    else:
        logging.error(f"Input path '{input_path}' is neither a file nor a directory.")
        return

    if args.output_type:
        out_lower = args.output_type.lower()
        if out_lower in ["tsv", "txt"]:
            out_type = out_lower
        else:
            try:
                out_type = FILE_TYPE_ALIASES[out_lower]
            except KeyError:
                logging.error(f"Unsupported output format: {args.output_type}")
                return
    else:
        prompt_msg = (
            "Enter desired output format (csv, parquet, json, excel, tsv, txt): "
        )
        user_output = input(prompt_msg).strip().lower()
        if user_output in ["tsv", "txt"]:
            out_type = user_output
        else:
            try:
                out_type = FILE_TYPE_ALIASES[user_output]
            except KeyError:
                logging.error(f"Unsupported output format: {user_output}")
                return

    conversion_params = prepare_conversion_parameters(input_path, args, out_type)
    if conversion_params is None:
        return
    files_to_process, output_dest, source_type = conversion_params

    # Determine Excel options once for the whole batch if needed.
    common_excel_sheet = args.sheet
    common_excel_range = args.range
    if args.sheet is None and args.range is None:
        # Check if there is any Excel file in the batch
        excel_files = [
            f for f in files_to_process if get_file_type_by_extension(f) == "excel"
        ]
        if excel_files:
            logging.info(
                "Excel options required for processing Excel files, prompting once."
            )
            common_excel_sheet, common_excel_range = prompt_excel_options(
                excel_files[0]
            )

    with duckdb.connect(database=":memory:") as conn:
        try:
            conn.install_extension("excel")
            conn.load_extension("excel")
        except Exception as e:
            logging.error(f"Failed to install/load excel extension: {e}")
            return

        for file in files_to_process:
            detected = get_file_type_by_extension(file)
            if detected is None:
                logging.info(f"Skipping file with unsupported extension: {file.name}")
                continue

            in_type = (
                FILE_TYPE_ALIASES[args.input_type.lower()]
                if args.input_type
                else detected
            )

            if in_type == "excel":
                excel_sheet = common_excel_sheet
                excel_range = common_excel_range
            else:
                excel_sheet = args.sheet
                excel_range = args.range

            process_file(
                file,
                in_type,
                out_type,
                conn,
                excel_sheet,
                excel_range,
                output_dir=output_dest,
                **extra_kwargs,
            )

    # --- Get delimiter for TXT export from CLI
    extra_kwargs = {}
    if out_type == "txt":
        if args.delimiter is not None:
            d = args.delimiter.strip().lower()
            if d == "t":
                extra_kwargs["delimiter"] = "\t"
            elif d == "c":
                extra_kwargs["delimiter"] = ","
            else:
                extra_kwargs["delimiter"] = args.delimiter
        else:
            answer = (
                input(
                    "For TXT export, choose t for tab separated or c for comma separated: "
                )
                .strip()
                .lower()
            )
            extra_kwargs["delimiter"] = "\t" if answer == "t" else ","

    with duckdb.connect(database=":memory:") as conn:
        try:
            conn.install_extension("excel")
            conn.load_extension("excel")
        except Exception as e:
            logging.error(f"Failed to install/load excel extension: {e}")
            return

        for file in files_to_process:
            detected = get_file_type_by_extension(file)
            if detected is None:
                logging.info(f"Skipping file with unsupported extension: {file.name}")
                continue

            in_type = (
                FILE_TYPE_ALIASES[args.input_type.lower()]
                if args.input_type
                else detected
            )

            if in_type == "excel":
                excel_sheet = common_excel_sheet
                excel_range = common_excel_range
            else:
                excel_sheet = args.sheet
                excel_range = args.range

            process_file(
                file,
                in_type,
                out_type,
                conn,
                excel_sheet,
                excel_range,
                output_dir=output_dest,
                **extra_kwargs,
            )


if __name__ == "__main__":
    main()
