#!/usr/bin/env python3
# /// script
# dependencies = [
#     "duckdb",
# ]
# ///

"""
A conversion tool to convert between popular data storage file types (CSV/TXT, JSON, Parquet, Excel)
using DuckDB's Python API.

Usage example on the command line (assuming the script is aliased as "conv"):
    conv path/to/folder -i excel -s 1 -c A2:E7 -o pq

If the input type is not provided, the file extension is used to auto-detect.
For Excel files, if no sheet or range has been provided through the command line,
the tool will ask whether you want to enter a sheet number/name or a range.

DuckDB reference:
- CSV import/export: https://duckdb.org/docs/guides/file_formats/csv_import
- Excel import/export: https://duckdb.org/docs/guides/file_formats/excel_import
- JSON import/export: https://duckdb.org/docs/guides/file_formats/json_export
- Parquet import/export: https://duckdb.org/docs/guides/file_formats/parquet_import
- Python API reference: https://duckdb.org/docs/api/python/reference/
"""

import argparse
import logging
from pathlib import Path
from typing import Optional, Union, Tuple
import duckdb

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

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
        # Example: entering "1" will be interpreted as sheet number 1 (converted later to "Sheet1")
        sheet_input = input(
            "Enter the sheet number (for example, 1 for Sheet1) or sheet name: "
        ).strip()
        try:
            sheet_num = int(sheet_input)
        except ValueError:
            sheet_name = sheet_input

    answer = input("Would you like to specify an import range? (y/n): ").strip().lower()
    if answer.startswith("y"):
        # Example input: A1:B2
        excel_range = input("Enter the range (for example, A1:B2): ").strip()

    if sheet_num is not None:
        sheet = sheet_num
    elif sheet_name is not None:
        sheet = sheet_name

    return sheet, excel_range


def process_file(
    file: Path,
    in_type: str,
    out_type: str,
    output_dest: Optional[Path],
    conn: duckdb.DuckDBPyConnection,
    excel_sheet: Optional[Union[int, str]] = None,
    excel_range: Optional[str] = None,
) -> None:
    """Process a single file conversion using DuckDB."""
    logging.info(f"Processing file: {file.name}")

    # Create a relation based on the input type
    if in_type == "csv":
        relation = conn.read_csv(str(file.resolve()))
    elif in_type == "json":
        relation = conn.read_json(str(file.resolve()))
    elif in_type == "parquet":
        relation = conn.from_parquet(str(file.resolve()))
    elif in_type == "excel":
        # Build the SQL query for reading an Excel file.
        query = f"SELECT * FROM read_xlsx('{file.resolve()}'"
        if excel_sheet is not None:
            # If sheet is an integer, convert to 'Sheet{number}'; otherwise, assume it's a valid name.
            sheet_param = (
                f"Sheet{excel_sheet}" if isinstance(excel_sheet, int) else excel_sheet
            )
            query += f", sheet = '{sheet_param}'"
        if excel_range is not None:
            query += f", range = '{excel_range}'"
        query += ")"
        relation = conn.sql(query)
    else:
        raise ValueError(f"Unsupported input file type: {in_type}")

    # Determine the output file path.
    if output_dest is not None:
        out_path = output_dest
        if out_path.is_dir() or out_path.suffix == "":
            # Ensure the directory exists.
            out_path.mkdir(parents=True, exist_ok=True)
            if out_type == "csv":
                output_file = out_path / f"{file.stem}.csv"
            elif out_type == "parquet":
                output_file = out_path / f"{file.stem}.parquet"
            elif out_type == "json":
                output_file = out_path / f"{file.stem}.json"
            elif out_type == "excel":
                output_file = out_path / f"{file.stem}.xlsx"
            else:
                raise ValueError(f"Unsupported output file type: {out_type}")
        else:
            output_file = out_path
    else:
        # No output destination specified.
        if file.is_file():
            if out_type == "csv":
                output_file = file.with_suffix(".csv")
            elif out_type == "parquet":
                output_file = file.with_suffix(".parquet")
            elif out_type == "json":
                output_file = file.with_suffix(".json")
            elif out_type == "excel":
                output_file = file.with_suffix(".xlsx")
            else:
                raise ValueError(f"Unsupported output file type: {out_type}")
        else:
            # Default for directory input.
            default_dir = file.parent / "converted"
            default_dir.mkdir(parents=True, exist_ok=True)
            if out_type == "csv":
                output_file = default_dir / f"{file.stem}.csv"
            elif out_type == "parquet":
                output_file = default_dir / f"{file.stem}.parquet"
            elif out_type == "json":
                output_file = default_dir / f"{file.stem}.json"
            elif out_type == "excel":
                output_file = default_dir / f"{file.stem}.xlsx"
            else:
                raise ValueError(f"Unsupported output file type: {out_type}")

    # Write out based on the desired output type.
    if out_type == "csv":
        relation.write_csv(str(output_file.resolve()))
    elif out_type == "parquet":
        relation.write_parquet(str(output_file.resolve()))
    elif out_type == "json":
        # For JSON, use a COPY statement from the query.
        query_str = relation.sql_query()
        conn.execute(f"COPY ({query_str}) TO '{output_file.resolve()}'")
    elif out_type == "excel":
        query_str = relation.sql_query()
        conn.execute(
            f"COPY ({query_str}) TO '{output_file.resolve()}' WITH (FORMAT XLSX, HEADER TRUE)"
        )
    else:
        raise ValueError(f"Unsupported output file type: {out_type}")

    logging.info(f"Written {out_type} file: {output_file.resolve()}")


def main():
    parser = argparse.ArgumentParser(
        description="Convert between popular data storage file types using DuckDB."
    )
    # Update choices to include the new aliases.
    valid_types = list(FILE_TYPE_ALIASES.keys())
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
    input_path = Path(args.input_path).resolve()
    output_dest = (
        Path(args.output_path).resolve() if args.output_path is not None else None
    )

    # Determine (or later override) the output type interactively if not passed.
    if args.output_type:
        out_type = FILE_TYPE_ALIASES[args.output_type.lower()]
    else:
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

    # Open DuckDB connection in memory.
    with duckdb.connect(database=":memory:") as conn:
        if input_path.is_file():
            # Determine input type.
            if args.input_type:
                in_type = FILE_TYPE_ALIASES[args.input_type.lower()]
            else:
                detected = get_file_type_by_extension(input_path)
                if detected is None:
                    logging.error(
                        f"Could not automatically detect file type for {input_path}"
                    )
                    return
                answer = (
                    input(
                        f"Detected input file type as '{detected}'. Use that? (y/n): "
                    )
                    .strip()
                    .lower()
                )
                if answer.startswith("n"):
                    user_in = (
                        input("Enter the input file type (csv, json, parquet, excel): ")
                        .strip()
                        .lower()
                    )
                    try:
                        in_type = FILE_TYPE_ALIASES[user_in]
                    except KeyError:
                        logging.error(f"Unsupported input format: {user_in}")
                        return
                else:
                    in_type = detected

            # For Excel files, if no sheet or range was provided via arguments, prompt the user.
            excel_sheet = args.sheet
            excel_range = args.range
            if in_type == "excel" and (excel_sheet is None and excel_range is None):
                excel_sheet, excel_range = prompt_excel_options(input_path)

            process_file(
                input_path,
                in_type,
                out_type,
                output_dest,
                conn,
                excel_sheet,
                excel_range,
            )
        elif input_path.is_dir():
            # For directory input, if output destination is not provided, default to a folder
            # named after the chosen output file type.
            if output_dest is None:
                output_dest = input_path.parent / out_type
                output_dest.mkdir(parents=True, exist_ok=True)
                logging.info(
                    f"Output directory not provided. Using default: {output_dest.resolve()}"
                )
            # Process every file in the directory having an allowed extension.
            for file in input_path.iterdir():
                if file.is_file():
                    detected = get_file_type_by_extension(file)
                    if detected is None:
                        logging.info(
                            f"Skipping file with unsupported extension: {file.name}"
                        )
                        continue
                    # Use command-line input type if provided; otherwise, use auto-detection.
                    in_type = (
                        FILE_TYPE_ALIASES[args.input_type.lower()]
                        if args.input_type
                        else detected
                    )
                    # For Excel files, if no global excel options passed, ask interactively once per file.
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
                        output_dest,
                        conn,
                        excel_sheet,
                        excel_range,
                    )
        else:
            logging.error(
                f"Input path '{input_path}' is neither a file nor a directory."
            )


if __name__ == "__main__":
    main()
