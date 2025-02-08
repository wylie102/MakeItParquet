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

import logging
from pathlib import Path
import duckdb

# Import CLI-related functions and constants from cli.py
from cli import (
    FILE_TYPE_ALIASES,
    parse_cli_arguments,
    get_file_type_by_extension,
    prompt_excel_options,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def process_file(
    file: Path,
    in_type: str,
    out_type: str,
    output_dest: Path,
    conn: duckdb.DuckDBPyConnection,
    excel_sheet: str = None,
    excel_range: str = None,
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
    # Use the CLI parser from cli.py
    args = parse_cli_arguments()
    input_path = Path(args.input_path).resolve()
    output_dest = (
        Path(args.output_path).resolve() if args.output_path is not None else None
    )

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

    with duckdb.connect(database=":memory:") as conn:
        if input_path.is_file():
            if args.input_type:
                in_type = FILE_TYPE_ALIASES[args.input_type.lower()]
            else:
                detected = get_file_type_by_extension(input_path)
                if detected is None:
                    logging.error(
                        f"Could not automatically detect file type for {input_path}"
                    )
                    return
                else:
                    in_type = detected

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
            if output_dest is None:
                output_dest = input_path.parent / out_type
                output_dest.mkdir(parents=True, exist_ok=True)
                logging.info(
                    f"Output directory not provided. Using default: {output_dest.resolve()}"
                )
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
