#!/usr/bin/env python3
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

from cli_parser import (
    parse_cli_arguments,
    FILE_TYPE_ALIASES,
    get_file_type_by_extension,
    prompt_excel_options,
)
from path_manager import create_path_manager

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

# Conversion lookup dictionary mapping (input_type, output_type) to conversion lambdas.
# These lambdas perform the conversion in one chained DuckDB statement.
CONVERSION_FUNCTIONS = {
    # CSV conversions
    ("csv", "parquet"): lambda conn, file, out_file, **kwargs: conn.read_csv(
        str(file.resolve())
    ).write_parquet(str(out_file.resolve())),
    ("csv", "json"): lambda conn, file, out_file, **kwargs: conn.read_csv(
        str(file.resolve())
    ).sql(
        f"COPY (SELECT * FROM read_csv_auto('{file.resolve()}')) TO '{out_file.resolve()}'"
    ),
    # JSON conversions
    ("json", "csv"): lambda conn, file, out_file, **kwargs: conn.read_json(
        str(file.resolve())
    ).write_csv(str(out_file.resolve())),
    ("json", "parquet"): lambda conn, file, out_file, **kwargs: conn.read_json(
        str(file.resolve())
    ).write_parquet(str(out_file.resolve())),
    # Parquet conversions
    ("parquet", "csv"): lambda conn, file, out_file, **kwargs: conn.from_parquet(
        str(file.resolve())
    ).write_csv(str(out_file.resolve())),
    ("parquet", "json"): lambda conn, file, out_file, **kwargs: conn.from_parquet(
        str(file.resolve())
    ).sql(
        f"COPY (SELECT * FROM read_parquet('{file.resolve()}')) TO '{out_file.resolve()}'"
    ),
    # Excel conversions (sheet and range parameters are optional)
    (
        "excel",
        "csv",
    ): lambda conn, file, out_file, sheet=None, range_=None, **kwargs: conn.sql(
        "SELECT * FROM read_xlsx('{}'{}{})".format(
            str(file.resolve()),
            (
                ", sheet = '{}'".format(
                    f"Sheet{sheet}" if isinstance(sheet, int) else sheet
                )
                if sheet is not None
                else ""
            ),
            (", range = '{}'".format(range_) if range_ is not None else ""),
        )
    ).write_csv(
        str(out_file.resolve())
    ),
    (
        "excel",
        "parquet",
    ): lambda conn, file, out_file, sheet=None, range_=None, **kwargs: conn.sql(
        "SELECT * FROM read_xlsx('{}'{}{})".format(
            str(file.resolve()),
            (
                ", sheet = '{}'".format(
                    f"Sheet{sheet}" if isinstance(sheet, int) else sheet
                )
                if sheet is not None
                else ""
            ),
            (", range = '{}'".format(range_) if range_ is not None else ""),
        )
    ).write_parquet(
        str(out_file.resolve())
    ),
    ("excel", "json"): lambda conn, file, out_file, sheet=None, range_=None, **kwargs: (
        lambda rel: conn.execute(f"COPY ({rel.sql_query()}) TO '{out_file.resolve()}'")(
            conn.sql(
                "SELECT * FROM read_xlsx('{}'{}{})".format(
                    str(file.resolve()),
                    (
                        ", sheet = '{}'".format(
                            f"Sheet{sheet}" if isinstance(sheet, int) else sheet
                        )
                        if sheet is not None
                        else ""
                    ),
                    (", range = '{}'".format(range_) if range_ is not None else ""),
                )
            )
        )
    ),
}


def process_file(
    file: Path,
    in_type: str,
    out_type: str,
    output_dest: Path,
    conn: duckdb.DuckDBPyConnection,
    excel_sheet=None,
    excel_range=None,
) -> None:
    """Process a single file conversion using the conversion lookup."""
    logging.info(f"Processing file: {file.name}")

    # Determine output file path
    if output_dest is not None:
        out_path = output_dest
        if out_path.is_dir() or out_path.suffix == "":
            out_path.mkdir(parents=True, exist_ok=True)
            output_file = out_path / (file.stem + EXTENSIONS[out_type])
        else:
            output_file = out_path
    else:
        # Use the file's own path for output if no output destination provided
        output_file = file.with_suffix(EXTENSIONS[out_type])

    conversion_key = (in_type, out_type)
    if conversion_key not in CONVERSION_FUNCTIONS:
        raise ValueError(f"Unsupported conversion: {in_type} to {out_type}")

    # For Excel input, pass the sheet and range parameters
    if in_type == "excel":
        CONVERSION_FUNCTIONS[conversion_key](
            conn, file, output_file, sheet=excel_sheet, range_=excel_range
        )
    else:
        CONVERSION_FUNCTIONS[conversion_key](conn, file, output_file)

    logging.info(f"Written {out_type} file: {output_file.resolve()}")


def main():
    args = parse_cli_arguments()
    input_path = Path(args.input_path).resolve()
    output_dest = (
        Path(args.output_path).resolve() if args.output_path is not None else None
    )

    # Determine desired output type
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
            # If no output destination is provided, use the DirectoryPathManager to generate one.
            if output_dest is None:
                pm = create_path_manager(input_path, out_type)
                output_dest = pm.output_path
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
