# !/usr/bin/env python3
# /// script
# dependencies = [
#     "duckdb",
# ]
# ///
"""
DuckConvert: A conversion tool to convert between popular data storage file types
(CSV/TXT, JSON, Parquet, Excel) using DuckDB's Python API.

If the input type is not provided, the file extension is used to auto-detect.
For Excel files, if no sheet or range has been provided via the command line,
the tool will prompt for options.
"""

import logging

from cli_interface import Settings

from conversion_manager import FileConversionManager, DirectoryConversionManager

# Mapping of output file extensions for naming purposes.
# EXTENSIONS = {
#     "csv": ".csv",
#     "tsv": ".tsv",
#     "txt": ".txt",
#     "parquet": ".parquet",
#     "json": ".json",
#     "excel": ".xlsx",
# }


# def process_file(
#     file: Path,
#     in_type: str,
#     out_type: str,
#     conn: duckdb.DuckDBPyConnection,
#     excel_sheet=None,
#     excel_range=None,
#     output_dir: Optional[Path] = None,
#     **kwargs,
# ) -> None:
#     """
#     Process a single file conversion:
#       1. Construct the output file (ensuring no overwrite).
#       2. Instantiate the conversion instance using the factory.
#       3. Run the conversion.
#     """
#     logging.info(f"Processing file: {file.name}")

#     # Determine the output file path.
#     if output_dir is not None:
#         output_file = output_dir / (file.stem + EXTENSIONS[out_type])
#     else:
#         output_file = file.with_suffix(EXTENSIONS[out_type])

#     # Avoid overwriting an existing file.
#     if output_file.exists():
#         base = output_file.stem
#         ext = output_file.suffix
#         counter = 1
#         candidate = output_file.parent / f"{base}_{counter}{ext}"
#         while candidate.exists():
#             counter += 1
#             candidate = output_file.parent / f"{base}_{counter}{ext}"
#         output_file = candidate

#     try:
#         # Instantiate the proper conversion instance.
#         conversion = get_conversion_instance(
#             conn,
#             src=file,
#             dst=output_file,
#             input_format=in_type,
#             output_format=out_type,
#             **(
#                 {"sheet": excel_sheet, "range_": excel_range}
#                 if in_type == "excel"
#                 else {}
#             ),
#             **kwargs,
#         )
#         conversion.run()
#         logging.info(f"Converted to {output_file.name}")
#     except Exception as e:
#         logging.error(f"Error processing file {file.name}: {e}")


# def convert(args: Settings) -> None:
#     # 'args' is now a Settings instance.

#     extra_kwargs = {"delimiter": args.args.delimiter} if args.out_type == "txt" else {}

#     # Open an in-memory DuckDB connection.
#     with duckdb.connect(database=":memory:") as conn:
#         # Determine if the Excel extension is needed.
#         ExcelUtils.excel_check_and_load(conn, args.out_type, args, files_to_process)
#         for f in files_to_process:
#             detected = get_file_type_by_extension(f)
#             if detected is None:
#                 logging.info(f"Skipping file with unsupported extension: {f.name}")
#                 continue
#             # Use the CLI input type if provided; otherwise, use auto-detected type.
#             in_type = (
#                 FILE_TYPE_ALIASES[args.args.input_type.lower()]
#                 if args.args.input_type
#                 else detected
#             )
#             process_file(
#                 f,
#                 in_type,
#                 args.out_type,
#                 conn,
#                 args.args.sheet,
#                 args.args.range,
#                 output_dir=output_dest,
#                 **extra_kwargs,
#             )

def create_conversion_manager(settings: Settings):
    """
    Factory function to create a conversion manager based on the input path.
    """
    if settings.file_or_dir == "file":
        return FileConversionManager(settings)
    else:
        return DirectoryConversionManager(settings)


def main():
    try:
        settings = Settings()
        create_conversion_manager(settings)
        
    except Exception as e:
        logging.error(f"Conversion failed: {e}")


if __name__ == "__main__":
    main()
