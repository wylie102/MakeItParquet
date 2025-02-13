#!/usr/bin/env python3
"""
CLI Interface module for DuckConvert.

Provides functions to parse command-line arguments, handle file type aliases,
and common user interactions (prompts for output type, Excel options, and TXT delimiters).
"""

import argparse
from pathlib import Path
import stat
from typing import Optional, Tuple, Dict
import logging
import queue
from logging.handlers import QueueHandler, QueueListener

# --- CLI Argument Parsing and File Type Helpers ---


def get_delimiter(
    existing: Optional[str] = None,
    prompt_text: str = "Enter delimiter (t for tab, c for comma): ",
) -> str:
    if existing is not None:
        return existing
    answer = input(prompt_text).strip().lower()
    return "\t" if answer == "t" else ","


def prompt_for_output_format() -> str:
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


#############################
# Settings Class
#############################


class Settings:
    """
    Settings class for DuckConvert Conversion Manager.
    """

    # Alias to extension map.
    ALIAS_TO_EXTENSION_MAP: Dict[str, str] = {
        "csv": ".csv",
        "txt": ".txt",
        "tsv": ".tsv",
        "json": ".json",
        "js": ".json",
        "parquet": ".parquet",
        "pq": ".parquet",
        "excel": ".xlsx",
        "ex": ".xlsx",
        "xls": ".xlsx",
    }

    # Reverse the alias to extension map.
    EXTENSION_TO_ALIAS_MAP: Dict[str, str] = {
        v: k for k, v in ALIAS_TO_EXTENSION_MAP.items()
    }


    def __init__(self) -> None:
        """
        Initialize the Settings object.
        """
        self.args = self._parse_cli_arguments()
        self._configure_logging()
        self.passed_input_format_ext, self.passed_output_format_ext = (
            self._validate_format_inputs()
        )
        self.input_path, self.file_or_dir = self._resolve_path_and_determine_type()

    def _parse_cli_arguments(self) -> argparse.Namespace:
        # Parse CLI arguments.
        parser = argparse.ArgumentParser(
            description="DuckConvert: Convert data files using DuckDB"
        )
        # Input path.
        parser.add_argument(
            "input_path", help="Path to the input file or directory", type=Path
        )
        # Output path.
        parser.add_argument(
            "-op", "--output_path", help="Specify output file path", type=Path
        )
        # Input format.
        parser.add_argument(
            "-i", "--input_format", help="Specify input file format", type=str
        )
        # Output format.
        parser.add_argument(
            "-o", "--output_format", help="Specify output file format", type=str
        )
        # Excel sheet.
        parser.add_argument(
            "-s", "--sheet", help="Excel sheet (name or number)", type=str
        )
        # Excel range.
        parser.add_argument("-c", "--range", help="Excel range (e.g., A2:E7)", type=str)
        # Log level.
        parser.add_argument(
            "--log-level",
            help="Set the logging level (e.g., DEBUG, INFO, WARNING)",
            default="INFO",
        )
        # Parse arguments and create argparse.Namespace object (args).
        args = parser.parse_args()
        return args

    def _configure_logging(self):
        """
        Configure async logging based on arguments.
        """
        self.log_queue = queue.Queue()

        # Create a single logger for the program.
        self.logger = logging.getLogger("duckconvert")
        numeric_level = getattr(logging, self.args.log_level.upper(), None)
        if not isinstance(numeric_level, int):
            numeric_level = logging.INFO

        # Console handler with module-aware formatting.
        self.logger.setLevel(numeric_level)
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(formatter)

        # Use the queue handler to make logging non-blocking.
        self.queue_handler = QueueHandler(self.log_queue)
        self.logger.addHandler(self.queue_handler)

        # Queue listener processes logs in a background thread.
        self.listener = QueueListener(self.log_queue, console_handler)
        self.listener.start()

    def _stop_logging(self):
        """
        Stops the logging listener before exiting.
        """
        self.listener.stop()

    def _resolve_path_and_determine_type(self) -> Tuple[Path, str]:
        # Resolve path.
        resolved_path = self.args.input_path.resolve()

        # Check if path exists and is a file or directory.
        try:
            stat_result = resolved_path.stat()
        except OSError as e:
            logging.error(f"Could not access '{resolved_path}': {e}")
            raise ValueError("Invalid input path")
        
        # Return path and type.
        if stat.S_ISREG(stat_result.st_mode):
            return resolved_path, "file"
        elif stat.S_ISDIR(stat_result.st_mode):
            return resolved_path, "dir"
        # Raise error if path is neither a file nor a directory.
        else:
            logging.error(
                f"Input path '{resolved_path}' is neither a file nor a directory."
            )
            raise ValueError("Invalid input path")

    def _validate_format_inputs(self) -> Tuple[Optional[str], Optional[str]]:
        # Validate input format.
        if self.args.input_format:
            try:
                input_ext = self.ALIAS_TO_EXTENSION_MAP[self.args.input_format]
            except KeyError:
                logging.error(f"Unsupported input format: {self.args.input_format}")
                raise ValueError("Invalid input type specified")
        else:
            input_ext = None

        # Validate output format.   
        if self.args.output_format:
            try:
                output_ext = self.ALIAS_TO_EXTENSION_MAP[self.args.output_format]
            except KeyError:
                logging.error(f"Unsupported output format: {self.args.output_format}")
                raise ValueError("Invalid output type specified")
        else:
            output_ext = None

        # Raise error if input and output formats are the same.
        if input_ext and output_ext:
            if input_ext == output_ext:
                raise ValueError("Input and output formats cannot be the same")
        # Return input and output extensions.
        return input_ext, output_ext

    def prompt_for_output_format(self, input_ext: str) -> str:
        # Prompt for output format.
        # Validate user input.
        while True:
            user_input = input("Enter desired output format (csv, tsv, txt, parquet(pq), json(js), excel(ex)): ").strip().lower()
            if user_input in self.ALIAS_TO_EXTENSION_MAP and self.ALIAS_TO_EXTENSION_MAP[user_input] != input_ext:
                output_ext = self.ALIAS_TO_EXTENSION_MAP[user_input]
                break
            elif user_input in self.ALIAS_TO_EXTENSION_MAP and self.ALIAS_TO_EXTENSION_MAP[user_input] == input_ext:
                print("Input and output formats cannot be the same")
            else:
                print("Invalid output format. Please try again.")
        return output_ext
            

    def _determine_excel_options(self):
        # If Excel options are not provided, prompt for them.
        self.sheet = self.args.sheet
        self.range = self.args.range
        if self.sheet is None and self.range is None:
            self.sheet, self.range = prompt_excel_options(self.input_path)

    def _determine_txt_options(self):
        # For TXT output, if no delimiter provided, prompt for it.
        self.delimiter = self.args.delimiter
        if self.delimiter is None:
            self.delimiter = prompt_for_txt_delimiter()

            txt_kwargs = prompt_for_txt_delimiter()
            self.args.delimiter = txt_kwargs["delimiter"]

    def _exit_program(self, message, error_type="error"):
        """
        Handles program exit with logging and stopping the logging system.
        """
        if error_type == "error":
            self.logger.error(message)
        elif error_type == "exception":
            self.logger.exception(message)

        self.settings.stop_logging()  # Ensure logging is stopped before exit
        exit(1)