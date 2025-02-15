#!/usr/bin/env python3
"""
CLI Interface module for Make-it-Parquet!.

This module provides:
- CLI argument parsing and validation
- Settings management for the application
- File type alias handling
- User interaction functions for prompts
- Logging configuration
"""

import argparse
from pathlib import Path
import stat
from typing import Optional, Tuple, Dict
import logging
import queue
from logging.handlers import QueueHandler, QueueListener

from conversion_manager import BaseConversionManager

# --- CLI Argument Parsing and File Type Helpers ---


def get_delimiter(
    existing: Optional[str] = None,
    prompt_text: str = "Enter delimiter (t for tab, c for comma): ",
) -> str:
    """
    Get delimiter for text file conversion.

    Args:
        existing: Optional existing delimiter to use
        prompt_text: Text to display when prompting user

    Returns:
        str: Tab or comma delimiter based on user input
    """
    if existing is not None:
        return existing
    answer = input(prompt_text).strip().lower()
    return "\t" if answer == "t" else ","


def prompt_for_output_format() -> str:
    """
    Prompt user for desired output format.

    Returns:
        str: Lowercase output format string (csv, parquet, etc)
    """
    return (
        input("Enter desired output format (csv, parquet, json, excel, tsv, txt): ")
        .strip()
        .lower()
    )


def prompt_for_txt_delimiter() -> dict:
    """
    Prompt user for TXT file delimiter preference.

    Returns:
        dict: Dictionary with 'delimiter' key containing tab or comma
    """
    answer = (
        input("For TXT export, choose t for tab separated or c for comma separated: ")
        .strip()
        .lower()
    )
    return {"delimiter": "\t" if answer == "t" else ","}


def prompt_excel_options(file: Path):
    """
    Prompt user for Excel-specific options.

    Args:
        file: Path to Excel file

    Returns:
        Tuple[Optional[str], Optional[str]]: Sheet name/number and cell range
    """
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
    Settings class for managing application configuration.

    Handles:
    - CLI argument parsing
    - File path resolution
    - Format validation
    - Logging setup
    - User prompts

    Attributes:
        ALIAS_TO_EXTENSION_MAP (Dict[str, str]): Maps format aliases to file extensions
        EXTENSION_TO_ALIAS_MAP (Dict[str, str]): Reverse mapping of aliases
        args (argparse.Namespace): Parsed CLI arguments
        logger (Logger): Application logger
        input_path (Path): Validated input file/directory path
        file_or_dir (str): Whether input is 'file' or 'dir'
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
        # Parse CLI arguments.
        self.args: argparse.Namespace = self._parse_cli_arguments()

        # Logging and conversion manager.
        self.logger: Optional[logging.Logger] = None
        self.log_queue: Optional[queue.Queue] = None
        self.conversion_manager: Optional[BaseConversionManager] = None

        # Flags for input and output extensions.
        ## CLI flags.
        self.input_ext_cli_flag: int = 0
        self.output_ext_cli_flag: int = 0
        ## Auto-detected flag.
        self.input_ext_auto_detected_flag: int = 0
        ## Prompt flags
        self.output_ext_from_prompt_flag: int = 0
        self.input_ext_from_prompt_flag: int = 0

        # Set optional attributes for additional settings.
        self.sheet = None
        self.range = None
        self.delimiter = None

        # Configure logging.
        self._configure_logging()

        # Validate and store input and output extensions.
        self.passed_input_format_ext, self.passed_output_format_ext = (
            self._validate_format_inputs()
        )

        # Resolve input path and determine if it's a file or directory.
        self.input_path, self.file_or_dir = self._resolve_path_and_determine_type()

    def _parse_cli_arguments(self) -> argparse.Namespace:
        """
        Parse command line arguments.

        Returns:
            argparse.Namespace: Parsed CLI arguments
        """
        # Parse CLI arguments.
        parser = argparse.ArgumentParser(
            description="Make-it-Parquet!: Conversion of data files powered by DuckDB"
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
        Configure asynchronous logging system.

        Sets up queue-based logging with console output.
        """
        self.log_queue = queue.Queue()

        # Create a single logger for the program.
        self.logger = logging.getLogger("Make-it-Parquet!")
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
        Stop the logging system cleanly.

        Ensures logging queue is processed before shutdown.
        """
        self.listener.stop()

    def _resolve_path_and_determine_type(self) -> Tuple[Path, str]:
        """
        Resolve input path and determine if it's a file or directory.

        Returns:
            Tuple[Path, str]: (resolved_path, 'file' or 'dir')

        Raises:
            ValueError: If path is invalid or inaccessible
        """
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
        """
        Validate input and output format arguments that were passed in directly from the command line.
        Does not interact with arguments generated by prompts.
        If no (valid) arguments are provided input_ext and output_ext will be None.

        Returns:
            Tuple[Optional[str], Optional[str]]: Validated input and output extensions
        """
        # Validate input format.
        if self.args.input_format:
            if self.args.input_format in self.ALIAS_TO_EXTENSION_MAP:
                input_ext = self.ALIAS_TO_EXTENSION_MAP[self.args.input_format]
                self.input_ext_cli_flag = 1
                logging.info(f"Input extension set to: {input_ext}")

            else:
                logging.error(
                    f"Received invalid input format: {self.args.input_format}\n Input format will be automatically detected."
                )
                input_ext = None
                self.input_ext_cli_flag = 0
        else:
            input_ext = None
            self.input_ext_cli_flag = 0
        # Validate output format.
        if self.args.output_format:
            if self.args.output_format in self.ALIAS_TO_EXTENSION_MAP:
                output_ext = self.ALIAS_TO_EXTENSION_MAP[self.args.output_format]
                logging.info(f"Output extension set to: {output_ext}")
            else:
                logging.error(
                    f"Received invalid output format: {self.args.output_format}"
                )
                output_ext = None
        else:
            output_ext = None

        # Raise error if input and output formats are the same.
        if input_ext and output_ext:
            if input_ext == output_ext:
                logging.error(
                    "Input and output formats cannot be the same. Input format will be automatically detected."
                )
                input_ext = None
                output_ext = None
                self.input_ext_cli_flag = 0
                self.output_ext_cli_flag = 0

        # Return input and output extensions.
        return input_ext, output_ext

    def prompt_for_output_format(self):
        """
        Prompt user for output format, ensuring it differs from input.

        Returns:
            str: Validated output extension
        """
        # Retrieve the input extension from the conversion manager.
        input_ext = self.conversion_manager.input_ext

        while True:
            # Prompt for output format.
            output_format = (
                input(
                    "Enter desired output format (csv, tsv, txt, parquet(pq), json(js), excel(ex)): "
                )
                .strip()
                .lower()
            )
            # Validate user input.
            ## Output extension is valid and different from input extension.
            if (
                output_format in self.ALIAS_TO_EXTENSION_MAP
                and self.ALIAS_TO_EXTENSION_MAP[output_format] != input_ext
            ):
                output_ext = self.ALIAS_TO_EXTENSION_MAP[output_format]
                self.conversion_manager.output_ext = output_ext
                self.output_format_from_prompt_flag = 1
                logging.info(f"Output format set to: {output_ext}")
                break
            ## Output extension is valid but the same as input extension.
            elif (
                output_format in self.ALIAS_TO_EXTENSION_MAP
                and self.ALIAS_TO_EXTENSION_MAP[output_format] == input_ext
            ):
                output_ext = self.ALIAS_TO_EXTENSION_MAP[output_format]
                ## Input extension was automatically detected.
                if (
                    self.input_ext_cli_flag == 0
                    and self.input_ext_auto_detected_flag == 1
                ):
                    logging.error(
                        f"Conflict detected: Output format '{output_ext}' is the same as the auto-detected input format '{input_ext}'."
                    )
                    input_answer = input(
                        "The input format was auto-detected. Would you like to change the from the detected input format? (y/n): "
                    )
                    # Wishes to change from detected input extension.
                    if input_answer.lower() == "y":
                        self._prompt_for_input_format()
                    # Wishes to keep detected input extension.
                    else:
                        logging.info("Please enter a different output format.")
                        continue
                ## Input extension was passed in directly.
                else:
                    logging.error(
                        f"Conflict detected: Output format '{output_ext}' is the same as the user-provided input format '{input_ext}'."
                    )
                    input_answer = input(
                        "The input format was provided directly. Would you like to change the input format? (y/n): "
                    )
                    # Wishes to change from passed in input extension.
                    if input_answer.lower() == "y":
                        self._prompt_for_input_format()
                    # Wishes to keep passed in input extension.
                    else:
                        logging.info("Please enter a different output format.")
                        continue
            else:
                logging.error(
                    "Invalid output format. Please enter a valid output format (note: formats do not include the '.' ."
                )
                continue

    def _prompt_for_input_format(
        self,
    ):
        """
        Prompt user for input format.
        """
        while True:
            input_format = (
                input(
                    "Enter desired input format (csv, tsv, txt, parquet(pq), json(js), excel(ex)): "
                )
                .strip()
                .lower()
            )
            if input_format in self.ALIAS_TO_EXTENSION_MAP:
                input_ext = self.ALIAS_TO_EXTENSION_MAP[input_format]
                self.input_ext_from_prompt_flag = 1
                logging.info(f"Input extension set to: {input_ext}")
                self.conversion_manager.input_ext = input_ext
                break
            else:
                logging.error(
                    "Invalid input format. Please enter a valid input format (note: formats do not include the '.' ."
                )
                continue

    def _determine_excel_options(self):
        """
        Set Excel-specific options from args or user prompts.

        Sets self.sheet and self.range based on args or user input.
        """
        # If Excel options are not provided, prompt for them.
        self.sheet = self.args.sheet
        self.range = self.args.range
        if self.sheet is None and self.range is None:
            self.sheet, self.range = prompt_excel_options(self.input_path)

    def _determine_txt_options(self):
        """
        Set TXT-specific options from args or user prompts.

        Sets self.delimiter based on args or user input.
        """
        # For TXT output, if no delimiter provided, prompt for it.
        self.delimiter = self.args.delimiter
        if self.delimiter is None:
            self.delimiter = prompt_for_txt_delimiter()

            txt_kwargs = prompt_for_txt_delimiter()
            self.args.delimiter = txt_kwargs["delimiter"]

    def _exit_program(self, message, error_type="error"):
        """
        Exit program with logging and cleanup.

        Args:
            message: Error message to log
            error_type: Type of error ('error' or 'exception')
        """
        if error_type == "error":
            self.logger.error(message)
        elif error_type == "exception":
            self.logger.exception(message)

        self._stop_logging()  # Ensure logging is stopped before exit
        exit(1)
