#! /usr/bin/env python3
"""
Module for managing application settings and configuration.

This module contains the Settings class, which is responsible for managing and storing information about the files being processed, the input/output extensions, and other settings.
Additionally, the InputOutputFlags class is used to manage flags related to how the input/output extensions are determined (e.g., from CLI arguments, auto-detected, or prompted).
"""

from Make_It_Parquet.file_information import (
    FileInfo,
    create_file_info,
)
from Make_It_Parquet.user_interface.cli_parser import (
    CLIArgs,
    InputOutputFlags,
    get_input_output_extensions,
)
from Make_It_Parquet.user_interface.logger import Logger


class Settings:
    """
    Settings class for managing application configuration.
    """

    def __init__(self, args: CLIArgs) -> None:
        """
        Initialize the Settings object.
        """
        # CLI arguments.
        self.args: CLIArgs = args
        # Logger.
        self.logger: Logger = Logger(self.args.log_level)
        # Input/output flags.
        self.input_output_flags: InputOutputFlags = InputOutputFlags()

        # get input and output extensions from CLI arguments (if provided).
        self.input_ext: str | None
        self.output_ext: str | None
        self.input_ext, self.output_ext = get_input_output_extensions(
            self.args, self.input_output_flags
        )

        # File information.
        self.file_info: FileInfo = create_file_info(self.args.input_path)

        # # Initialise attributes for additional settings.
        # self.excel_settings = None
        # self.txt_settings = None

    def exit_program(self, message: str, error_type: str | None = "error") -> None:
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

        self.logger.stop_logging()
        exit(1)
