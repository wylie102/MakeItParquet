#! /usr/bin/env python3
"""
Module for managing application settings and configuration.

This module contains the Settings class, which is responsible for managing and storing information about the files being processed, the input/output extensions, and other settings.
Additionally, the InputOutputFlags class is used to manage flags related to how the input/output extensions are determined (e.g., from CLI arguments, auto-detected, or prompted).
"""

import logging

from Make_It_Parquet.extension_mapping import ALLOWED_FILE_EXTENSIONS
from Make_It_Parquet.file_information import (
    FileInfo,
    create_file_info,
)
from .cli_parser import (
    CLIArgs,
    get_input_output_extensions,
)
from .logger import Logger


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

        # get input and output extensions from CLI arguments (if provided).
        self.supplied_input_ext: str | None
        self.supplied_output_ext: str | None
        self.supplied_input_ext, self.supplied_output_ext = get_input_output_extensions(
            self.args.input_format, self.args.output_format
        )
        self.detected_input_ext: str | None = None

        # File information.
        if self.args.input_path:
            self.file_info: FileInfo = create_file_info(self.args.input_path)

        # # Initialise attributes for additional settings.
        # self.excel_settings = None
        # self.txt_settings = None

    def exit_program(self, message: str, error_type: str | None = "info") -> None:
        """
        Exit program with logging and cleanup.

        Args:
            message: Error message to log
            error_type: Type of error ('error' or 'exception')
        """
        message = f"{message} Exiting program."
        if error_type == "error":
            self.logger.error(message)
        elif error_type == "exception":
            self.logger.exception(message)
        else:
            self.logger.info(message)

        self.logger.stop_logging()
        exit(1)  # TODO: look at deleting any converted files etc. if needed.

    def set_input_ext(self, input_ext: str, method: str) -> None:
        if input_ext in ALLOWED_FILE_EXTENSIONS:
            if method == "detected":
                self.detected_input_ext = input_ext
                self.supplied_input_ext = None
            elif method == "supplied":
                self.supplied_input_ext = input_ext
                self.detected_input_ext = None
            else:
                logging.error("Unable to update input ext, method is invalid")
        else:
            logging.error("Unable to update input ext, supplied extension is invalid.")

    def set_output_ext(self, output_ext: str) -> None:
        if output_ext in ALLOWED_FILE_EXTENSIONS:
            self.supplied_output_ext = output_ext

    @property
    def master_input_ext(self) -> str | None:
        if self.supplied_input_ext and not self.detected_input_ext:
            return self.supplied_input_ext
        elif self.detected_input_ext and not self.supplied_input_ext:
            return self.detected_input_ext
        elif not self.supplied_input_ext and not self.detected_input_ext:
            return None
        elif self.supplied_input_ext and self.detected_input_ext:
            if self.supplied_input_ext == self.detected_input_ext:
                return self.supplied_input_ext
            elif self.supplied_input_ext != self.detected_input_ext:
                self.exit_program(
                    f"Conflict between detected input extension: '{
                        self.detected_input_ext
                    }' and supplied input extension: '{self.supplied_output_ext}'."
                )

    @property
    def master_output_ext(self) -> str | None:
        return self.supplied_output_ext
