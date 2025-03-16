#! /usr/bin/env python3
"""
Module for managing application settings and configuration.

This module contains the Settings class, which is responsible for managing and storing information about the files being processed, the input/output extensions, and other settings.
Additionally, the InputOutputFlags class is used to manage flags related to how the input/output extensions are determined (e.g., from CLI arguments, auto-detected, or prompted).
"""

from Make_It_Parquet.file_information import (
    create_file_info,
)
from Make_It_Parquet.user_interface.cli_parser import (
    CLIArgs,
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
        self.file_info = create_file_info(self.args.path)

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


class InputOutputFlags:
    """
    Flags class for managing flags.
    """

    def __init__(self):
        # Flags for input and output extensions.
        ## CLI flags.
        self.input_ext_supplied_from_cli: int = 0
        self.output_ext_supplied_from_cli: int = 0
        ## Auto-detected flag.
        self.input_ext_auto_detected: int = 0
        ## Prompt flags.
        self.output_ext_supplied_from_prompt: int = 0
        self.input_ext_supplied_from_prompt: int = 0

    def set_flags(
        self, environment: str, input_ext: str | None, output_ext: str | None
    ):
        """
        Set the flags based on the environment.
        """
        # TODO: 16-Feb-2025: implement functions to appropriately reset other flags.
        if environment == "cli":
            self.set_cli_flags(input_ext, output_ext)
        elif environment == "prompt":
            self.set_prompt_flags(input_ext, output_ext)
        elif environment == "auto":
            self.input_ext_auto_detected = 1
        else:
            raise ValueError(f"Invalid environment: {environment}")

    def set_cli_flags(self, input_ext: str | None, output_ext: str | None):
        """
        Set the CLI flags.
        """
        if input_ext:
            self.input_ext_supplied_from_cli = 1
            self.input_ext_auto_detected = 0
            self.input_ext_supplied_from_prompt = 0
        if output_ext:
            self.output_ext_supplied_from_cli = 1
            self.output_ext_supplied_from_prompt = 0

    def set_prompt_flags(self, input_ext: str | None, output_ext: str | None):
        """
        Set the prompt flags.
        """
        if input_ext:
            self.input_ext_supplied_from_prompt = 1
            self.input_ext_auto_detected = 0
            self.input_ext_supplied_from_cli = 0
        if output_ext:
            self.output_ext_supplied_from_prompt = 1
            self.output_ext_supplied_from_cli = 0
