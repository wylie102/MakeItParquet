#! /usr/bin/env python3
"""
Module for managing application settings and configuration.

This module contains the Settings class, which is responsible for managing and storing information about the files being processed, the input/output extensions, and other settings.
Additionally, the InputOutputFlags class is used to manage flags related to how the input/output extensions are determined (e.g., from CLI arguments, auto-detected, or prompted).
"""

from typing import Optional
from ..file_information import (
    create_file_info_dict,
    determine_file_or_dir,
)
import argparse
from .cli_parser import get_input_output_extensions
from .logger import Logger


class Settings:
    """
    Settings class for managing application configuration.
    """

    def __init__(self, args: argparse.Namespace) -> None:
        """
        Initialize the Settings object.
        """
        # CLI arguments.
        self.args = args
        # Logger.
        self.logger = Logger(self.args.log_level)
        # Input/output flags.
        self.input_output_flags = InputOutputFlags()

        # get input and output extensions from CLI arguments (if provided).
        self.input_ext, self.output_ext = get_input_output_extensions(
            self.args, self.input_output_flags
        )

        # File information.
        self.file_info_dict = create_file_info_dict(self.args.path)
        self.file_or_dir = determine_file_or_dir(self.file_info_dict["path"])
        # Initialise attributes for additional settings.
        self.excel_settings = None
        self.txt_settings = None


class InputOutputFlags:
    """
    Flags class for managing flags.
    """

    def __init__(self):
        # Flags for input and output extensions.
        ## CLI flags.
        self.input_ext_supplied_from_cli: bool = False
        self.output_ext_supplied_from_cli: bool = False
        ## Auto-detected flag.
        self.input_ext_auto_detected: bool = False
        ## Prompt flags.
        self.output_ext_supplied_from_prompt: bool = False
        self.input_ext_supplied_from_prompt: bool = False

    def set_flags(
        self, environment: str, input_ext: Optional[str], output_ext: Optional[str]
    ):
        """
        Set the flags based on the environment.
        """
        # TODO: 16-Feb-2025: implement functions to appropriately reset other flags.
        if environment == "cli":
            self.set_cli_flags(input_ext, output_ext)
        elif environment == "prompt":
            pass
        elif environment == "auto":
            pass
        else:
            raise ValueError(f"Invalid environment: {environment}")

    def set_cli_flags(self, input_ext: Optional[str], output_ext: Optional[str]):
        """
        Set the CLI flags.
        """
        self.input_ext_supplied_from_cli = input_ext is not None
        self.output_ext_supplied_from_cli = output_ext is not None
