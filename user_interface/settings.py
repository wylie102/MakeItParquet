#! /usr/bin/env python3

from typing import Optional
from file_information import resolve_path, file_or_dir, generate_file_stat
from user_interface.interactive import prompt_excel_options, prompt_for_txt_delimiter
import argparse
from user_interface.cli_parser import get_input_output_extensions


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
        # Input/output flags.
        self.input_output_flags = InputOutputFlags()
        # Validate input and output format arguments.
        self.input_ext, self.output_ext = get_input_output_extensions(self.args, self.input_output_flags)
        # File information.
        self.path = resolve_path(self.args.input_path)
        self.stat = generate_file_stat(self.path)
        self.file_or_dir = file_or_dir(self.stat)
        
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

    def set_flags(self, environment: str, input_ext: Optional[str], output_ext: Optional[str]):
        """
        Set the flags based on the environment.
        """
        # TODO: 16-Feb-2025: implement functions to appropriately reset other flags.
        if environment == "cli":
            self.set_cli_flags(input_ext, output_ext)
        elif environment == "prompt":
            self.set_prompt_flags(input_ext, output_ext)
        elif environment == "auto":
            self.set_auto_flags(input_ext, output_ext)
        else:
            raise ValueError(f"Invalid environment: {environment}")

    def set_cli_flags(self, input_ext: Optional[str], output_ext: Optional[str]): 
        """
        Set the CLI flags.
        """
        self.input_ext_supplied_from_cli = input_ext is not None
        self.output_ext_supplied_from_cli = output_ext is not None
        


def determine_excel_options(self):
    """
    Set Excel-specific options from args or user prompts.
    Sets self.sheet and self.range based on args or user input.
    """
    # If Excel options are not provided, prompt for them.
    self.sheet = self.args.sheet
    self.range = self.args.range
    if self.sheet is None and self.range is None:
        self.sheet, self.range = prompt_excel_options(self.input_path)


def determine_txt_options(self):
    """
    Set TXT-specific options from args or user prompts.
    """
    # For TXT output, if no delimiter provided, prompt for it.
    self.delimiter = self.args.delimiter
    if self.delimiter is None:
        self.delimiter = prompt_for_txt_delimiter()

    txt_kwargs = prompt_for_txt_delimiter()
    self.args.delimiter = txt_kwargs["delimiter"]
