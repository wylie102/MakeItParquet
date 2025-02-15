#! /usr/bin/env python3

from user_interface.cli_parser import parse_cli_arguments, validate_format_inputs
from file_information import resolve_path, file_or_dir, generate_file_stat
from user_interface.interactive import prompt_excel_options, prompt_for_txt_delimiter


class Settings:
    """
    Settings class for managing application configuration.
    """

    def __init__(self) -> None:
        """
        Initialize the Settings object.
        """
        # Input/output flags.
        self.input_output_flags = InputOutputFlags()
        # CLI arguments.
        self.args = parse_cli_arguments(self.input_output_flags)
        # Validate input and output format arguments.
        self.input_ext, self.output_ext = validate_format_inputs(self)
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
        self.input_ext_cli_flag: int = 0
        self.output_ext_cli_flag: int = 0
        ## Auto-detected flag.
        self.input_ext_auto_detected_flag: int = 0
        ## Prompt flags
        self.output_ext_from_prompt_flag: int = 0
        self.input_ext_from_prompt_flag: int = 0


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
