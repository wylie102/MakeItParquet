#! /usr/bin/env python3

import argparse
from typing import Tuple, Optional, TYPE_CHECKING
import logging
from pathlib import Path
from extension_mapping import ALIAS_TO_EXTENSION_MAP

if TYPE_CHECKING:
    from user_interface.settings import InputOutputFlags


def parse_cli_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed CLI arguments
    """
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
    parser.add_argument("-s", "--sheet", help="Excel sheet (name or number)", type=str)
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


def get_input_output_extensions(
    args: argparse.Namespace, input_output_flags: "InputOutputFlags"
) -> Tuple[Optional[str], Optional[str]]:
    """
    Validate input and output format arguments that were passed in directly from the command line.
    Does not interact with arguments generated by prompts.
    If no (valid) arguments are provided input_ext and output_ext will be None.

    Returns:
        Tuple[Optional[str], Optional[str]]: Validated input and output extensions
    """
    input_ext = _validate_format(args.input_format)
    output_ext = _validate_format(args.output_format)
    # If supplied extensions are the same, set them to None.
    # Input will now be detected automatically.
    # Output will be supplied via user prompt as part of the main program.
    # TODO: 16-Feb-2025: Check if this is the best approach.
    if _input_output_extensions_same(input_ext, output_ext):
        input_ext = None
        output_ext = None
    # Set flags.
    input_output_flags.set_flags("cli", input_ext, output_ext)
    # Return input and output extensions.
    return input_ext, output_ext


def _check_format_supported(format: str) -> bool:
    """
    Check if a format is supported.
    """
    if format in ALIAS_TO_EXTENSION_MAP:
        return True
    else:
        logging.error(f"Received invalid format: {format}")
        return False


def _map_format_to_extension(format: str) -> str:
    """
    Map a format to an extension.
    """
    return ALIAS_TO_EXTENSION_MAP[format]


def _validate_format(format: Optional[str]) -> Optional[str]:
    """
    Validate a format string by checking its existence, support, and then mapping it to an extension.
    """
    # Check if format is provided.
    if not format:
        return None

    # Check if supported
    if not _check_format_supported(format):
        return None

    # Map format to extension
    return _map_format_to_extension(format)


def _input_output_extensions_same(
    input_ext: Optional[str], output_ext: Optional[str]
) -> bool:
    """
    Check if input and output extensions are the same.
    """
    # Raise error if input and output formats are the same.
    if input_ext and output_ext:
        if input_ext == output_ext:
            # log error
            logging.error(
                "Input and output extensions cannot be the same. Input extension will be automatically detected, please specify output extension."
            )
            return True
        else:
            return False
    else:
        return False
