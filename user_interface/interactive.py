#! /usr/bin/env python3
import logging
from typing import Optional, TYPE_CHECKING
from pathlib import Path

if TYPE_CHECKING:
    from user_interface.settings import Settings  # Import only for MyPy

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

def prompt_for_output_format(settings: 'Settings', ALIAS_TO_EXTENSION_MAP: dict):
    """
    Prompt user for output format, ensuring it differs from input.

    Returns:
        str: Validated output extension
    """
    # Retrieve the input extension from the conversion manager.
    input_ext = settings.input_ext

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
            output_format in ALIAS_TO_EXTENSION_MAP
            and ALIAS_TO_EXTENSION_MAP[output_format] != input_ext
        ):
            output_ext = ALIAS_TO_EXTENSION_MAP[output_format]
            logging.info(f"Output format set to: {output_ext}")
            break
        ## Output extension is valid but the same as input extension.
        elif (
            output_format in ALIAS_TO_EXTENSION_MAP
            and ALIAS_TO_EXTENSION_MAP[output_format] == input_ext
        ):
            output_ext = ALIAS_TO_EXTENSION_MAP[output_format]
            ## Input extension was automatically detected.
            if (
                settings.input_output_flags.input_ext_cli_flag == 0
                and settings.input_output_flags.input_ext_auto_detected_flag == 1
            ):
                logging.error(
                    f"Conflict detected: Output format '{output_ext}' is the same as the auto-detected input format '{input_ext}'."
                )
                input_answer = input(
                    "The input format was auto-detected. Would you like to change the from the detected input format? (y/n): "
                )
                # Wishes to change from detected input extension.
                if input_answer.lower() == "y":
                    prompt_for_input_format()
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
                    prompt_for_input_format(ALIAS_TO_EXTENSION_MAP)
                # Wishes to keep passed in input extension.
                else:
                    logging.info("Please enter a different output format.")
                    continue
        else:
            logging.error(
            "Invalid output format. Please enter a valid output format (note: formats do not include the '.' ."
        )
        continue

def prompt_for_input_format(ALIAS_TO_EXTENSION_MAP: dict):
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
            if input_format in ALIAS_TO_EXTENSION_MAP:
                input_ext = ALIAS_TO_EXTENSION_MAP[input_format]
                logging.info(f"Input extension set to: {input_ext}")
                break
            else:
                logging.error(
                    "Invalid input format. Please enter a valid input format (note: formats do not include the '.' ."
                )
                continue
