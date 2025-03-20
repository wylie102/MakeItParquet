#! /usr/bin/env python3
import logging
from pathlib import Path
from Make_It_Parquet.extension_mapping import ALIAS_TO_EXTENSION_MAP

TYPE_CHECKING = False
if TYPE_CHECKING:
    from Make_It_Parquet.user_interface.settings import Settings


def prompt_for_output_extension(input_ext: str, settings: Settings):
    """
    Prompt user for output format, ensuring it differs from input.

    Returns:
        str: Validated output extension
    """
    while True:
        # get output_extension
        output_ext = _get_extension()

        ## Output extension is valid but the same as input extension.
        if output_ext == input_ext:
            ## If input extension was automatically detected.
            _offer_chance_to_change_input_ext(input_ext, output_ext, settings)
            continue

        settings.supplied_output_ext = output_ext
        break


def _get_extension() -> str:
    while True:
        # Prompt for output format.
        output_format = _prompt_user_for_format()

        # Validate user input.
        ## If format is valid, return extension.
        output_ext = _check_format_return_extension(output_format)
        if not output_ext:
            continue
        return output_ext


def _prompt_user_for_format() -> str:
    output_format = (
        input(
            "Enter desired output format (csv, tsv, txt, parquet(pq), json(js), excel(ex)): "
        )
        .strip()
        .lower()
    )
    return output_format


def _check_format_return_extension(output_format: str) -> str | None:
    if output_format in ALIAS_TO_EXTENSION_MAP:
        output_ext = ALIAS_TO_EXTENSION_MAP[output_format]
        return output_ext
    else:
        logging.error(
            "Invalid output format. Please enter a valid output format (note: formats do not include the '.' ."
        )


def _offer_chance_to_change_input_ext(
    input_ext: str, output_ext: str, settings: Settings
) -> None:
    if settings.detected_input_ext and not settings.supplied_input_ext:
        method = "automatically detected"
    else:
        method = "user-provided"

        logging.error(
            f"""Conflict detected:\n
            Output extension '{output_ext}' is the same as the {method} input extension '{input_ext}'.\n
            Would you like to change the from the detected input format?"""
        )
        # Wishes to change from detected input extension.
        if _yes_no_bool():
            prompt_for_input_extension(
                settings
            )  # TODO: place checks/restarts on conversion logic after this is called. Need to check files with input_ext exist.
        # Wishes to keep detected input extension.
        else:
            logging.info("Please enter a different output format.")


def _yes_no_bool():
    while True:
        answer = input("(y/n): ").strip().lower()
        if answer == "y":
            return True
        elif answer == "n":
            return False
        else:
            logging.error("Invalid response. Please enter: 'y/n'")
            continue


def prompt_for_input_extension(settings: Settings):
    """
    Prompt user for input format.
    """
    input_ext: str = _get_extension()
    settings.update_input_ext(input_ext, "supplied")
    logging.info(f"Input extension set to: {input_ext}")


def get_delimiter(
    existing: str | None = None,
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
    return r"\t" if answer == "t" else ","


def prompt_for_txt_delimiter() -> dict[str, str]:
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


# def determine_excel_options(args: argparse.Namespace):
#     """
#     Set Excel-specific options from args or user prompts.
#     Sets self.sheet and self.range based on args or user input.
#     """
#     # If Excel options are not provided, prompt for them.
#     sheet = args.sheet
#     range = args.range
#     if sheet is None and range is None:
#         sheet, range = prompt_excel_options(input_path)
#
#
# def determine_txt_options(args: argparse.Namespace):
#     """
#     Set TXT-specific options from args or user prompts.
#     """
#     # For TXT output, if no delimiter provided, prompt for it.
#     delimiter = args.delimiter
#     if delimiter is None:
#         delimiter = prompt_for_txt_delimiter()
#
#     txt_kwargs = prompt_for_txt_delimiter()
#     args.delimiter = txt_kwargs["delimiter"]
