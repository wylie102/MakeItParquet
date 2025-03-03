#!/usr/bin/env python3
"""
Module for managing file and directory conversions.

This module provides classes to manage the conversion of files and directories:
- BaseConversionManager: Base class with common functionality
- FileConversionManager: Handles single file conversions
- DirectoryConversionManager: Handles directory conversions

The managers handle:
- Validating input/output formats
- Managing conversion queues
- Coordinating import/export operations
"""

import os
import queue
import re
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Union

import converters as conv
from extension_mapping import (
    ALIAS_TO_EXTENSION_MAP,
    ALLOWED_FILE_EXTENSIONS,
    import_class_map,
)
from file_information import create_file_info_dict_from_scandir
from user_interface.interactive import prompt_for_input_format, prompt_for_output_format

if TYPE_CHECKING:
    from make_it_parquet import MakeItParquet


class BaseConversionManager:
    """
    Base class for file and directory conversion managers.

    Attributes:
        settings: Settings instance
        file_info_dict: Dictionary containing file information
        input_path: Path to the input file or directory
        input_ext: Extension of the input file
        output_ext: Extension of the output file
        file_or_dir: Whether the input is a file or directory
        import_queue: Queue for importing files
        export_queue: Queue for exporting files
        import_queue_status_flag: Flag indicating whether the import queue is empty
        export_queue_status_flag: Flag indicating whether the export queue is empty
    """

    def __init__(self, mp: "MakeItParquet"):
        """
        Initialize the conversion manager.
        """
        # Attach MakeItParquet instance to the conversion manager.
        self.mp = mp
        # Attach Settings instance to the conversion manager.
        self.settings = mp.settings

        # Store file information.
        self.file_info_dict = self.settings.file_info_dict

        # Store parsed CLI arguments by copying from Settings.
        self.input_path: Path = self.file_info_dict["path"]
        self.input_ext: Optional[str] = self.mp.settings.input_ext
        self.output_ext: Optional[str] = self.settings.output_ext

        # File or directory.
        self.file_or_dir: str = self.settings.file_or_dir

        # Initialize queues for import/export.
        self.import_queue = queue.Queue()
        self.export_queue = queue.Queue()

        # initialize flags for import/export queues.
        self.import_queue_status_flag_flag = 0  # zero = empty
        self.export_queue_status_flag = 0  # zero = empty

    def _generate_import_class(self):
        """
        Create appropriate input class based on file extension.

        Determines and instantiates the correct input class based on the input
        file extension. Handles special cases for Excel files based on output format.

        Returns:
            BaseInputConnection: Instance of appropriate input class for the file type

        Raises:
            ValueError: If input extension is not supported
        """
        if not self.input_ext == ".xlsx":
            self._return_standard_import_class()
        else:
            self._return_excel_import_class()

    def _return_standard_import_class(self):
        """Returns a non-excel import class."""
        if self.input_ext:
            return import_class_map[self.input_ext]

    def _return_excel_import_class(self):
        """Returns an excel import class."""
        # TODO: 21-Feb-2025: Write excel_utils.py functions/methods to load excel extension.
        pass  # TODO: 21-Feb-2025: Refactor excel_utils.py

    def _update_import_queue_status_flag(self):
        """Checks import queue, if empty, sets flag to 0, else sets flag to 1."""
        if self.import_queue.empty():
            self.import_queue_status_flag = 0
        else:
            self.import_queue_status_flag = 1

    def _update_export_queue_status_flag(self):
        """Checks export queue, if empty, sets flag to 0, else sets flag to 1."""
        if self.export_queue.empty():
            self.export_queue_status_flag = 0
        else:
            self.export_queue_status_flag = 1

    def _import_and_export_queues_empty(self):
        """Returns True if both import and export queues are empty, else returns false."""
        if (
            self.import_queue_status_flag_flag == 0
            and self.export_queue_status_flag == 0
        ):
            return True
        else:
            return False

    def _determine_output_extension(self):
        """
        Set the output file extension.

        If output extension is not already set in settings, prompts user to select
        one. Validates that output format differs from input format.
        """
        if not self.output_ext:
            prompt_for_output_format(self.settings, ALIAS_TO_EXTENSION_MAP)

    def _generate_export_class(self):
        """
        Create appropriate output class based on output format.

        Determines and instantiates the correct output class based on the desired
        output format.

        Returns:
            BaseOutputConnection: Instance of appropriate output class for the format

        Raises:
            ValueError: If output format is not supported
        """
        export_class_map = {
            "csv": conv.CSVOutput,
            "tsv": conv.TsvOutput,
            "txt": conv.TxtOutput,
            "json": conv.JSONOutput,
            "parquet": conv.ParquetOutput,
            "excel": conv.ExcelOutput,
        }

        if self.output_ext in export_class_map:
            return export_class_map[self.output_ext]()

        raise ValueError(f"Unsupported output extension: {self.output_ext}")


class FileConversionManager(BaseConversionManager):
    """
    Manager for single file conversions.

    Handles the conversion of a single input file to the desired output format.

    Attributes:
        Inherits all attributes from BaseConversionManager
    """

    def __init__(self, mp: "MakeItParquet"):
        super().__init__(mp)

        # Check input extension and generate import class.
        self._check_input_extension()
        self.import_class = self._generate_import_class()
        self._start_conversion_process()

        # Determine the output extension.
        self._determine_output_extension()

        # Generate the export class.
        self.export_class = self._generate_export_class()

    def _check_input_extension(self):
        """
        Validate the input file extension.

        Checks if the input file extension is supported. If the input extension is not
        set, it is inferred from the file path's suffix. If the extension is not allowed,
        the program exits after logging an error.

        Raises:
            SystemExit: If file extension is not in the ALLOWED_FILE_EXTENSIONS.
        """
        # Determine the input extension.
        if not self.input_ext:
            self.input_ext = self.file_info_dict["file_extension"]

        if (
            self.input_ext not in ALLOWED_FILE_EXTENSIONS
        ):  # TODO consider changing to allow re-entering of input extension or checking the input/output flags, and/or performing a manual check on the input type
            self.mp.exit_program(
                f"Invalid file extension: {self.input_ext}. Allowed: {ALLOWED_FILE_EXTENSIONS}"
            )

    def _start_conversion_process(self):
        pass  # TODO: 26/02/2025 implement this method.


class DirectoryConversionManager(BaseConversionManager):
    """
    Manager for directory conversions.

    Handles the conversion of multiple files in a directory.

    Attributes:
        Inherits all attributes from BaseConversionManager
    """

    def __init__(self, mp: "MakeItParquet"):
        super().__init__(mp)

        # initialize directory conversion attributes.
        self.dir_file_list = []
        self.extension_file_groups = defaultdict(list)
        self.extension_counts = defaultdict(int)
        self.conversion_file_list: List[Dict[str, Union[Path, str, int, str]]] = []

        # creates and orders self.conversion_file_list.
        self._generate_extension_file_groups()  # creates dir_file_list, groups files into extension_file_groups, totals extension_counts.
        self._detect_majority_extension()  # detects majority extension, sets self.input_ext, sets conversion_file_list, updates flags.
        self._order_files_by_size()

    def _order_files_by_size(self):
        """
        Sort the file dictionary by file size.

        Orders files from smallest to largest to optimize processing.
        """
        self.conversion_file_list.sort(key=lambda x: x["file_size"], reverse=True)

    def _create_list_of_file_info_dicts(self):
        """Create a list of file information dictionaries from the directory."""
        with os.scandir(self.input_path) as entries:
            for entry in entries:
                if entry.is_file():
                    file_info = create_file_info_dict_from_scandir(entry)
                    self.dir_file_list.append(file_info)

    def _group_files_by_extension(self):
        """Group files by extension and count the number of files for each extension."""
        for file_info in self.dir_file_list:
            ext = file_info["file_extension"]
            if ext in ALLOWED_FILE_EXTENSIONS:
                self.extension_file_groups[ext].append(file_info)
                self.extension_counts[ext] += 1

    def _exit_if_no_files(self):
        """Exit the program if no compatible file types are found."""
        if not self.extension_counts:
            self.mp.exit_program("No compatible file types found", error_type="error")

    def _generate_extension_file_groups(self):
        """
        Generate information about files in the directory.
        Scans the directory for files matching the allowed extensions and groups them by extension.
        """
        self._create_list_of_file_info_dicts()
        self._group_files_by_extension()
        self._exit_if_no_files()

    def _sort_extensions_by_count(self):
        """Sort the extensions by the number of files in each group."""
        self.extension_counts = dict(
            sorted(
                self.extension_counts.items(), key=lambda item: item[1], reverse=True
            )
        )

    def _check_for_ambiguous_file_types(self):
        """Check if there are ambiguous file types and prompt for input if necessary."""
        if len(self.extension_counts) > 1:
            top_keys = list(self.extension_counts.keys())
            if self.extension_counts[top_keys[0]] == self.extension_counts[top_keys[1]]:
                self.settings.logger.error(
                    f"Ambiguous file types found {self.extension_counts}. Please specify which one to convert."
                )
                prompt_for_input_format(ALIAS_TO_EXTENSION_MAP)
                return True
        return False

    def _set_input_extension_and_file_list(self):
        """Set input extension and file list. Also updates flags."""
        self.input_ext = list(self.extension_counts.keys())[0]
        self.settings.input_output_flags.set_flags("auto", self.input_ext, None)
        self.conversion_file_list = self.extension_file_groups[self.input_ext]

    def _detect_majority_extension(self):
        """determine the majority file extension in the directory."""
        # Check for ambiguity: if more than one alias has the top count, ask the user to specify.
        self._sort_extensions_by_count()
        if not self._check_for_ambiguous_file_types():
            self._set_input_extension_and_file_list()

    def _start_conversion_process(self):
        """Starts the conversion process by adding file paths to the import queue"""
        if self._import_and_export_queues_empty():
            if self.output_ext is None:
                self._place_file_paths_in_queue()

    def _place_file_paths_in_queue(self):
        """Gets path of each file in self.conversion_file_list and places in queue"""
        for file_info in self.conversion_file_list:
            self.import_queue.put(file_info["path"])


# TODO: (13-Feb-2025) Implement below when needed. DO NOT DELETE.


# def replacer(match: re.Match) -> str: TODO: find out where this originally went.
def replacer(input_ext: str, match: re.Match) -> str:
    """
    Replace the matched string in the input extension with the correct case.

    Args:
    match: re.Match
        The matched string.

    Returns:
    str:
        The matched string with the correct case.
    """
    orig = match.group()
    if orig.isupper():
        return input_ext.upper()
    elif orig.islower():
        return input_ext.lower()
    elif orig[0].isupper() and orig[1:].islower():
        return input_ext.capitalize()
    else:
        return input_ext


def replace_alias_in_string(self) -> str:
    """
    Replace the matched string in the input extension with the correct case.

    This method takes in the input extension and returns the same extension
    with the correct case. If the extension is not found in the string, the
    original string is returned.

    Args:
        self (str): The input extension.

    Returns:
        str: The matched string with the correct case.
    """
    pattern = re.compile(re.escape(self.input_ext), re.IGNORECASE)
    result, count = pattern.subn(replacer, self.input_ext)
    if count == 0:
        result = f"{self.input_ext}"
    return result


def generate_output_name(self) -> str:
    """
    Generate a name for the output file.

    If the input extension is present in the input file name (case-insensitive),
    the output name is generated by replacing the input extension with the
    correct case. Otherwise, the output name is generated by appending the
    output extension to the input file name.

    Returns:
        str: The generated output name.
    """
    if self.input_ext and self.input_ext.lower() in self.input_path.name.lower():
        return self._replace_alias_in_string()
    else:
        return f"{self.input_path.name}_{self.output_ext}"


def generate_output_path(input_path: Path, output_ext: str) -> Path:
    """
    Generate a path for the output file.

    Args:
        input_path (Path): The path to the input file.
        output_ext (str): The output file extension.

    Returns:
        Path: The path to the output file.
    """
    return input_path.with_suffix(f".{output_ext}")
