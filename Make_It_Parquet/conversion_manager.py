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
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Union

import converters as conv
from extension_mapping import (
    ALIAS_TO_EXTENSION_MAP,
    ALLOWED_FILE_EXTENSIONS,
    import_class_map,
)
from file_information import FileInfoDict, create_file_info_dict_from_scandir
from user_interface.interactive import prompt_for_input_format, prompt_for_output_format

if TYPE_CHECKING:
    from make_it_parquet import MakeItParquet


class BaseConversionManager:
    """
    Base class for file and directory conversion managers.

    Provides common functionality for managing file conversions including:
     management for imports/exports
    - Input/output format validation
    - Import/export class generation

    Attributes:
        ALLOWED_FILE_EXTENSIONS (set): Set of allowed file extensions (.csv, .json, etc)
        settings (Settings): Settings object containing CLI arguments and config
        logger (Logger): Logger instance for this manager
        input_path (Path): Path to input file/directory
        input_ext (str): Input file extension/format
        output_ext (str): Output file extension/format
        import_queue (Queue): Queue for files to be imported
        export_queue (Queue): Queue for files to be exported
        import_class (BaseInputConnection): Class instance for handling imports
        export_class (BaseOutputConnection): Class instance for handling exports
        file_or_dir (str): Whether input path is a 'file' or 'dir'
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
            self._return_standard_import_class
        else:
            self._return_excel_import_class

    def _return_standard_import_class(self):
        """Returns a non-excel import class."""
        if self.input_ext:
            return import_class_map[self.input_ext]

    def _return_excel_import_class(self):
        """Returns an excel import class."""
        # TODO: 21-Feb-2025: Write excel_utils.py functions/methods to laod excel extension.
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

    def _start_conversion_process(self):
        """
        Controller that runs through three phases:
        1. Bulk import until the output extension is received.
        2. Drain the export queue (export all imported files).
        3. Process the remaining files in a balanced 1:1 import/export manner.
        """
        # Assume self.import_queue is pre-populated in order (largest first)
        # and self.export_queue is initially empty.
        # Also, assume self.output_ext is None until set externally.

        if self._import_and_export_queues_empty():
            if self.output_ext is None:
                pass  # TODO: NEXT finish implementing this method.

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


class DirectoryConversionManager(BaseConversionManager):
    """
    Manager for directory conversions.

    Handles the conversion of multiple files in a directory.

    Attributes:
        Inherits all attributes from BaseConversionManager
        file_dictionary (List[Tuple[Path, str, int]]): List of files to convert
            Each tuple contains (file_path, file_name, file_size)
    """

    def __init__(self, mp: "MakeItParquet"):
        super().__init__(mp)

        self.file_groups = defaultdict(list)
        self.extension_counts = defaultdict(int)
        self.dir_file_list = []

        self.file_list: List[Dict[str, Union[Path, str, int, str]]] = []
        self._generate_file_dictionary()
        self._order_files_by_size()

    def _order_files_by_size(self):
        """
        Sort the file dictionary by file size.

        Orders files from smallest to largest to optimize processing.
        """
        # self.file_dictionary.sort(key=lambda x: x[2]) TODO 26-Feb-2025: fix this

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
                self.file_groups[ext].append(file_info)
                self.extension_counts[ext] += 1

    def _exit_if_no_files(self):
        """Exit the program if no compatible file types are found."""
        if not self.extension_counts:
            self.mp.exit_program("No compatible file types found", error_type="error")

    def _generate_file_dictionary(self):
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
            sorted(self.extension_counts.items(), key=lambda item: item[1], reverse=True)

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
        """Set the input extension and update the flags."""
        self.input_ext = list(self.extension_counts.keys())[0]
        self.file_list = self.file_groups[self.input_ext]

    def _auto_detect_majority_extension(self):
        """determine the majority file extension in the directory."""
        # Check for ambiguity: if more than one alias has the top count, ask the user to specify.
        self._sort_extensions_by_count()
        if not self._check_for_ambiguous_file_types():
            self._set_input_extension_and_file_list()
            self.settings.input_output_flags.set_flags("auto", self.input_ext, None)


# TODO: (13-Feb-2025) Implement below when needed. DO NOT DELETE.


def _replace_alias_in_string(self) -> str:
    pattern = re.compile(re.escape(self.input_ext), re.IGNORECASE)


def replacer(match: re.Match) -> str:
    orig = match.group()
    if orig.isupper():
        return self.input_ext.upper()
    elif orig.islower():
        return self.input_ext.lower()
    elif orig[0].isupper() and orig[1:].islower():
        return self.input_ext.capitalize()
    else:
        return self.input_ext

    result, count = pattern.subn(replacer, self.input_ext)
    if count == 0:
        result = f"{self.input_ext}"
    return result


def _generate_output_name(self) -> str:
    if self.input_ext and self.input_ext.lower() in self.input_path.name.lower():
        return self._replace_alias_in_string()
    else:
        return f"{self.input_path.name}_{self.output_ext}"


def generate_output_path(input_path: Path, output_ext: str) -> Path:
    return input_path.with_suffix(f".{output_ext}")


def get_conversion_params(
    input_path: Path, input_ext: str
) -> Tuple[List[Path], None, str]:
    return ([input_path], None, input_ext.lstrip("."))
