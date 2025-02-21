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
from typing import TYPE_CHECKING, Dict, Optional, Set, Union
from user_interface.interactive import prompt_for_output_format

import converters as conv
from extension_mapping import (
    ALIAS_TO_EXTENSION_MAP,
    ALLOWED_FILE_EXTENSIONS,
    import_class_map,
)
from file_information import (
    create_file_info_dict,
    determine_file_or_dir,
    generate_file_stat,
    resolve_path,
)

if TYPE_CHECKING:
    from make_it_parquet import MakeItParquet


class BaseConversionManager:
    """
    Base class for file and directory conversion managers.

    Provides common functionality for managing file conversions including:
    - Queue management for imports/exports
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
        self.file_dictionary: Dict[str, Dict[str, Union[Path, str, int, str]]] = {}
        self._generate_file_dictionary()
        self._order_files_by_size()
        self._convert_directory()

    def _order_files_by_size(self):
        """
        Sort the file dictionary by file size.

        Orders files from smallest to largest to optimize processing.
        """
        self.file_dictionary.sort(key=lambda x: x[2])

    def _convert_directory(self):
        """
        Initialize directory conversion.

        Determines input format if not specified and populates file dictionary.
        """
        if self.input_ext:
            self.file_dictionary = self._generate_directory_info(self.input_ext)[1]
        else:
            self.input_ext, self.file_dictionary = self._generate_directory_info()

    def _generate_file_dictionary(self):
        """
        Generate information about files in the directory.

        Scans the directory for files matching the allowed extensions and groups them by extension.
        At the same time, counts the number of files for each extension.

        Args:
            input_ext (Optional[str]): File extension to filter by (e.g. '.csv', '.json'). If provided,
                                       then only files with that extension are considered.

        Returns:
            Tuple containing:
                - str: The chosen or majority extension.
                - List[Tuple[Path, str, int]]: A list of tuples for the files of the majority extension.
                  Each tuple contains the full file path, the file name, and the file size in bytes.
                - Dict[str, int]: A dictionary mapping each extension to the count of files for that extension.

        Raises:
            SystemExit: If no compatible files are found in the directory.
        """
        allowed_extensions: Set[str] = self.ALLOWED_FILE_EXTENSIONS.copy()
        file_groups = defaultdict(list)
        extension_counts = defaultdict(int)

        with os.scandir(self.input_path) as entries:
            for entry in entries:
                if entry.is_file():
                    file_info = self._get_file_info(
                        entry
                    )  # TODO 19-Feb-2025: Look at using functions from file_information for this.
                    ext = file_info["ext"]
                    if ext in allowed_extensions:
                        file_groups[ext].append(file_info)
                        extension_counts[ext] += 1

        # Check if any files have been found
        if not extension_counts:
            return self.settings._exit_program(
                "No compatible file types found", error_type="error"
            )

        # Determine the majority extension (if a tie occurs, ask the user to specify the extension).
        # Order the extensions by count in descending order.
        sorted_extensions = sorted(
            extension_counts, key=lambda x: extension_counts[x], reverse=True
        )

        # Check for ambiguity: if more than one alias has the top count, ask the user to specify.
        if len(sorted_extensions) > 1:
            if (
                extension_counts[sorted_extensions[0]]
                == extension_counts[sorted_extensions[1]]
            ):
                self.logger.error(
                    f"Ambiguous file types found {sorted_extensions}. Please specify which one to convert."
                )
                self.settings._prompt_for_input_format()
            else:
                self.input_ext = sorted_extensions[0]
                self.settings.input_ext_auto_detected_flag = 1

        self.file_dictionary: Dict[str, Dict[str, Union[Path, str, int, str]]] = (
            file_groups[self.input_ext]
        )

    def _try_using_file_information_functions(self):
        resolve_path(self.input_path)
        generate_file_stat(self.input_path)
        determine_file_or_dir(self.input_path)
        create_file_info_dict(self.input_path)


# TODO: (13-Feb-2025) Implement below when needed. DO NOT DELETE.
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
# def _replace_alias_in_string(self) -> str:
#     pattern = re.compile(re.escape(self.input_ext), re.IGNORECASE)

# def replacer(match: re.Match) -> str:
#     orig = match.group()
#     if orig.isupper():
#             return self.input_ext.upper()
#     elif orig.islower():
#             return self.input_ext.lower()
#     elif orig[0].isupper() and orig[1:].islower():
#         return self.input_ext.capitalize()
#     else:
#         return self.input_ext

#     result, count = pattern.subn(replacer, self.input_ext)
#     if count == 0:
#         result = f"{self.input_ext}"
#     return result

# def _generate_output_name(self) -> str:
#     if self.input_ext and self.input_ext.lower() in self.input_path.name.lower():
#         return self._replace_alias_in_string()
#     else:
#         return f"{self.input_path.name}_{self.output_ext}"


# def generate_output_path(input_path: Path, output_ext: str) -> Path:
#     return input_path.with_suffix(f".{output_ext}")

# def get_conversion_params(input_path: Path, input_ext: str) -> Tuple[List[Path], None, str]:
#     return ([input_path], None, input_ext.lstrip("."))
