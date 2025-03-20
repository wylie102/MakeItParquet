#!/usr/bin/env python3
"""
Module for managing file and directory conversions.

This module provides classes to manage the conversion of files and directories:
- FileConversionManager: Handles single file conversions
- DirectoryConversionManager: Handles directory conversions

The managers handle:
- Validating input/output formats.
- Checking a directory for the majority file type.
- Creating lists of files to be converted.
- Creating new names for folder/files based on input and output formats.
"""

import os
from collections import defaultdict
from typing import override


from Make_It_Parquet.extension_mapping import ALLOWED_FILE_EXTENSIONS
from Make_It_Parquet.file_information import FileInfo, create_file_info
from Make_It_Parquet.user_interface.prompts import prompt_for_input_extension

TYPE_CHECKING = False
if TYPE_CHECKING:
    from Make_It_Parquet.user_interface.settings import Settings
    from pathlib import Path


class FileManager:
    """
    Class for managing target files, also base class for directory conversion managers.
    """

    def __init__(self, settings: Settings):
        """
        Initialize the conversion manager.
        """
        # Attach Settings and input_path
        self.settings: Settings = settings
        self.input_path: Path = self.settings.file_info.file_path

        # Initialize input_ext and conversion file list
        self.input_ext: str | None = self.settings.master_input_ext
        self.conversion_file_list: list[FileInfo] = []

    def get_conversion_list(self) -> None:
        # Check input extension and generate conversion file list.
        self._get_input_extension()
        self._set_conversion_file_list()

    def _get_input_extension(self):
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
            self.settings.detected_input_ext = self.settings.file_info.file_ext

        # Validate input_ext
        if (
            self.input_ext not in ALLOWED_FILE_EXTENSIONS
        ):  # TODO consider changing to allow re-entering of input extension or checking the input/output flags, and/or performing a manual check on the input type
            self.settings.exit_program(
                f"Invalid file extension: {self.input_ext}. Allowed: {ALLOWED_FILE_EXTENSIONS}"
            )

    def _set_conversion_file_list(self):
        """sets conversion file dict list in same format as that used in directory manager."""
        self.conversion_file_list.append(self.settings.file_info)


class DirectoryManager(FileManager):
    """
    Manager for directory conversions.

    Handles the conversion of multiple files in a directory.

    Attributes:
        Inherits all attributes from BaseConversionManager
    """

    def __init__(self, settings: Settings):
        super().__init__(settings)

        # Initialize file group attributes
        self.extension_file_groups: dict[str, list[FileInfo]] = defaultdict(list)
        self.extension_counts: dict[str, int] = defaultdict(int)

        # get file groups
        self._get_extension_file_groups()

    @override
    def _get_input_extension(self):
        if not self.input_ext:
            self._detect_majority_extension()

    @override
    def _set_conversion_file_list(self):
        """Set input extension and file list. Also updates flags."""
        if self.input_ext:
            self.conversion_file_list: list[FileInfo] = self.extension_file_groups[
                self.input_ext
            ]
        self._order_files_by_size()

    def _order_files_by_size(self):
        """
        Sort the file dictionary by file size.

        Orders files from smallest to largest to optimize processing.
        """
        self.conversion_file_list.sort(
            key=lambda x: x.file_size,
            reverse=True,
        )

    def _get_extension_file_groups(self):
        """
        Generate information about files in the directory.
        Scans the directory for files matching the allowed extensions and groups them by extension.
        """
        info_dicts_list: list[FileInfo] = self._create_list_of_file_info_dicts()
        self._group_files_by_extension(info_dicts_list)
        self._exit_if_no_files()

    def _create_list_of_file_info_dicts(self) -> list[FileInfo]:
        """Create a list of file information dictionaries from the directory."""
        file_info_list: list[FileInfo] = []
        with os.scandir(self.input_path) as entries:
            for entry in entries:
                if entry.is_file():
                    file_info: FileInfo = create_file_info(entry)
                    file_info_list.append(file_info)
        return file_info_list

    def _group_files_by_extension(self, info_dicts_list: list[FileInfo]):
        """Group files by extension and count the number of files for each extension."""
        for file_info in info_dicts_list:
            ext: str = file_info.file_ext
            if ext in ALLOWED_FILE_EXTENSIONS:
                self.extension_file_groups[ext].append(file_info)
                self.extension_counts[ext] += 1

    def _exit_if_no_files(self):
        """Exit the program if no compatible file types are found."""
        if not self.extension_counts:
            self.settings.exit_program(
                "No compatible file types found", error_type="error"
            )

    def _detect_majority_extension(self):
        """determine the majority file extension in the directory."""

        # Create dict with counts of file formats ordered most to least
        self._sort_extensions_by_count()

        # If no majority file format then prompt user for input format
        if self._no_clear_majority_file_format():
            prompt_for_input_extension(self.settings)
        self.settings.detected_input_ext = list(self.extension_counts.keys())[0]

    def _sort_extensions_by_count(self):
        """Sort the extensions by the number of files in each group."""
        self.extension_counts = dict(
            sorted(
                self.extension_counts.items(), key=lambda item: item[1], reverse=True
            )
        )

    def _no_clear_majority_file_format(self):
        """Check if there are ambiguous file types and prompt for input if necessary."""
        if len(self.extension_counts) > 1:
            top_keys = list(self.extension_counts.keys())
            if self.extension_counts[top_keys[0]] == self.extension_counts[top_keys[1]]:
                self.settings.logger.error(  # TODO: check logger level
                    f"Ambiguous file types found {self.extension_counts}. Please specify which one to convert."
                )
                return True
        return False
