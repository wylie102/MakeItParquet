#!/usr/bin/env python3
"""
Module for managing file and directory paths for file conversions.

This module defines classes to:
    - Detect if a given path is a file or a directory.
    - Infer the file type (for processing and naming) from the file file_ext.
    - Generate an output file path (by changing the file_ext) or output directory name
      while preserving common capitalization conventions.
"""

import logging
import os
from pathlib import Path
import threading
from typing import Dict, Tuple, List, Optional

from cli_interface import Settings
from converters import _generate_import_class, _generate_export_class

logger = logging.getLogger(__name__)


class BaseConversionManager:
    """
    Base class for file and directory conversion managers.
    """

    def __init__(self, settings: Settings):
        # Attach settings object.
        self.settings = settings

        # Store parsed CLI arguments.
        self.input_path = self.settings.input_path
        self.input_ext = self.settings.passed_input_format_ext
        self.output_ext = self.settings.passed_output_format_ext
        self.file_or_dir = self.settings.file_or_dir

        # Initialize import and export classes.
        self.import_class = None
        self.export_class = None

    def _determine_output_extension(self):
        """
        Determine the output extension based on the passed output format or prompt the user for one.
        """
        if not self.output_ext:
            self.output_ext = self.settings.prompt_for_output_format()


class FileConversionManager(BaseConversionManager):
    """
    Class for managing file conversions.
    """

    def __init__(self, settings: Settings):
        super().__init__(settings)
        self._convert_single_file()

    def _convert_single_file(self):
        """
        Convert a single file.
        """
        # Determine the input extension.
        if not self.input_ext:
            self.input_ext = self.input_path.suffix.lower()

        # Initialize the import class.
        self.import_class = _generate_import_class(self)

        # Start file import in asynchronous multiprocessing thread.
        thread_import = threading.Thread(
            target=self.import_class.import_file, args=(self.input_path,)
        )
        thread_import.start()

        # Determine the output extension.
        self._determine_output_extension()

        # Initialize the export class and start file export.
        self.export_class = _generate_export_class(self)
        logger.info(f"Converting {self.input_path.name} to {self.output_ext}.")
        thread_import.join()
        self.export_class.export_file(self.input_path, self.output_ext)
        logger.info(
            f"File {self.input_path.name} converted to {self.output_ext}.\nConversion complete!"
        )


class DirectoryConversionManager(BaseConversionManager):
    """
    Class for managing directory conversions.
    """

    def __init__(self, settings: Settings):
        super().__init__(settings)
        self.file_dictionary: List[Tuple[Path, str, int]] = []
        self._order_files_by_size()
        self._convert_directory()

    def _order_files_by_size(self):
        self.file_dictionary.sort(key=lambda x: x[2])

    def _convert_directory(self):
        if self.input_ext:
            self.file_dictionary = self._generate_directory_info(self.input_ext)[1]
        else:
            self.input_ext, self.file_dictionary = self._generate_directory_info()

    def _generate_directory_info(  # TODO: (12-Feb-2025) Refactor this function.
        self, input_ext: Optional[str] = None
    ) -> Tuple[str, List[Tuple[Path, str, int]]]:
        """
        Determine and return the majority alias for files in the directory based on
        a mapping of file extensions (naming_ext_map).
        Saves dictionary of files of majority alias to list of files with name and size.
        """
        counts: Dict[str, int] = {}
        file_groups: Dict[str, List[Tuple[Path, str, int]]] = (
            {}
        )  # Store lists of matching files with name and size
        majority_extension = None  # The alias with the highest count so far.
        leader_count = 0  # The current highest alias count.
        second_highest = 0  # The runner-up count.

        with os.scandir(self.input_path) as entries:
            for entry in entries:
                if not entry.is_file():
                    continue

                # Get file path
                file_path = Path(entry.path)

                # Include only files with recognized extensions.
                file_ext = file_path.suffix.lower()
                if file_ext not in self.settings.EXTENSION_TO_ALIAS_MAP:
                    continue

                # Get file path, size, and name
                file_size = entry.stat().st_size  # Get file size
                file_name = entry.name

                # Add file to file_groups if it doesn't exist
                if file_ext not in file_groups:
                    file_groups[file_ext] = []
                file_groups[file_ext].append((file_path, file_name, file_size))

                counts[file_ext] = counts.get(file_ext, 0) + 1

                # Update majority_extension and runner-up counts
                if file_ext == majority_extension:
                    leader_count = counts[file_ext]
                elif counts[file_ext] > leader_count:
                    second_highest = leader_count
                    majority_extension = file_ext
                    leader_count = counts[file_ext]
                elif counts[file_ext] > second_highest:
                    second_highest = counts[file_ext]

                remaining = sum(counts.values()) - leader_count
                if (
                    leader_count > second_highest + remaining
                    and majority_extension is not None
                ):
                    return (
                        majority_extension,
                        file_groups[majority_extension],
                    )  # Early return

        if majority_extension is None:
            raise ValueError("No majority alias found")

        # Check for ambiguity: if more than one alias has the top count, ask the user to specify.
        leaders = [
            file_ext for file_ext, count in counts.items() if count == leader_count
        ]
        if len(leaders) > 1:
            raise ValueError(
                f"Ambiguous file types found {leaders}. Please specify which one to convert."
            )  # TODO: (11-Feb-2025) Implement user prompt.

        # Return the majority extension and its corresponding file list directly
        return majority_extension, file_groups[majority_extension]

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
