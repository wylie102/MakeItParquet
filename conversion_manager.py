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
import queue
import threading
from typing import Dict, Tuple, List, Optional

from cli_interface import Settings
from converters import (
    CSVInput,
    TsvInput,
    TxtInput,
    JSONInput,
    ParquetInput,
    ExcelInputUntyped,
    ExcelInputTyped,
    CSVOutput,
    TsvOutput,
    TxtOutput,
    JSONOutput,
    ParquetOutput,
    ExcelOutput,
)

logger = logging.getLogger(__name__)


class BaseConversionManager:
    """
    Base class for file and directory conversion managers.
    """

    # Allowed file extensions.
    ALLOWED_FILE_EXTENSIONS = {".csv",
        ".json",
        ".parquet",
        ".txt"}
        

    def __init__(self, settings: Settings):
        """
        Initialize the conversion manager.
        """
        # Attach settings object.
        self.settings = settings
        self.logger = settings.logger

        # Store parsed CLI arguments.
        self.input_path = self.settings.input_path
        self.input_ext = self.settings.passed_input_format_ext
        self.output_ext = self.settings.passed_output_format_ext

        # Initialize the import and export queues.
        self.import_queue: queue.Queue[Tuple[Path, str, int]] = queue.Queue()
        self.export_queue: queue.Queue[Tuple[Path, str, int]] = queue.Queue()

        # Initialize import and export class variables.
        self.import_class = None
        self.export_class = None

        # File or directory.
        self.file_or_dir = self.settings.file_or_dir
        # Subclasses are split by file or directory on instantiation.

    def _generate_import_class(self):
        """
        Generate the appropriate input class based on the input path and file extension.
        """
        import_class_map = {
            "csv": CSVInput,
            "tsv": TsvInput,
            "txt": TxtInput,
            "json": JSONInput,
            "parquet": ParquetInput,
        }

        if self.input_ext == "excel":
            if self.output_ext in ("csv", "tsv", "txt"):
                return ExcelInputUntyped()
            elif self.output_ext in ("parquet", "json", None):
                return ExcelInputTyped()
            else:
                raise ValueError(
                    f"Unsupported output extension for Excel: {self.output_ext}"
                )

        if self.input_ext in import_class_map:
            return import_class_map[self.input_ext]()

        raise ValueError(f"Unsupported input extension: {self.input_ext}")

    def _generate_export_class(self):
        """
        Generate the appropriate output class based on the output path and file extension.
        """
        export_class_map = {
            "csv": CSVOutput,
            "tsv": TsvOutput,
            "txt": TxtOutput,
            "json": JSONOutput,
            "parquet": ParquetOutput,
            "excel": ExcelOutput,
        }

        if self.output_ext in export_class_map:
            return export_class_map[self.output_ext]()

        raise ValueError(f"Unsupported output extension: {self.output_ext}")

    def _determine_output_extension(self):
        """
        Determine the output extension based on the passed output format or prompt the user for one.
        """
        if not self.output_ext:
            self.output_ext = self.settings.prompt_for_output_format(self.input_ext)

    def _start_import_queue_handler(self):
        """
        Start the import queue handler.
        """
        queue_handler = threading.Thread(
            target=self.import_queue.get, args=(self.input_path,)
        )
        queue_handler.start()
        # TODO: (13-Feb-2025) Check and finish queue handler.


class FileConversionManager(BaseConversionManager):
    """
    Class for managing file conversions.
    """

    def __init__(self, settings: Settings):
        super().__init__(settings)
        self.check_input_extension()

        # Add file to import queue and generate the import class.
        self.import_queue.put(
            (self.input_path, self.input_path.name, 1)
        )  # sham size to satisfy queue requirements
        self.import_class = self._generate_import_class()

        # Determine the output extension.
        self._determine_output_extension()

        # Generate the export class.
        self.export_class = self._generate_export_class()

    def check_input_extension(self):
        """
        Check the input extension and exit if it is not supported.
        """
        try:
            # Determine the input extension
            if not self.input_ext:
                self.input_ext = self.input_path.suffix.lower()

            if self.input_ext not in self.ALLOWED_FILE_EXTENSIONS:
                self.settings._exit_program(
                    f"Invalid file extension: {self.input_ext}. Allowed: {self.ALLOWED_FILE_EXTENSIONS}"
                )

        except Exception as e:
            self.settings._exit_program(
                f"Unexpected error in check: {e}", error_type="exception"
            )

    def _start_file_import(self):
        """
        Start the file import in an asynchronous multiprocessing thread.
        """
        thread_import = threading.Thread(
            target=self.import_class.import_file, args=(self.input_path,)
        )
        thread_import.start()


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

    def _generate_directory_info(              # TODO: (12-Feb-2025) Refactor this function.
        self, input_ext: Optional[str] = None
    ) -> Tuple[str, List[Tuple[Path, str, int]]]:
        """
        Determine and return the majority alias for files in the directory based on
        a mapping of file extensions (naming_ext_map).
        Saves dictionary of files of majority alias to list of files with name and size.
        """
        
        with os.scandir(self.input_path) as entries:
            remaining_extensions = self.ALLOWED_FILE_EXTENSIONS.copy()
            file_stat_list = [self._get_file_info(entry) for entry in entries if entry.is_file() and entry.suffix.lower() in remaining_extensions]
            file_stat_list_dict = 
            

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
            return self.settings._exit_program(
                "No compatible file types found", error_type="error"
            )
        else:
            # Check for ambiguity: if more than one alias has the top count, ask the user to specify.
            leaders = [
                file_ext for file_ext, count in counts.items() if count == leader_count
            ]
            if len(leaders) > 1:
                logger.error(
                    f"Ambiguous file types found {leaders}. Please specify which one to convert."
                )
                majority_extension = self.settings._prompt_for_input_format()
                return (majority_extension, file_groups[majority_extension])
            # TODO: (11-Feb-2025) Implement user prompt.

            # Return the majority extension and its corresponding file list directly
            return majority_extension, file_groups[majority_extension]


    def _get_file_info(self, entry):
        """Returns a dictionary containing full file information."""
        stat_info = entry.stat()  # Cached metadata
        path_obj = Path(entry.path)  # Convert string path to Path object

            return {
                "path": path_obj,
                "name": entry.name,
                "size": stat_info.st_size,
                "ext": path_obj.suffix.lower(),
            }   

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
