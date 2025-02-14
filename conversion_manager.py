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

import logging
import os
from pathlib import Path
import queue
import threading
from typing import Dict, Tuple, List, Optional

from cli_interface import Settings
from converters import (
    BaseInputConnection,
    BaseOutputConnection,
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
        self.settings: Settings = settings
        self.settings.conversion_manager = self
        self.logger: logging.Logger = settings.logger

        # Store parsed CLI arguments by copying from Settings.
        self.input_path: Path = self.settings.input_path
        self.input_ext: str = self.settings.passed_input_format_ext
        self.output_ext: str = self.settings.passed_output_format_ext

        # Initialise the import and export queues.
        self.import_queue: queue.Queue[Tuple[Path, str, int]] = queue.Queue()
        self.export_queue: queue.Queue[Tuple[Path, str, int]] = queue.Queue()

        # Initialise import and export class variables.
        self.import_class: Optional[BaseInputConnection] = None
        self.export_class: Optional[BaseOutputConnection] = None

        # File or directory.
        self.file_or_dir: str = self.settings.file_or_dir
        # Subclasses are split by file or directory on instantiation.

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
        import_class_map = {
            "csv": CSVInput,
            "tsv": TsvInput,
            "txt": TxtInput,
            "json": JSONInput,
            "parquet": ParquetInput,
        }

        if self.input_ext == "excel":
            if self.output_ext in (".csv", ".tsv", ".txt"):
                return ExcelInputUntyped()
            elif self.output_ext in (".parquet", ".json", None):
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
        Create appropriate output class based on output format.
        
        Determines and instantiates the correct output class based on the desired
        output format.
        
        Returns:
            BaseOutputConnection: Instance of appropriate output class for the format
            
        Raises:
            ValueError: If output format is not supported
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
        Set the output file extension.
        
        If output extension is not already set in settings, prompts user to select
        one. Validates that output format differs from input format.
        """
        if not self.output_ext:
            self.output_ext = self.settings.prompt_for_output_format(self.input_ext)

    def _start_import_queue_handler(self):
        """
        Start asynchronous processing of the import queue.
        
        Creates and starts a thread to process files in the import queue.
        Files are imported one at a time to avoid overwhelming DuckDB.
        The queue handler continues running until all files are processed.
        """
        queue_handler = threading.Thread(
            target=self.import_queue.get, args=(self.input_path,)
        )
        queue_handler.start()
        # TODO: (13-Feb-2025) Check and finish queue handler.


class FileConversionManager(BaseConversionManager):
    """
    Manager for single file conversions.
    
    Handles the conversion of a single input file to the desired output format.
    
    Attributes:
        Inherits all attributes from BaseConversionManager
    """

    def __init__(self, settings: Settings):
        super().__init__(settings)
        self._check_input_extension()
        self.import_class = self._generate_import_class()
        # Add file to import queue and generate the import class.
        self.import_queue.put(
            (self.input_path, self.input_path.name, 1)
        )  # sham size to satisfy queue requirements
        

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
            self.input_ext = self.input_path.suffix.lower()

        if self.input_ext not in self.ALLOWED_FILE_EXTENSIONS:
            self.settings._exit_program(
                f"Invalid file extension: {self.input_ext}. Allowed: {self.ALLOWED_FILE_EXTENSIONS}"
            )

    def _start_file_import(self):
        """
        Start the asynchronous file import process.
        
        Spawns a new thread to call the import_file method of the input class with the input path.
        This allows file import to occur concurrently without blocking the main thread.
        """
        thread_import = threading.Thread(
            target=self.import_class.import_file, args=(self.input_path,)
        )
        thread_import.start()


class DirectoryConversionManager(BaseConversionManager):
    """
    Manager for directory conversions.
    
    Handles the conversion of multiple files in a directory.
    
    Attributes:
        Inherits all attributes from BaseConversionManager
        file_dictionary (List[Tuple[Path, str, int]]): List of files to convert
            Each tuple contains (file_path, file_name, file_size)
    """

    def __init__(self, settings: Settings):
        super().__init__(settings)
        self.file_dictionary: List[Tuple[Path, str, int]] = []
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

    def _generate_directory_info(self, input_ext: Optional[str] = None) -> Tuple[str, List[Tuple[Path, str, int]]]:
        """
        Generate information about files in the directory.
        
        Scans the directory for files matching the input extension or determines the most common
        file type if no extension is specified. Groups files by extension and validates compatibility.
        
        Args:
            input_ext (Optional[str]): File extension to filter by (e.g. '.csv', '.json')
            
        Returns:
            Tuple containing:
                - str: Most common file extension or specified input extension
                - List[Tuple[Path, str, int]]: List of tuples containing:
                    - Path: Full path to file
                    - str: File name
                    - int: File size in bytes
                
        Raises:
            SystemExit: If no compatible files are found in directory
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
            # TODO: (11-Feb-2025) Implement user prompt for input extension.

            # Return the majority extension and its corresponding file list directly
            return majority_extension, file_groups[majority_extension]


    def _get_file_info(self, entry) -> Dict:
        """
        Extract metadata from a directory entry.
        
        Gets file information including path, name, size and extension from
        a directory entry object.
        
        Args:
            entry (os.DirEntry): Directory entry object from os.scandir()
            
        Returns:
            Dict containing:
                - path (Path): Full path to file
                - name (str): File name
                - size (int): File size in bytes
                - ext (str): Lowercase file extension
        """
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
