#!/usr/bin/env python3
"""
Module for managing file paths and related operations.

This module contains a class for:

Checking if a path is a file or directory.
Checking the file extension and inferring the file type.
Generating output file paths based on the input file path and output type.
"""

from pathlib import Path
from typing import Dict, List, Optional
import re


class PathManager:
    """
    Class for managing file paths and related operations.
    """

    def __init__(self, input_path: Path, output_ext: str):
        self.input_path = input_path
        self.output_ext = output_ext

        self.input_name = self.input_path.name
        self.input_dir = self.input_path.parent

        self.file_or_dir = self._file_or_dir()
        self.file_ext = self._get_file_ext()

        self.output_name = self._generate_output_name()
        self.output_path = self._generate_output_path()

        # Mapping file extensions to input type names.
        self.ALLOWED_EXT_MAP = {
            ".csv": "csv",
            ".txt": "csv",  # treat .txt exactly like CSV
            ".json": "json",
            ".parquet": "parquet",
            ".parq": "parquet",
            ".pq": "parquet",
            ".xlsx": "excel",
        }

    # Private methods
    def _file_or_dir(self) -> str:
        """Check if the input path is a file or directory."""
        return "file" if self.input_path.is_file() else "directory"

    def _infer_file_type(self, file_path: Path) -> Optional[str]:
        """Infer the file type from the file extension."""
        if file_path.is_file():
            if file_path.suffix.lower() in self.ALLOWED_EXT_MAP:
                return self.ALLOWED_EXT_MAP.get(file_path.suffix.lower())
            else:
                return None
        else:
            return None

    def _infer_file_type_from_dir(self) -> Optional[str]:
        """Infer the file type from the directory."""
        file_types: Dict[str, int] = {}
        files = list(self.input_path.iterdir())
        for file_path in files:
            file_type = self._infer_file_type(file_path)
            if file_type:
                file_types[file_type] = file_types.get(file_type, 0) + 1
        if len(file_types) == 1:
            return list(file_types.keys())[0]
        elif len(file_types) > 1:
            # return most common file type
            return max(file_types, key=lambda k: file_types[k])
        else:
            return None

    def _get_file_ext(self) -> Optional[str]:
        """Get the file extension."""
        if self.file_or_dir == "file":
            return self.input_path.suffix.lower().replace(".", "")
        else:
            return self._infer_file_type_from_dir()

    def _search_dir_name_for_valid_alias(self) -> Optional[str]:
        """Search the directory name for the file extension."""
        for valid_ext in self.ALLOWED_EXT_MAP.values():
            if valid_ext in self.input_name:
                return valid_ext
        return None

    def _generate_output_name(self) -> Optional[str]:
        """Generate the output name."""
        if self.file_or_dir == "directory":
            if self._search_dir_name_for_valid_alias():
                # replace input alias with output alias
                return re.sub(
                    self._search_dir_name_for_valid_alias(),
                    self.output_ext,
                    self.input_name,
                )
            else:
                return f"{self.input_name} {self.output_ext}"
        else:
            return self.input_name

    def _generate_output_path(self) -> Path:
        if self.file_or_dir == "file":
            return self.input_path.with_suffix(f".{self.file_ext}")
        else:
            return self.input_dir / self.output_name
