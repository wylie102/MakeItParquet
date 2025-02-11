#!/usr/bin/env python3
"""
Module for managing file and directory paths for file conversions.

This module defines classes to:
    - Detect if a given path is a file or a directory.
    - Infer the file type (for processing and for naming) from the file extension.
    - Generate an output file path (by changing the extension) or output directory name
      (by replacing the file-type "alias" in the directory name) while preserving
      common capitalization conventions.

Special handling is provided for .txt files: they are processed like CSV files,
but when naming directories the literal "txt" is used.
"""

import re
from pathlib import Path
from typing import Optional, Dict
from collections import Counter


# Added helper function for path normalization.
def normalize_path(path: Path) -> str:
    """Return the canonical string representation of a file path."""
    return str(path.resolve())


class BasePathManager:
    """
    Base class for path management.
    """

    # Mapping for processing: treat .txt as CSV so that conversion code works uniformly.
    ALLOWED_EXT_MAP: Dict[str, str] = {
        ".csv": "csv",
        ".txt": "csv",  # processing: treat txt like csv
        ".json": "json",
        ".parquet": "parquet",
        ".parq": "parquet",
        ".pq": "parquet",
        ".xlsx": "excel",
    }

    # Mapping for naming: keep the literal extension for naming purposes.
    NAMING_EXT_MAP: Dict[str, str] = {
        ".csv": "csv",
        ".txt": "txt",  # naming: keep txt as txt
        ".tsv": "tsv",  # added mapping for TSV files
        ".json": "json",
        ".parquet": "parquet",
        ".parq": "parquet",
        ".pq": "parquet",
        ".xlsx": "excel",
    }

    def __init__(self, input_path: Path, output_ext: str):
        self.input_path: Path = input_path
        self.output_ext: str = output_ext

        self.input_name: str = self.input_path.name
        self.input_dir: Path = self.input_path.parent
        self.input_alias: Optional[str] = None  # default input_alias attribute

    @staticmethod
    def _replace_alias_in_string(text: str, old_alias: str, new_alias: str) -> str:
        """
        Replace all occurrences of old_alias in text with new_alias while preserving capitalization.
        If no occurrence is found, appends the new alias to the text.
        """
        pattern = re.compile(re.escape(old_alias), re.IGNORECASE)

        def replacer(match: re.Match) -> str:
            orig = match.group()
            if orig.isupper():
                return new_alias.upper()
            elif orig.islower():
                return new_alias.lower()
            elif orig[0].isupper() and orig[1:].islower():
                return new_alias.capitalize()
            else:
                return new_alias

        result, count = pattern.subn(replacer, text)
        if count == 0:
            result = f"{text} {new_alias}"
        return result


class FilePathManager(BasePathManager):
    """
    Class for managing file paths.

    For files, the output path is created by changing the file extension.
    """

    def __init__(self, input_path: Path, output_ext: str):
        super().__init__(input_path, output_ext)
        # Get the original extension without the dot (preserving its case)
        self.file_ext: str = self.input_path.suffix.lstrip(".")
        self.output_path: Path = self._generate_output_path()

    def _generate_output_path(self) -> Path:
        """
        Generate the output file path by changing the file's extension to the output_ext.
        """
        return self.input_path.with_suffix(f".{self.output_ext}")


def infer_majority_alias_in_directory(
    directory: Path, naming_ext_map: Dict[str, str]
) -> Optional[str]:
    """
    Infer the most common alias among files in the given directory based on naming_ext_map.
    This function is independent of the directory name.
    """
    aliases = [
        naming_ext_map[file.suffix.lower()]
        for file in directory.iterdir()
        if file.is_file() and file.suffix.lower() in naming_ext_map
    ]
    if not aliases:
        return None
    majority_alias, _ = Counter(aliases).most_common(1)[0]
    return majority_alias


class DirectoryPathManager(BasePathManager):
    """
    Class for managing directory paths.
    Separates file type inference (via majority alias) from naming logic.
    """

    def __init__(self, input_path: Path, output_ext: str):
        super().__init__(input_path, output_ext)
        # Use the new helper to infer the majority alias from files, not from the directory name
        self.input_alias = infer_majority_alias_in_directory(
            self.input_path, self.NAMING_EXT_MAP
        )
        self.output_name = self._generate_output_name()
        self.output_path: Path = self.input_dir / self.output_name

    def _generate_output_name(self) -> str:
        """
        Generate the output directory name.
        If the inferred alias is found inside the directory name,
        replace it using _replace_alias_in_string;
        otherwise, append '_' plus the output extension.
        """
        if self.input_alias and self.input_alias.lower() in self.input_name.lower():
            return self._replace_alias_in_string(
                self.input_name, self.input_alias, self.output_ext
            )
        else:
            return f"{self.input_name}_{self.output_ext}"

    def get_files(self, desired_input_type: Optional[str] = None) -> list:
        """
        Return a list of file Paths in the directory that have a recognized extension.
        If `desired_input_type` is provided, only return files whose inferred type matches it.
        """
        allowed = set(self.ALLOWED_EXT_MAP.keys())
        files = []
        for f in self.input_path.iterdir():
            if f.is_file() and f.suffix.lower() in allowed:
                if (
                    desired_input_type is None
                    or self.ALLOWED_EXT_MAP[f.suffix.lower()] == desired_input_type
                ):
                    files.append(f)
        return files


def create_path_manager(input_path: Path, output_ext: str) -> BasePathManager:
    """
    Factory function that returns an instance of FilePathManager or DirectoryPathManager
    based on whether input_path is a file or directory.

    Args:
        input_path: The Path object for the input file or directory.
        output_ext: The desired output extension or alias.

    Returns:
        An instance of FilePathManager (if input_path is a file) or
        DirectoryPathManager (if input_path is a directory).
    """
    if input_path.is_file():
        return FilePathManager(input_path, output_ext)
    else:
        return DirectoryPathManager(input_path, output_ext)


def prepare_parameters(input_path: Path, out_type: str) -> tuple:
    """
    Prepare conversion parameters from the input path using path management.

    Returns:
        files: list of file Paths (for a file, a singleton list).
        output_dest: output directory (None for file input).
        source_type: inferred type alias.
    """
    if input_path.is_file():
        pm = FilePathManager(input_path, out_type)
        # For a single file, source_type is inferred from the extension.
        alias = input_path.suffix.lstrip(".").lower()
        return ([input_path], None, alias)
    else:
        dpm = DirectoryPathManager(input_path, out_type)
        files = dpm.get_files(dpm.input_alias)
        output_dest = dpm.output_path
        output_dest.mkdir(parents=True, exist_ok=True)
        return (files, output_dest, dpm.input_alias)
