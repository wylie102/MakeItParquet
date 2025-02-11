#!/usr/bin/env python3
"""
Module for managing file and directory paths for file conversions.

This module defines classes to:
    - Detect if a given path is a file or a directory.
    - Infer the file type (for processing and naming) from the file extension.
    - Generate an output file path (by changing the extension) or output directory name
      while preserving common capitalization conventions.
"""

import re
import os
from pathlib import Path
from typing import Optional, Dict

from cli_interface import Settings


def normalize_path(path: Path) -> str:
    """Return the canonical string representation of a file path."""
    return str(path.resolve())


class BaseConversionManager(Settings):
    ALLOWED_EXT_MAP: Dict[str, str] = {
        ".csv": "csv",
        ".txt": "txt",
        ".tsv": "tsv",
        ".json": "json",
        ".parquet": "parquet",
        ".xlsx": "excel",
    }

    FILE_TYPE_ALIASES = {
        "csv": "csv",
        "txt": "txt",
        "tsv": "tsv",
        "json": "json",
        "js": "json",
        "excel": "excel",
        "ex": "excel",
        "xlsx": "excel",
        "parquet": "parquet",
        "pq": "parquet",
    }

    def __init__(self, settings: Settings):
        self.settings = settings
        self.input_name: str = self.settings.input_path.name
        self.input_dir: Path = self.settings.input_path.parent
        self.input_alias: str = self._get_input_alias(self.settings.input_path)
        # TODO: Initiate loading of largest file of input type into duckdb connection.
        self.output_alias: str = self.settings.prompt_for_output_type()

    def _get_input_alias(self, input_path: Path) -> str:
        if self.settings.file_or_dir == "file":
            return self.ALLOWED_EXT_MAP[input_path.suffix.lower()]
        else:
            return self.infer_majority_alias_in_directory(
                input_path, self.FILE_TYPE_ALIASES
            )

    def get_total_files(self, directory: Path) -> int:
        """
        Return the total number of files in the given directory using os.scandir()
        for optimal performance.
        """
        return sum(1 for entry in os.scandir(directory) if entry.is_file())

    def infer_majority_alias_in_directory(
        self, directory: Path, naming_ext_map: Dict[str, str]
    ) -> str:
        """
        Determine and return the majority alias for files in the directory based on
        a mapping of file extensions (naming_ext_map). The function counts eligible files
        while checking after each one whether early termination can occur—that is, if the
        current leader mathematically cannot be overtaken given the remaining unprocessed files.

        This implementation uses `get_total_files()` to efficiently count the files in the directory.
        It assumes that virtually all files are of the relevant type.
        """
        total = self.get_total_files(
            directory
        )  # TODO: Check whether this could be done faster duckdb.
        if total == 0:
            raise ValueError("No aliases found in directory")

        counts: Dict[str, int] = {}
        processed = 0
        leader = None  # The alias with the highest count so far.
        leader_count = 0  # The current highest alias count.
        second_highest = 0  # The runner-up count.

        for file in directory.iterdir():
            if not file.is_file():
                continue
            ext = file.suffix.lower()
            if ext not in naming_ext_map:
                continue

            processed += 1
            alias = naming_ext_map[ext]
            counts[alias] = counts.get(alias, 0) + 1

            # Update the leader and runner-up counts.
            if alias == leader:
                leader_count = counts[alias]
            elif counts[alias] > leader_count:
                second_highest = leader_count
                leader = alias
                leader_count = counts[alias]
            elif counts[alias] > second_highest:
                second_highest = counts[alias]

            remaining = total - processed
            # Check if the current leader is mathematically unbeatable.
            if leader_count > second_highest + remaining and leader is not None:
                return leader

        if leader is None:
            raise ValueError("No majority alias found")

        # Check for ambiguity: if more than one alias has the top count, ask the user to specify.
        leaders = [alias for alias, count in counts.items() if count == leader_count]
        if len(leaders) > 1:
            raise ValueError(
                f"Ambiguous file types found {leaders}. Please specify which one to convert."
            )  # TODO: Implement user prompt.

        return leader

    def _replace_alias_in_string(
        self, text: str, old_alias: str, new_alias: str
    ) -> str:
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


class FileConversionManager(BaseConversionManager):
    def __init__(self, settings):
        super().__init__(settings)
        self.file_ext: str = self.input_path.suffix.lstrip(".")
        self.output_path: Path = self._generate_output_path()

    def _generate_output_path(self) -> Path:
        return self.input_path.with_suffix(f".{self.output_ext}")

    def get_conversion_params(self):
        return ([self.input_path], None, self.input_path.suffix.lstrip(".").lower())


class DirectoryConversionManager(BaseConversionManager):
    def __init__(self, settings):
        super().__init__(settings)
        self.output_name = self._generate_output_name()
        self.output_path: Path = self.input_dir / self.output_name
        self.files_to_process = self.get_files(self.input_alias)

    def _generate_output_name(self) -> str:
        if self.input_alias and self.input_alias.lower() in self.input_name.lower():
            return self._replace_alias_in_string(
                self.input_name, self.input_alias, self.output_ext
            )
        else:
            return f"{self.input_name}_{self.output_ext}"

    def get_files(self, desired_input_type: Optional[str] = None) -> list:
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


def create_conversion_manager(settings: Settings):
    """
    Factory function to create a conversion manager based on the input path.
    """
    if settings.file_or_dir == "file":
        return FileConversionManager(settings)
    else:
        return DirectoryConversionManager(settings)
