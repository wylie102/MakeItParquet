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
from pathlib import Path
from typing import Optional, Dict
from collections import Counter


def normalize_path(path: Path) -> str:
    """Return the canonical string representation of a file path."""
    return str(path.resolve())


class BasePathManager:
    ALLOWED_EXT_MAP: Dict[str, str] = {
        ".csv": "csv",
        ".txt": "csv",  # processing: treat txt like csv
        ".json": "json",
        ".parquet": "parquet",
        ".parq": "parquet",
        ".pq": "parquet",
        ".xlsx": "excel",
    }
    NAMING_EXT_MAP: Dict[str, str] = {
        ".csv": "csv",
        ".txt": "txt",
        ".tsv": "tsv",
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
        self.input_alias: Optional[str] = None

    @staticmethod
    def _replace_alias_in_string(text: str, old_alias: str, new_alias: str) -> str:
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
    def __init__(self, input_path: Path, output_ext: str):
        super().__init__(input_path, output_ext)
        self.file_ext: str = self.input_path.suffix.lstrip(".")
        self.output_path: Path = self._generate_output_path()

    def _generate_output_path(self) -> Path:
        return self.input_path.with_suffix(f".{self.output_ext}")

    def get_conversion_params(self):
        return ([self.input_path], None, self.input_path.suffix.lstrip(".").lower())


def infer_majority_alias_in_directory(
    directory: Path, naming_ext_map: Dict[str, str]
) -> Optional[str]:
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
    def __init__(self, input_path: Path, output_ext: str):
        super().__init__(input_path, output_ext)
        self.input_alias = infer_majority_alias_in_directory(
            self.input_path, self.NAMING_EXT_MAP
        )
        self.output_name = self._generate_output_name()
        self.output_path: Path = self.input_dir / self.output_name

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

    def get_conversion_params(self):
        files = self.get_files(self.input_alias)
        self.output_path.mkdir(parents=True, exist_ok=True)
        return (files, self.output_path, self.input_alias)


def create_path_manager(input_path: Path, output_ext: str):
    if input_path.is_file():
        return FilePathManager(input_path, output_ext)
    else:
        return DirectoryPathManager(input_path, output_ext)
