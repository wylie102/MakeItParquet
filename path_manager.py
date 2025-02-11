#!/usr/bin/env python3
"""
Module for managing file paths for DuckConvert.

Provides a simple FilePathManager class that retrieves files from a directory
and designates an output directory.
"""

from pathlib import Path
from typing import List, Optional


class FilePathManager:
    def __init__(self, input_dir: Path, output_type: str):
        self.input_dir = input_dir
        self.output_type = output_type
        # Define the output directory (for example, a "converted" subfolder).
        self.output_path = input_dir / "converted"
        # Optionally, infer the majority file type from files in the directory.
        self.input_alias = self._infer_input_alias()

    def _infer_input_alias(self) -> Optional[str]:
        # A simple heuristic: use the file type of the first file found.
        for f in self.input_dir.iterdir():
            if f.is_file():
                ext = f.suffix.lower()
                if ext == ".csv":
                    return "csv"
                elif ext in [".tsv", ".txt"]:
                    return "txt"
                elif ext == ".json":
                    return "json"
                elif ext in [".parquet", ".pq"]:
                    return "parquet"
                elif ext in [".xlsx", ".xls"]:
                    return "excel"
        return None

    def get_files(self, file_type: str) -> List[Path]:
        """
        Return a list of files in the directory matching the given file_type.
        (This example uses file extension matching.)
        """
        return [
            f
            for f in self.input_dir.iterdir()
            if f.is_file() and f.suffix.lower().lstrip(".") == file_type
        ]


def create_path_manager(input_dir: Path, output_type: str) -> FilePathManager:
    return FilePathManager(input_dir, output_type)
