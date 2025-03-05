#! /usr/bin/env python3

from pathlib import Path
import os
import stat
from typing import TypedDict


class FileInfoDict(TypedDict):
    path: Path
    stat_obj: os.stat_result
    file_name: str
    file_size: int
    file_extension: str
    file_or_directory: str


def resolve_path(input_path: Path) -> Path:
    """Resolves input path."""
    return input_path.resolve()


def generate_file_stat(path: Path) -> os.stat_result:
    """Generates file stat."""
    return path.stat()


def _file_or_dir_from_stat(stat_obj: os.stat_result) -> str:
    """Determines if an os.stat_result object represents a file or directory."""
    return "file" if stat.S_ISREG(stat_obj.st_mode) else "directory"


def create_file_info_dict_from_path(input_path: Path) -> FileInfoDict:
    """Creates an info dictionary for the given input path."""
    path = resolve_path(input_path)
    stat_obj = generate_file_stat(path)
    return {
        "path": path,
        "stat_obj": stat_obj,
        "file_name": path.name,
        "file_size": stat_obj.st_size,
        "file_extension": path.suffix,
        "file_or_directory": _file_or_dir_from_stat(stat_obj),
    }


def create_file_info_dict_from_scandir(entry: os.DirEntry) -> FileInfoDict:
    """Creates an info dictionary for the given input path."""
    path = resolve_path(entry.path)
    stat_obj = generate_file_stat(path)
    return {
        "path": path,
        "stat_obj": stat_obj,
        "file_name": path.name,
        "file_size": stat_obj.st_size,
        "file_extension": path.suffix,
        "file_or_directory": _file_or_dir_from_stat(stat_obj),
    }
