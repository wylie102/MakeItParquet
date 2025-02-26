#! /usr/bin/env python3

from pathlib import Path
import os
import stat
from typing import Union, TypedDict


def resolve_path(input_path: Path) -> Path:
    """Resolves input path."""
    return input_path.resolve()


def generate_file_stat(path: Path) -> os.stat_result:
    """Generates file stat."""
    return path.stat()


def determine_file_or_dir(obj: Union[Path, os.stat_result]) -> str:
    """
    Determines if the given object represents a file or a directory.

    Accepts:
      - pathlib.Path: Uses the is_file method.
      - os.stat_result: Uses the stat module to check for regular files.

    Returns:
      "file" if the object represents a file, or "directory" otherwise.
    """
    if isinstance(obj, Path):
        return _file_or_dir_from_path(obj)
    elif isinstance(obj, os.stat_result):
        return _file_or_dir_from_stat(obj)


def _file_or_dir_from_path(path: Path) -> str:
    """Determines if a pathlib.Path object is a file or directory."""
    return "file" if path.is_file() else "directory"


def _file_or_dir_from_stat(stat_obj: os.stat_result) -> str:
    """Determines if an os.stat_result object represents a file or directory."""
    return "file" if stat.S_ISREG(stat_obj.st_mode) else "directory"


def file_size(stat_obj: os.stat_result) -> int:
    """Returns the size of the file."""
    return stat_obj.st_size


def file_name(path: Path) -> str:
    """Returns the name of the file."""
    return path.name


def file_extension(path: Path) -> str:
    """Returns the extension of the file."""
    return path.suffix


class FileInfoDict(TypedDict):
    path: Path
    stat_obj: os.stat_result
    file_name: str
    file_size: int
    file_extension: str


def create_file_info_dict_from_path(input_path: Path) -> FileInfoDict:
    """Creates an info dictionary for the given input path."""
    path = resolve_path(input_path)
    stat_obj = generate_file_stat(path)
    return {
        "path": path,
        "stat_obj": stat_obj,
        "file_name": file_name(path),
        "file_size": file_size(stat_obj),
        "file_extension": file_extension(path),
    }


def create_file_info_dict_from_scandir(entry: os.DirEntry) -> FileInfoDict:
    """Creates an info dictionary for the given input path."""
    path = resolve_path(entry.path)
    stat_obj = generate_file_stat(path)
    return {
        "path": path,
        "stat_obj": stat_obj,
        "file_name": file_name(path),
        "file_size": file_size(stat_obj),
        "file_extension": file_extension(path),
    }
