#! /usr/bin/env python3

from pathlib import Path
import os
import stat
from typing import TypedDict, Union


class FileInfoDict(TypedDict):
    path: Path
    stat_obj: os.stat_result
    file_name: str
    file_size: int
    file_extension: str
    file_or_directory: str


def resolve_path(input: Union[Path, os.DirEntry]) -> Path:
    """Resolves input path."""
    if isinstance(input, Path):
        return input.resolve()
    elif isinstance(input, os.DirEntry):
        return input.path.resolve()


def get_file_stat(
    input: Union[Path, os.DirEntry], resolved_path: Path
) -> os.stat_result:
    """Creates a file stat of the target file, which can be submitted as a path or an os.DirEntry object.

    Args:
        input: The target file in the form of a Path or os.DirEntry.
        resolved_path: Resolved path to be used instead of user entered path if object passed is a path.

    Returns:
        os.stat_result
    """
    if isinstance(input, Path):
        return resolved_path.stat()
    elif isinstance(input, os.DirEntry):
        return input.stat()


def file_or_dir_from_stat(stat_obj: os.stat_result) -> str:
    """Determines if an os.stat_result object represents a file or directory."""
    return "file" if stat.S_ISREG(stat_obj.st_mode) else "directory"


def create_file_info_dict(input: Union[Path, os.DirEntry]) -> FileInfoDict:
    """Creates an info dictionary for the given input path."""
    path = resolve_path(input)
    stat_obj = get_file_stat(input, path)
    return {
        "path": path,
        "stat_obj": stat_obj,
        "file_name": path.name,
        "file_size": stat_obj.st_size,
        "file_extension": path.suffix,
        "file_or_directory": file_or_dir_from_stat(stat_obj),
    }
