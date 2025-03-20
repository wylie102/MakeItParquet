#! /usr/bin/env python3

from pathlib import Path
import os
import stat
from typing import NamedTuple


class FileInfo(NamedTuple):
    file_path: Path
    stat_obj: os.stat_result
    file_name: str
    file_size: int
    file_ext: str
    file_or_directory: str


def resolve_path(input: Path | os.DirEntry[str]) -> Path:
    """Resolves input path."""
    if isinstance(input, Path):
        return input.resolve()
    else:
        return Path(input.path).resolve()


def get_file_stat(
    input: Path | os.DirEntry[str], resolved_path: Path
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
    else:
        return input.stat()


def file_or_dir_from_stat(stat_obj: os.stat_result) -> str:
    """Determines if an os.stat_result object represents a file or directory."""
    return "file" if stat.S_ISREG(stat_obj.st_mode) else "directory"


def create_file_info(input: Path | os.DirEntry[str]) -> FileInfo:
    """Creates an info dataclass for the given input path."""
    path = resolve_path(input)
    stat_obj = get_file_stat(input, path)
    file_name = path.name
    file_size = stat_obj.st_size
    file_extension = path.suffix
    file_or_directory = file_or_dir_from_stat(stat_obj)
    file_info = FileInfo(
        path, stat_obj, file_name, file_size, file_extension, file_or_directory
    )
    return file_info
