#! /usr/bin/env python3

from pathlib import Path
import os

def resolve_path(input_path: Path) -> Path:
    """resolves input path"""
    return input_path.resolve()

def generate_file_stat(path: Path) -> os.stat_result: 
    """generates file stat"""
    return path.stat()

def file_or_dir(stat: os.stat_result) -> str:
    """determines if the path is a file or a directory"""
    return "file" if stat.is_file() else "directory"

def file_size(stat: os.stat_result) -> int:
    """returns the size of the file"""
    return stat.st_size

def file_name(path: Path) -> str:
    """returns the name of the file"""
    return path.name    

def file_extension(path: Path) -> str:
    """returns the extension of the file"""
    return path.suffix

def create_file_info_dict(input_path: Path) -> dict:
    """creates info dict"""
    path = resolve_path(input_path)
    stat = generate_file_stat(path)
    return {
        "path": path,
        "file_size": file_size(stat),
        "file_name": file_name(path),
        "file_extension": file_extension(path),
    }
