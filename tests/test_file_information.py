#! /usr/bin/env python3

import pytest
from pathlib import Path
from Make_It_Parquet.file_information import (
    resolve_path,
    file_or_dir_from_stat,
    get_file_stat,
    create_file_info_dict,
)

# Hard-coded golden info dictionary.
GOLDEN_INFO = {
    "txt": {
        "file_size": 226669,
        "file_name": "sample.txt",
        "file_extension": ".txt",
        "is_file": True,
    },
    "csv": {
        "file_size": 22,
        "file_name": "sample.csv",
        "file_extension": ".csv",
        "is_file": True,
    },
    "tsv": {
        "file_size": 1288,
        "file_name": "sample.tsv",
        "file_extension": ".tsv",
        "is_file": True,
    },
    "parquet": {
        "file_size": 132101,
        "file_name": "sample.parquet",
        "file_extension": ".parquet",
        "is_file": True,
    },
    "json": {
        "file_size": 46,
        "file_name": "sample.json",
        "file_extension": ".json",
        "is_file": True,
    },
    "xlsx": {
        "file_size": 4099,
        "file_name": "sample.xlsx",
        "file_extension": ".xlsx",
        "is_file": True,
    },
}


# Fixture to load the golden sample files from the fixed directory.
@pytest.fixture(scope="session")
def sample_files():
    base_dir = Path(
        "/Users/wylie/Desktop/Projects/MakeItParquet/tests/files_for_testing_with/sample_files"
    )
    file_paths = {
        "txt": base_dir / "sample.txt",
        "csv": base_dir / "sample.csv",
        "tsv": base_dir / "sample.tsv",
        "parquet": base_dir / "sample.parquet",
        "json": base_dir / "sample.json",
        "xlsx": base_dir / "sample.xlsx",
    }
    return file_paths


# Parameterize tests by file type. TODO:update for new method
@pytest.mark.parametrize("file_type", list(GOLDEN_INFO.keys()))
def test_resolve_path(sample_files, file_type):
    file_path = sample_files[file_type]
    assert resolve_path(file_path) == file_path


# TODO: create test for os.DirEntry input
@pytest.mark.parametrize("file_type", list(GOLDEN_INFO.keys()))
def test_get_file_stat(sample_files, file_type):
    file_path = sample_files[file_type].resolve()
    expected = GOLDEN_INFO[file_type]
    stat_obj = get_file_stat(file_path, file_path)
    assert stat_obj.st_size == expected["file_size"]


@pytest.mark.parametrize("file_type", list(GOLDEN_INFO.keys()))
def test_file_or_dir_from_stat(sample_files, file_type):
    file_path = sample_files[file_type]
    stat_obj = file_path.os.stat
    assert file_or_dir_from_stat(stat_obj) == "file"


# TODO: update to account for larger file dictionary.
@pytest.mark.parametrize("file_type", list(GOLDEN_INFO.keys()))
def test_create_file_info_dict(sample_files, file_type):
    file_path = sample_files[file_type]
    expected = GOLDEN_INFO[file_type]
    info = create_file_info_dict(file_path)
    # Normalize the path to a string for comparison.
    assert info["path"] == expected["path"]
    assert info["file_size"] == expected["file_size"]
    assert info["file_name"] == expected["file_name"]
    assert info["file_extension"] == expected["file_extension"]
