#!/usr/bin/env python3

import pytest
from pathlib import Path
from Make_It_Parquet.file_information import (
    resolve_path,
    file_or_dir_from_stat,
    get_file_stat,
    create_file_info_dict,
)

# Import the generated golden info
from tests.files_for_testing_with.golden_info import GOLDEN_INFO


# Fixture to load the sample files from the fixed directory
@pytest.fixture(scope="session")
def sample_files():
    file_paths = {}
    for file_type, info in GOLDEN_INFO.items():
        file_paths[file_type] = Path(info["path"])
    return file_paths


@pytest.mark.parametrize("file_type", list(GOLDEN_INFO.keys()))
def test_resolve_path(sample_files, file_type):
    file_path = sample_files[file_type]
    resolved = resolve_path(file_path)
    assert str(resolved) == GOLDEN_INFO[file_type]["path"]


@pytest.mark.parametrize("file_type", list(GOLDEN_INFO.keys()))
def test_get_file_stat(sample_files, file_type):
    file_path = sample_files[file_type]
    stat_obj = get_file_stat(file_path, file_path)
    golden_stat = GOLDEN_INFO[file_type]["stat_obj"]

    # Test key properties from the stat object
    assert stat_obj.st_size == golden_stat["st_size"]
    assert stat_obj.st_mode == golden_stat["st_mode"]


@pytest.mark.parametrize("file_type", list(GOLDEN_INFO.keys()))
def test_file_or_dir_from_stat(sample_files, file_type):
    file_path = sample_files[file_type]
    stat_obj = file_path.stat()
    expected_type = GOLDEN_INFO[file_type]["file_or_directory"]
    assert file_or_dir_from_stat(stat_obj) == expected_type


@pytest.mark.parametrize("file_type", list(GOLDEN_INFO.keys()))
def test_create_file_info_dict(sample_files, file_type):
    file_path = sample_files[file_type]
    golden_data = GOLDEN_INFO[file_type]

    info = create_file_info_dict(file_path)

    # Test all important properties
    assert str(info["path"]) == golden_data["path"]
    assert info["file_size"] == golden_data["file_size"]
    assert info["file_name"] == golden_data["file_name"]
    assert info["file_extension"] == golden_data["file_extension"]
    assert info["file_or_directory"] == golden_data["file_or_directory"]
