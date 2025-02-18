#! /usr/bin/env python3

import pytest
import argparse
import sys
from pathlib import Path
from Make_It_Parquet.user_interface.settings import InputOutputFlags
from Make_It_Parquet.user_interface.cli_parser import (
    parse_cli_arguments,
    _check_format_supported,
    _map_format_to_extension,
    _validate_format,
    _input_output_extensions_same,
    get_input_output_extensions,
)


@pytest.fixture
def args():
    return argparse.Namespace()


@pytest.fixture
def input_output_flags():
    return InputOutputFlags()


# Test minimal arguments
def test_parse_cli_arguments_minimal(monkeypatch):
    """Test with only the required argument (input_path)."""
    monkeypatch.setattr(sys, "argv", ["script_name", "data/input.csv"])

    args = parse_cli_arguments()

    assert args.input_path == Path("data/input.csv")
    assert args.output_path is None
    assert args.input_format is None
    assert args.output_format is None
    assert args.excel_sheet is None
    assert args.excel_range is None
    assert args.log_level == "INFO"  # Default value


###--- test parse_cli_arguments ---###


# Test all possible arguments
def test_parse_cli_arguments_all_options(monkeypatch):
    """Test with all possible arguments."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "script_name",
            "data/input.csv",
            "-op",
            "data/output.parquet",
            "-i",
            "csv",
            "-o",
            "parquet",
            "-es",
            "Sheet1",
            "-er",
            "A1:B10",
            "--log-level",
            "DEBUG",
        ],
    )

    args = parse_cli_arguments()

    assert args.input_path == Path("data/input.csv")
    assert args.output_path == Path("data/output.parquet")
    assert args.input_format == "csv"
    assert args.output_format == "parquet"
    assert args.excel_sheet == "Sheet1"
    assert args.excel_range == "A1:B10"
    assert args.log_level == "DEBUG"


# Test missing input path
def test_parse_cli_arguments_missing_input(monkeypatch):
    """Test when no input path is provided (should raise an error)."""
    monkeypatch.setattr(sys, "argv", ["script_name"])

    with pytest.raises(SystemExit):
        parse_cli_arguments()


# Test invalid log level
def test_parse_cli_arguments_invalid_log_level(monkeypatch):
    """Test invalid log level (should still parse but with default)."""
    monkeypatch.setattr(
        sys, "argv", ["script_name", "data/input.csv", "--log-level", "INVALID"]
    )

    args = parse_cli_arguments()
    assert args.log_level == "INVALID"  # argparse does not validate log levels


###--- test _check_format_supported ---###


# Test valid input formats
@pytest.mark.parametrize(
    "format, expected",
    [
        ("csv", True),
        ("tsv", True),
        ("txt", True),
        ("parquet", True),
        ("pq", True),
        ("json", True),
        ("js", True),
        ("excel", True),
        ("ex", True),
        ("xlsx", True),
    ],
)
def test_check_format_supported(format, expected):
    assert _check_format_supported(format) == expected


# Test invalid input formats
@pytest.mark.parametrize("invalid_format", ["invalid", "unsupported", "bad"])
def test_check_format_supported_invalid(invalid_format, caplog):
    with caplog.at_level("WARNING"):
        result = _check_format_supported(invalid_format)
    assert result is False
    # Check that the error message was logged correctly.
    assert f"Received invalid format: {invalid_format}" in caplog.text


###--- test _map_format_to_extension ---###


@pytest.mark.parametrize(
    "format, expected",
    [
        ("csv", ".csv"),
        ("tsv", ".tsv"),
        ("txt", ".txt"),
        ("parquet", ".parquet"),
        ("pq", ".parquet"),
        ("json", ".json"),
        ("js", ".json"),
        ("excel", ".xlsx"),
        ("xlsx", ".xlsx"),
    ],
)
def test_map_format_to_extension(format, expected):
    assert _map_format_to_extension(format) == expected


###--- test _validate_format ---###


# Test valid input formats
@pytest.mark.parametrize(
    "format, expected",
    [
        ("csv", ".csv"),
        ("tsv", ".tsv"),
        ("txt", ".txt"),
        ("parquet", ".parquet"),
        ("pq", ".parquet"),
        ("json", ".json"),
        ("js", ".json"),
        ("excel", ".xlsx"),
        ("xlsx", ".xlsx"),
        ("invalid", None),
        ("unsupported", None),
        ("bad", None),
    ],
)
def test_validate_format(format, expected):
    result = _validate_format(format)
    assert result == expected


###--- test _input_output_extensions_same ---###


@pytest.mark.parametrize(
    "input_ext, output_ext, expected",
    [
        ("csv", "csv", True),
        ("csv", "parquet", False),
        (None, None, False),
        ("csv", None, False),
        (None, "csv", False),
    ],
)
def test_input_output_extensions_same(input_ext: str, output_ext: str, expected: bool):
    result = _input_output_extensions_same(input_ext, output_ext)
    assert result == expected


###--- test get_input_output_extensions ---###


@pytest.mark.parametrize(
    "input_format, output_format, expected",
    [
        # Both valid and different formats
        ("csv", "parquet", (".csv", ".parquet")),
        # Both valid but the same (should be reset to None)
        ("csv", "csv", (None, None)),
        # Valid input, invalid output (invalid output should yield None)
        ("csv", "invalid", (".csv", None)),
        # Invalid input, valid output (invalid input should yield None)
        ("invalid", "parquet", (None, ".parquet")),
        # Both formats missing
        (None, None, (None, None)),
        # Input missing, valid output provided
        (None, "csv", (None, ".csv")),
        # Valid input provided, output missing
        ("json", None, (".json", None)),
    ],
)
def test_get_input_output_extensions(
    input_format, output_format, expected, input_output_flags, args
):
    """
    Test get_input_output_extensions for a representative set of scenarios:
      - Valid and differing formats.
      - Same valid formats (which should reset both to None).
      - One valid and one invalid format.
      - Missing format(s).
    """
    # Update the args fixture with the given format values.
    args.input_format = input_format
    args.output_format = output_format

    # Call the function under test.
    result = get_input_output_extensions(args, input_output_flags)

    # Assert that the result matches the expected tuple.
    assert result == expected
