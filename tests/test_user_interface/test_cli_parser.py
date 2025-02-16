#! /usr/bin/env python3

import pytest
import argparse
import sys
from pathlib import Path
from user_interface.settings import InputOutputFlags
from user_interface.cli_parser import (
    validate_input_format,
    validate_output_format,
    reset_extensions_if_same,
    validate_format_inputs,
    parse_cli_arguments,
)


@pytest.fixture
def args():
    return argparse.Namespace()


@pytest.fixture
def input_output_flags():
    return InputOutputFlags()


def test_parse_cli_arguments_minimal(monkeypatch):
    """Test with only the required argument (input_path)."""
    monkeypatch.setattr(sys, "argv", ["script_name", "data/input.csv"])

    args = parse_cli_arguments()

    assert args.input_path == Path("data/input.csv")
    assert args.output_path is None
    assert args.input_format is None
    assert args.output_format is None
    assert args.sheet is None
    assert args.range is None
    assert args.log_level == "INFO"  # Default value


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
            "-s",
            "Sheet1",
            "-c",
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
    assert args.sheet == "Sheet1"
    assert args.range == "A1:B10"
    assert args.log_level == "DEBUG"


def test_parse_cli_arguments_missing_input(monkeypatch):
    """Test when no input path is provided (should raise an error)."""
    monkeypatch.setattr(sys, "argv", ["script_name"])

    with pytest.raises(SystemExit):
        parse_cli_arguments()


def test_parse_cli_arguments_invalid_log_level(monkeypatch):
    """Test invalid log level (should still parse but with default)."""
    monkeypatch.setattr(
        sys, "argv", ["script_name", "data/input.csv", "--log-level", "INVALID"]
    )

    args = parse_cli_arguments()
    assert args.log_level == "INVALID"  # argparse does not validate log levels


@pytest.mark.parametrize(
    "input_format, expected",
    [
        ("csv", ".csv"),
        ("tsv", ".tsv"),
        ("txt", ".txt"),
        ("parquet", ".parquet"),
        ("pq", ".parquet"),
        ("json", ".json"),
        ("js", ".json"),
        ("excel", ".xlsx"),
        ("ex", ".xlsx"),
        ("xlsx", ".xlsx"),
    ],
)
def test_validate_input_format(args: argparse.Namespace, input_format, expected):
    args.input_format = input_format
    assert validate_input_format(args) == expected


def test_validate_input_format_none(args: argparse.Namespace):
    args.input_format = None
    assert validate_input_format(args) is None


# Test invalid input formats
@pytest.mark.parametrize("invalid_format", ["invalid", "unsupported", "bad"])
def test_validate_input_format_invalid(
    args: argparse.Namespace, invalid_format, caplog
):
    args.input_format = invalid_format
    with caplog.at_level("ERROR"):
        result = validate_input_format(args)
    assert result is None
    # Check that the error message was logged correctly.
    assert f"Received invalid input format: {invalid_format}" in caplog.text


@pytest.mark.parametrize(
    "output_format, expected",
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
def test_validate_output_format(args: argparse.Namespace, output_format, expected):
    args.output_format = output_format
    assert validate_output_format(args) == expected


def test_validate_output_format_none(args: argparse.Namespace):
    args.output_format = None
    assert validate_output_format(args) is None


# Test invalid output formats
@pytest.mark.parametrize("invalid_format", ["invalid", "unsupported", "bad"])
def test_validate_output_format_invalid(
    args: argparse.Namespace, invalid_format, caplog
):
    args.output_format = invalid_format
    with caplog.at_level("ERROR"):
        result = validate_output_format(args)
    assert result is None
    # Check that the error message was logged correctly.
    assert f"Received invalid output format: {invalid_format}" in caplog.text


def test_reset_extensions_if_same():
    input_ext, output_ext = reset_extensions_if_same(".csv", ".csv")
    assert input_ext is None
    assert output_ext is None
    input_ext, output_ext = reset_extensions_if_same(".csv", ".parquet")
    assert input_ext == ".csv"
    assert output_ext == ".parquet"


def test_reset_extensions_if_same_logging(caplog):
    """Test that providing the same input and output extensions logs an error message."""
    with caplog.at_level("ERROR"):
        input_ext, output_ext = reset_extensions_if_same(".csv", ".csv")
    assert input_ext is None
    assert output_ext is None
    assert "Input and output extensions cannot be the same" in caplog.text


def test_validate_format_inputs(
    args: argparse.Namespace, input_output_flags: InputOutputFlags
):
    args.input_format = "csv"
    args.output_format = "parquet"
    input_ext, output_ext = validate_format_inputs(args, input_output_flags)
    assert input_ext == ".csv"
    assert output_ext == ".parquet"
    args.input_format = None
    args.output_format = None
    input_ext, output_ext = validate_format_inputs(args, input_output_flags)
    assert input_ext is None
    assert output_ext is None
    args.input_format = "csv"
    args.output_format = "csv"
    input_ext, output_ext = validate_format_inputs(args, input_output_flags)
    assert input_ext is None
    assert output_ext is None
