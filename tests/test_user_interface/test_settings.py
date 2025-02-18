import argparse
import pytest
from pathlib import Path
from Make_It_Parquet.user_interface.settings import (
    Settings,
    InputOutputFlags,
)


@pytest.fixture
def mock_args():
    args = argparse.Namespace(
        input_path=Path(
            "/Users/wylie/Desktop/Projects/MakeItParquet/tests/files_for_testing_with/sample_files/sample.csv"
        ),
        log_level="INFO",
        input_format=None,
        output_format=None,
        sheet=None,
        range=None,
    )
    return args


@pytest.fixture
def mock_settings(mock_args):
    return Settings(mock_args)


def test_settings_initialisation(mock_settings: Settings, mock_args):
    assert mock_settings.args == mock_args
    assert mock_settings.logger is not None
    assert mock_settings.input_output_flags is not None
    assert mock_settings.input_ext is None
    assert mock_settings.output_ext is None
    assert mock_settings.path == mock_args.input_path.resolve()
    assert mock_settings.stat is not None
    assert mock_settings.file_or_dir is not None

    assert mock_settings.excel_settings is None
    assert mock_settings.txt_settings is None


@pytest.fixture
def mock_input_output_flags():
    return InputOutputFlags()


def test_input_output_flags_initialisation(mock_input_output_flags: InputOutputFlags):
    assert mock_input_output_flags.input_ext_supplied_from_cli is False
    assert mock_input_output_flags.output_ext_supplied_from_cli is False
    assert mock_input_output_flags.input_ext_auto_detected is False
    assert mock_input_output_flags.output_ext_supplied_from_prompt is False
    assert mock_input_output_flags.input_ext_supplied_from_prompt is False


def test_input_output_flags_set_flags(mock_input_output_flags: InputOutputFlags):
    mock_input_output_flags.set_flags("cli", "csv", "parquet")
    assert mock_input_output_flags.input_ext_supplied_from_cli is True
    assert mock_input_output_flags.output_ext_supplied_from_cli is True
    assert mock_input_output_flags.input_ext_auto_detected is False
    assert mock_input_output_flags.output_ext_supplied_from_prompt is False
    assert mock_input_output_flags.input_ext_supplied_from_prompt is False


def test_input_output_flags_set_flags_invalid_environment(
    mock_input_output_flags: InputOutputFlags,
):
    with pytest.raises(ValueError):
        mock_input_output_flags.set_flags("invalid", "csv", "parquet")


def test_set_cli_flags_with_input_ext(mock_input_output_flags: InputOutputFlags):
    mock_input_output_flags.set_cli_flags("csv", None)
    assert mock_input_output_flags.input_ext_supplied_from_cli is True
    assert mock_input_output_flags.output_ext_supplied_from_cli is False
    assert mock_input_output_flags.input_ext_auto_detected is False
    assert mock_input_output_flags.output_ext_supplied_from_prompt is False
    assert mock_input_output_flags.input_ext_supplied_from_prompt is False


def test_set_cli_flags_with_output_ext(mock_input_output_flags: InputOutputFlags):
    mock_input_output_flags.set_cli_flags(None, "parquet")
    assert mock_input_output_flags.input_ext_supplied_from_cli is False
    assert mock_input_output_flags.output_ext_supplied_from_cli is True
    assert mock_input_output_flags.input_ext_auto_detected is False
    assert mock_input_output_flags.output_ext_supplied_from_prompt is False
    assert mock_input_output_flags.input_ext_supplied_from_prompt is False


def test_set_cli_flags_with_input_and_output_ext(
    mock_input_output_flags: InputOutputFlags,
):
    mock_input_output_flags.set_cli_flags("csv", "parquet")
    assert mock_input_output_flags.input_ext_supplied_from_cli is True
    assert mock_input_output_flags.output_ext_supplied_from_cli is True
    assert mock_input_output_flags.input_ext_auto_detected is False
    assert mock_input_output_flags.output_ext_supplied_from_prompt is False
    assert mock_input_output_flags.input_ext_supplied_from_prompt is False


def test_set_cli_flags_with_no_input_or_output_ext(
    mock_input_output_flags: InputOutputFlags,
):
    mock_input_output_flags.set_cli_flags(None, None)
    assert mock_input_output_flags.input_ext_supplied_from_cli is False
    assert mock_input_output_flags.output_ext_supplied_from_cli is False
    assert mock_input_output_flags.input_ext_auto_detected is False
    assert mock_input_output_flags.output_ext_supplied_from_prompt is False
    assert mock_input_output_flags.input_ext_supplied_from_prompt is False
