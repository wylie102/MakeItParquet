#!/usr/bin/env uv run -m
# /// script
# dependencies = [
#     "duckdb",
# ]
# ///
"""
Make-it-Parquet!: A data file conversion tool powered by DuckDB.
"""

import threading

from Make_It_Parquet.conversion_manager import ConversionManager
from Make_It_Parquet.file_manager import DirectoryManager, FileManager
from Make_It_Parquet.user_interface.cli_parser import CLIArgs, parse_cli_arguments
from Make_It_Parquet.user_interface.settings import Settings


def create_file_manager(
    settings: Settings,
) -> FileManager | DirectoryManager:
    """
    Determines the appropriate conversion manager (file or directory) depending on
    the input provided via the settings object.

    Args:
        settings (Settings): The application settings and configuration.

    Returns:
        FileManager or DirectoryManager: Instance corresponding to the target type.
    """
    if settings.file_info.file_or_directory == "file":
        return FileManager(settings)
    else:
        return DirectoryManager(settings)


def main() -> None:
    """
    Initializes the application settings, creates the file manager and conversion manager,
    and runs export preparation and conversion concurrently.
    """
    # Parse CLI arguments and initialize settings.
    args: CLIArgs = parse_cli_arguments()
    settings: Settings = Settings(args)

    # Create FileManager (or DirectoryManager) and generate the conversion file list.
    file_manager = create_file_manager(settings)
    file_manager.get_conversion_list()

    # Initialize the ConversionManager.
    conversion_manager = ConversionManager(file_manager)

    # Create threads for export preparation and conversion processing.
    thread_prepare = threading.Thread(
        target=conversion_manager.prepare_for_export, name="PrepareExportThread"
    )
    thread_run = threading.Thread(
        target=conversion_manager.run_conversion, name="RunConversionThread"
    )

    # Start both threads.
    thread_prepare.start()
    thread_run.start()

    # Wait for both threads to complete.
    thread_prepare.join()
    thread_run.join()

    # Clean up and exit.
    settings.exit_program("Conversion complete.")


if __name__ == "__main__":
    main()
