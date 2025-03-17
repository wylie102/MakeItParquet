#!/usr/bin/env python3 --script
# /// script
# dependencies = [
#     "duckdb",
# ]
# ///
"""
Make-it-Parquet!: A data file conversion tool powered by DuckDB.
"""

from Make_It_Parquet.conversion_manager import ConversionManager

from Make_It_Parquet.file_manager import DirectoryManager, FileManager
from Make_It_Parquet.user_interface.cli_parser import parse_cli_arguments, CLIArgs
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
    Initialises the application settings and triggers the conversion process.
    """
    # Parse CLI arguments and store apliccable ones in Settings object.
    args: CLIArgs = parse_cli_arguments()
    settings: Settings = Settings(args)

    # Create FileManager and ConversionManager instances to manage conversion and file naming processes.
    file_manager: FileManager | DirectoryManager = create_file_manager(settings)
    file_manager.analyze_files()
    conversion_manager: ConversionManager = ConversionManager(file_manager)

    # Trigger conversion process.
    conversion_manager.run_conversion()  # TODO: This function call is a placeholder. Create a function which starts the conversion process and run it, may be two functions, one in file manager and another in conversion manager.

    # End program and ensure correct cleanup.
    settings.exit_program("Conversion complete.")


if __name__ == "__main__":
    main()
