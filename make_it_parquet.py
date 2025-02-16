# !/usr/bin/env python3
# /// script
# dependencies = [
#     "duckdb",
# ]
# ///
"""
Make-it-Parquet!: A data file conversion tool powered by DuckDB.
"""

from user_interface.logger import Logger
from typing import Optional
from user_interface.settings import Settings
from conversion_manager import FileConversionManager, DirectoryConversionManager
from user_interface.cli_parser import parse_cli_arguments

class MakeItParquet:
    """
    Main class for the Make-it-Parquet! conversion tool.
    """

    def __init__(self):
        """
        Initialise the MakeItParquet class.
        """
        self.args = parse_cli_arguments()
        self.settings = Settings(self.args)
        self.logger = Logger(self.settings.args.log_level)
        self.conversion_manager = self._create_conversion_manager(self.settings)

    def _create_conversion_manager(self, settings: Settings) -> FileConversionManager | DirectoryConversionManager:
        """
        Factory function to create a conversion manager based on the input path.

        Determines the appropriate conversion manager (file or directory) depending on
        the input provided via the settings object.

        Args:
            settings (Settings): The application settings and configuration.

        Returns:
            FileConversionManager or DirectoryConversionManager: Instance corresponding to the target type.
        """
        if settings.file_or_dir == "file":
            return FileConversionManager(settings)
        else:
            return DirectoryConversionManager(settings)

    def exit_program(self, message: str, error_type: Optional[str] = "error") -> None:
        """
        Exit program with logging and cleanup.

        Args:
            message: Error message to log
            error_type: Type of error ('error' or 'exception')
        """
        if error_type == "error":
            self.logger.error(message)
        elif error_type == "exception":
            self.logger.exception(message)

        self.settings.logger._stop_logging()  # Ensure logging is stopped before exit
        exit(1)


def main() -> None:
    """
    Main entry point for the Make-it-Parquet! conversion tool.

    Initialises the application settings and triggers the conversion process by creating
    the appropriate conversion manager.
    """
    mp = MakeItParquet()
    mp.exit_program("Conversion complete.")


if __name__ == "__main__":
    main()
