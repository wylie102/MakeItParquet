# !/usr/bin/env python3
# /// script
# dependencies = [
#     "duckdb",
# ]
# ///
"""
Make-it-Parquet!: A data file conversion tool powered by DuckDB.
"""

from typing import Optional, Union
from user_interface.settings import Settings
from file_manager import FileManager, DirectoryManager
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
        self.file_manager = self._create_conversion_manager()

    def _create_conversion_manager(
        self,
    ) -> Union[FileManager, DirectoryManager]:
        """
        Determines the appropriate conversion manager (file or directory) depending on
        the input provided via the settings object.

        Args:
            settings (Settings): The application settings and configuration.

        Returns:
            FileManager or DirectoryManager: Instance corresponding to the target type.
        """
        if self.settings.file_info_dict["file_or_directory"] == "file":
            return FileManager(self)
        else:
            return DirectoryManager(self)

    def exit_program(self, message: str, error_type: Optional[str] = "error") -> None:
        """
        Exit program with logging and cleanup.

        Args:
            message: Error message to log
            error_type: Type of error ('error' or 'exception')
        """
        if error_type == "error":
            self.settings.logger.error(message)
        elif error_type == "exception":
            self.settings.logger.exception(message)

        self.settings.logger.stop_logging()
        exit(1)


def main() -> None:
    """
    Initialises the application settings and triggers the conversion process by creating
    """
    mp = MakeItParquet()
    mp.exit_program("Conversion complete.")


if __name__ == "__main__":
    main()
