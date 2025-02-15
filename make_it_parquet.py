# !/usr/bin/env python3
# /// script
# dependencies = [
#     "duckdb",
# ]
# ///
"""
Make-it-Parquet!: A file conversion tool powered by DuckDB.
"""


from cli_interface import Settings

from conversion_manager import FileConversionManager, DirectoryConversionManager


def create_conversion_manager(settings: Settings):
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


def main():
    """
    Main entry point for the Make-it-Parquet! conversion tool.

    Initialises the application settings and triggers the conversion process by creating
    the appropriate conversion manager.
    """
    settings = Settings()
    create_conversion_manager(settings)


if __name__ == "__main__":
    main()
