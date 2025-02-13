# !/usr/bin/env python3
# /// script
# dependencies = [
#     "duckdb",
# ]
# ///
"""
DataTad: A file conversion tool powered by DuckDB.
"""



from cli_interface import Settings

from conversion_manager import FileConversionManager, DirectoryConversionManager

def create_conversion_manager(settings: Settings):
    """
    Factory function to create a conversion manager based on the input path.
    """
    if settings.file_or_dir == "file":
        return FileConversionManager(settings)
    else:
        return DirectoryConversionManager(settings)


def main():
    """
    Main function to run the DataTad conversion tool.
    """
    settings = Settings()
    create_conversion_manager(settings)

if __name__ == "__main__":
    main()
