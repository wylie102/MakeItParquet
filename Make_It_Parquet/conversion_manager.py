#!/usr/bin/env python3
"""Manages the conversion of files to different formats using DuckDB."""

from queue import Queue
import os
import uuid
import tempfile
import duckdb
from Make_It_Parquet.user_interface.interactive import prompt_for_output_format
import Make_It_Parquet.converters as conv
from Make_It_Parquet.user_interface.settings import Settings
from .extension_mapping import (
    ALIAS_TO_EXTENSION_MAP,
    import_class_map,
)
from Make_It_Parquet.file_information import FileInfo
from pathlib import Path

from Make_It_Parquet.file_manager import FileManager, DirectoryManager


class ConversionManager:
    """Manages the conversion process using a single persistent DuckDB connection.

    This class handles file import and export operations following a workflow:
    - Files are imported one-by-one into uniquely named tables
    - If output format is not yet specified, imported files are queued
    - Once output format is set, pending files are processed in order
    - After clearing the queue, operates in one-in, one-out mode

    Attributes:
        db_path: Path to the DuckDB database file
        conn: Active DuckDB connection
        output_ext: File extension for exported files
        settings: Application settings object
        import_queue: Queue of files to be imported
        pending_exports: Files imported but not yet exported
        one_in_one_out: Whether to export immediately after import
    """

    def __init__(self, file_manager: FileManager | DirectoryManager) -> None:
        """Initializes the conversion manager.

        Args:
            file_manager: BaseFileManager instance containing conversion settings
                and file list information
        """
        self.db_path: str = os.path.join(
            tempfile.gettempdir(), f"make_it_parquet_{uuid.uuid4()}.db"
        )
        self.conn: duckdb.DuckDBPyConnection = duckdb.connect(self.db_path)
        self.input_ext: str | None = file_manager.input_ext
        self.output_ext: str | None = file_manager.output_ext
        self.settings: Settings = file_manager.settings
        self.import_queue: Queue[Path] = Queue()
        self.pending_exports: list[dict[str, Path | str]] = []
        self.one_in_one_out: bool = (
            self.output_ext is not None
        )  # TODO: ? Change to exists rather than the double negative

        self._populate_import_queue(file_manager.conversion_file_list)

    def _populate_import_queue(self, conversion_file_list: list[FileInfo]) -> None:
        """Populates the import queue with file paths.

        Args:
            conversion_file_list: List of file dictionaries containing paths
        """
        for file_info in conversion_file_list:
            self.import_queue.put(file_info.path)

    def _generate_import_class(self):
        """
        Create appropriate input class based on file extension.

        Determines and instantiates the correct input class based on the input
        file extension. Handles special cases for Excel files based on output format.

        Returns:
            BaseInputConnection: Instance of appropriate input class for the file type

        Raises:
            ValueError: If input extension is not supported
        """
        if not self.input_ext == ".xlsx":
            self._return_standard_import_class()
        else:
            self._return_excel_import_class()

    def _return_standard_import_class(self):
        """Returns a non-excel import class."""
        if self.input_ext:
            return import_class_map[self.input_ext]

    def _return_excel_import_class(self):
        """Returns an excel import class."""
        # TODO: 21-Feb-2025: Write excel_utils.py functions/methods to load excel extension.
        pass  # TODO: 21-Feb-2025: Refactor excel_utils.py

    def _determine_output_extension(self):
        """
        Set the output file extension.

        If output extension is not already set in settings, prompts user to select
        one. Validates that output format differs from input format.
        """
        if not self.output_ext:
            prompt_for_output_format(self.settings, ALIAS_TO_EXTENSION_MAP)

    def _generate_export_class(self):
        """
        Create appropriate output class based on output format.

        Determines and instantiates the correct output class based on the desired
        output format.

        Returns:
            BaseOutputConnection: Instance of appropriate output class for the format

        Raises:
            ValueError: If output format is not supported
        """
        export_class_map = {
            "csv": conv.CSVOutput,
            "tsv": conv.TsvOutput,
            "txt": conv.TxtOutput,
            "json": conv.JSONOutput,
            "parquet": conv.ParquetOutput,
            "excel": conv.ExcelOutput,
        }

        if self.output_ext in export_class_map:
            return export_class_map[self.output_ext]()

        raise ValueError(f"Unsupported output extension: {self.output_ext}")

    def _generate_table_name(self, file_path: Path) -> str:
        """Generates a unique table name for the imported file.

        Args:
            file_path: Path to the file being imported

        Returns:
            A unique table name based on the filename and a UUID
        """
        base = file_path.stem  # TODO: check origin of file_path to check it actually has been turned into a Path object.
        return f"{base}_{uuid.uuid4().hex[:8]}"

    def _import_file(self, file_path: Path) -> str:
        """Imports a file into a DuckDB table.

        Args:
            file_path: Path to the file to import

        Returns:
            The name of the created table

        Raises:
            duckdb.Error: If the import operation fails
        """
        table_name = self._generate_table_name(file_path)
        import_sql = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}')"  # TODO: this needs to call correct import class for the file ext.
        _ = self.conn.execute(import_sql)
        return table_name

    def _export_file(self, file_path: Path, table_name: str) -> None:
        """Exports a table to a file with the specified output extension.

        Args:
            file_path: Original path of the imported file
            table_name: Name of the table to export

        Raises:
            duckdb.Error: If the export operation fails
        """
        base = os.path.splitext(os.path.basename(file_path))[0]
        output_dir = os.path.dirname(file_path)
        output_file = os.path.join(output_dir, f"{base}.{self.output_ext}")

        export_sql = f"COPY {table_name} TO '{output_file}' (FORMAT '{self.output_ext}')"  # TODO: may also need to do use output class here, although with moving to using persistent db and copy it may not be necessary.
        _ = self.conn.execute(
            export_sql
        )  # TODO: may need a check here to ensure successful export before dropping tables.
        _ = self.conn.execute(f"DROP TABLE {table_name}")

        self._log_success(file_path, output_file)

    def _log_success(self, input_file: str, output_file: str) -> None:
        """Logs successful file conversion if logger is available.

        Args:
            input_file: Path to the original input file
            output_file: Path to the newly created output file
        """
        if (
            self.settings and hasattr(self.settings, "logger")
        ):  # TODO: check whether these checks for existance of settings and logger are redundant or not.
            self.settings.logger.info(
                f"File {input_file} successfully converted to {output_file}"
            )

    def run_conversion(self) -> None:
        """Processes files from the import queue.

        Workflow:
        1. If output_ext is not set, files are imported and held in pending_exports
        2. Once output_ext is set, pending files are processed in order
        3. After that, switches to immediate import-export mode
        """
        while not self.import_queue.empty():
            file_path = self.import_queue.get()
            table_name = self._import_file(file_path)

            if not self.one_in_one_out:
                # Add current file to pending exports
                self.pending_exports.append(
                    {"file_path": file_path, "table_name": table_name}
                )

                # If output extension is now set, process all pending files
                if self.output_ext is not None:
                    self._process_pending_exports()
                    self.one_in_one_out = True
            else:
                # In one-in, one-out mode; export immediately after import
                self._export_file(file_path, table_name)

            self.import_queue.task_done()

    def _process_pending_exports(self) -> None:
        """Processes all pending exports in order."""
        for pending in self.pending_exports:
            self._export_file(pending["file_path"], pending["table_name"])
        self.pending_exports.clear()

    def close_connection(self, cleanup_db_file: bool = False) -> None:
        """Closes the DuckDB connection and optionally removes the DB file.

        Args:
            cleanup_db_file: If True, the database file will be deleted
        """
        self.conn.close()
        if cleanup_db_file and os.path.exists(self.db_path):
            os.remove(self.db_path)
