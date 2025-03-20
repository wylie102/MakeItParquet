#!/usr/bin/env python3
"""Manages the conversion of files to different formats using DuckDB."""

from queue import Queue
import os
import uuid
import tempfile
import duckdb
from Make_It_Parquet.conversion_data import ConversionData
from Make_It_Parquet.user_interface.prompts import prompt_for_output_extension

TYPE_CHECKING = False
if TYPE_CHECKING:
    from Make_It_Parquet.file_information import FileInfo
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
        self.conn: duckdb.DuckDBPyConnection = duckdb.connect(database=self.db_path)  # pyright: ignore[reportUnknownMemberType]
        self.file_manager: FileManager | DirectoryManager = file_manager
        self.import_queue: Queue[FileInfo] = Queue()
        self.pending_exports: list[ConversionData] = []
        self.one_in_one_out: bool = (
            self.file_manager.settings.supplied_output_ext is not None
        )

        if self.file_manager.input_ext:
            self._store_prepared_import_statement(self.file_manager.input_ext)
        self._populate_import_queue(file_manager.conversion_file_list)

    def _store_prepared_import_statement(self, input_ext: str) -> None:
        # Store prepared statement in duckdb database.
        _ = self.conn.execute(
            ConversionData.generate_prepared_import_statement(input_ext)
        )

    def _populate_import_queue(self, conversion_file_list: list[FileInfo]) -> None:
        """Populates the import queue with file paths.

        Args:
            conversion_file_list: List of file dictionaries containing paths
        """
        for file_info in conversion_file_list:
            self.import_queue.put(file_info)

    

    def run_conversion(self) -> None:
        """Processes files from the import queue.

        Workflow:
        1. If output_ext is not set, files are imported and held in pending_exports
        2. Once output_ext is set, pending files are processed in order
        3. After that, switches to immediate import-export mode
        """
        # Start import process
        while not self.import_queue.empty():
            # import file and store returned data.
            conversion_data = self._import_file()

            # Check for output_ext, if none keep importing files
            # If output_ext provided export all tables, and switch to one in one out.
            if not self.one_in_one_out:
                # add last file to pending_exports
                self.pending_exports.append(conversion_data)
                # If output extension is now set, process all pending files
                if self.file_manager.settings.supplied_output_ext:
                    # Store the prepared export statement
                    self._store_prepared_export_statement(
                        self.file_manager.settings.supplied_output_ext
                    )
                    self._process_pending_exports()
                    self.one_in_one_out = True

            # In one-in, one-out mode; export immediately after import
            else:
                self._export_file(conversion_data)

            # releasses process to check queue again.
            self.import_queue.task_done()

        # Shut down connection and clean up temp files.
        self.close_connection(True)

    def _import_file(self) -> ConversionData:
            file_info = self.import_queue.get()
            conversion_data = ConversionData(
                file_info.file_ext, file_info.file_path
            )
            _ = self.conn.execute(conversion_data.import_attributes.import_query)
            return conversion_data

    def _determine_output_extension(self):
        """
        Set the output file extension.

        If output extension is not already set in settings, prompts user to select
        one. Validates that output format differs from input format.
        """
        if not self.file_manager.settings.supplied_output_ext:
            if self.file_manager.input_ext:
                prompt_for_output_extension(
                    self.file_manager.input_ext, self.file_manager.settings
                )

    def _store_prepared_export_statement(self, output_ext: str) -> None:
        # Store prepared statement in duckdb database.
        _ = self.conn.execute(
            ConversionData.generate_prepared_export_statement(output_ext)
        )

    def _process_pending_exports(self) -> None:
        """Processes all pending exports in order."""
        for conversion_data in self.pending_exports:
            self._export_file(conversion_data)

    def _export_file(self, conversion_data: ConversionData) -> None:
        """
        Exports a table to a file with the specified output extension.
        Drops table and logs successful conversion.
        """
        # Export table to file.
        self._export_table(conversion_data)
        # Drop table.
        self._drop_table(conversion_data)
        # Log conversion
        self._log_conversion(conversion_data)

    def _export_table(self, conversion_data: ConversionData) -> None:
        export_statement: str = (
            conversion_data.export_attributes.generate_export_statement
        )
        _ = self.conn.execute(export_statement)    self.pending_exports.clear()

    def _drop_table(self, conversion_data: ConversionData):
            drop_statement: str = conversion_data.import_attributes.table_name
            _ = self.conn.execute(f"DROP TABLE {drop_statement}")

    def _log_conversion(self, conversion_data: ConversionData):
            import_file: str = conversion_data.import_attributes.file_path.name
            export_file: str = conversion_data.export_attributes.file_path.name
            self.file_manager.settings.logger.info(
                f"File {import_file} successfully converted to {export_file}"
            )


    def close_connection(self, cleanup_db_file: bool = False) -> None:
        """Closes the DuckDB connection and optionally removes the DB file.

        Args:
            cleanup_db_file: If True, the database file will be deleted
        """
        self.conn.close()
        if cleanup_db_file and os.path.exists(self.db_path):
            os.remove(self.db_path)
