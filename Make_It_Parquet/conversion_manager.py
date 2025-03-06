#!/usr/bin/env python3
"""Manages the conversion of files to different formats using DuckDB."""

from typing import List, Dict, Optional, Any
from queue import Queue
import os
import uuid
import duckdb
from user_interface.settings import Settings


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

    def __init__(
        self,
        db_path: str,
        conversion_file_list: List[Dict[str, Any]],
        output_ext: Optional[str] = None,
        settings: Optional[Settings] = None,
    ) -> None:
        """Initializes the conversion manager.

        Args:
            db_path: Path to the DuckDB database file
            conversion_file_list: List of file dictionaries to convert
            output_ext: Optional output file extension
            settings: Optional settings object
        """
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)  # Persistent, file-based connection
        self.output_ext = output_ext
        self.settings = settings
        self.import_queue: Queue[str] = Queue()
        self.pending_exports: List[Dict[str, str]] = []
        self.one_in_one_out: bool = output_ext is not None

        self._populate_import_queue(conversion_file_list)

    def _populate_import_queue(
        self, conversion_file_list: List[Dict[str, Any]]
    ) -> None:
        """Populates the import queue with file paths.

        Args:
            conversion_file_list: List of file dictionaries containing paths
        """
        for file_info in conversion_file_list:
            self.import_queue.put(file_info["path"])

    def _generate_table_name(self, file_path: str) -> str:
        """Generates a unique table name for the imported file.

        Args:
            file_path: Path to the file being imported

        Returns:
            A unique table name based on the filename and a UUID
        """
        base = os.path.splitext(os.path.basename(file_path))[0]
        return f"{base}_{uuid.uuid4().hex[:8]}"

    def _import_file(self, file_path: str) -> str:
        """Imports a file into a DuckDB table.

        Args:
            file_path: Path to the file to import

        Returns:
            The name of the created table

        Raises:
            duckdb.Error: If the import operation fails
        """
        table_name = self._generate_table_name(file_path)
        import_sql = (
            f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}')"
        )
        self.conn.execute(import_sql)
        return table_name

    def _export_file(self, file_path: str, table_name: str) -> None:
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

        export_sql = (
            f"COPY {table_name} TO '{output_file}' (FORMAT '{self.output_ext}')"
        )
        self.conn.execute(export_sql)
        self.conn.execute(f"DROP TABLE {table_name}")

        self._log_success(file_path, output_file)

    def _log_success(self, input_file: str, output_file: str) -> None:
        """Logs successful file conversion if logger is available.

        Args:
            input_file: Path to the original input file
            output_file: Path to the newly created output file
        """
        if self.settings and hasattr(self.settings, "logger"):
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

    def set_output_extension(self, output_ext: str) -> None:
        """Sets the output file extension for exports.

        Args:
            output_ext: The file extension to use for exported files
        """
        self.output_ext = output_ext

    def close_connection(self, cleanup_db_file: bool = False) -> None:
        """Closes the DuckDB connection and optionally removes the DB file.

        Args:
            cleanup_db_file: If True, the database file will be deleted
        """
        self.conn.close()
        if cleanup_db_file and os.path.exists(self.db_path):
            os.remove(self.db_path)
