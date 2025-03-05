#!/usr/bin/env python3

from typing import List, Dict, Optional
from queue import Queue
import duckdb
import os
import uuid
from user_interface.settings import Settings


class ConversionManager:
    """
    Manages the conversion process using a single persistent DuckDB connection.

    - Files are imported one-by-one into uniquely named tables.
    - While the output extension is not yet set, imported file info is stored in a pending_exports list.
    - When the user sets the output extension, the current file is allowed to finish,
      then all pending files (in order) are exported.
    - After that, the process switches to one-in, one-out mode (import a file, immediately export it).
    """

    def __init__(
        self,
        db_path: str,
        conversion_file_list: List[Dict],
        output_ext: Optional[str] = None,
        settings: Optional[Settings] = None,
    ) -> None:
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)  # Persistent, file-based connection
        self.conversion_file_list = conversion_file_list
        self.output_ext = output_ext  # May be updated later via CLI
        self.settings = settings
        self.import_queue: Queue[str] = Queue()
        self._populate_import_queue()
        # Files imported before output_ext is set are held here (in order)
        self.pending_exports: List[Dict[str, str]] = []
        # one_in_one_out is False until output_ext is set during import
        self.one_in_one_out: bool = output_ext is not None

    def _populate_import_queue(self) -> None:
        """Populate the import queue with file paths from the conversion_file_list."""
        for file_info in self.conversion_file_list:
            self.import_queue.put(file_info["path"])

    def _generate_table_name(self, file_path: str) -> str:
        """Generate a unique table name based on the file name and a UUID."""
        base = os.path.splitext(os.path.basename(file_path))[0]
        return f"{base}_{uuid.uuid4().hex[:8]}"

    def _import_file(self, file_path: str) -> str:
        """
        Import a file by creating a table in DuckDB.
        Returns the generated table name.
        Uses a placeholder SQL statement (e.g. read_csv_auto) for the import.
        """
        table_name = self._generate_table_name(file_path)
        import_sql = (
            f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}')"
        )
        self.conn.execute(import_sql)
        return table_name

    def _export_file(self, file_path: str, table_name: str) -> None:
        """
        Export a table using a COPY command (with the user-specified output extension).
        After export, the table is dropped.
        A success message is logged via self.settings.logger.
        """
        base = os.path.splitext(os.path.basename(file_path))[0]
        output_file = os.path.join(
            os.path.dirname(file_path), f"{base}.{self.output_ext}"
        )
        export_sql = (
            f"COPY {table_name} TO '{output_file}' (FORMAT '{self.output_ext}')"
        )
        self.conn.execute(export_sql)
        self.conn.execute(f"DROP TABLE {table_name}")
        if self.settings and hasattr(self.settings, "logger"):
            self.settings.logger.info(
                f"File {file_path} successfully converted to {output_file}"
            )

    def run_conversion(self) -> None:
        """
        Process files from the import queue.

        - While output_ext is not set, each imported file is added to pending_exports.
        - Once output_ext is set (even if mid-import), the current file finishes importing;
          then, flush all pending_exports in order (export files 1, then 2, etc.).
        - After flushing, switch to one-in, one-out mode: each subsequent file is imported and immediately exported.
        """
        while not self.import_queue.empty():
            file_path = self.import_queue.get()
            table_name = self._import_file(file_path)

            if not self.one_in_one_out:
                if self.output_ext is None:
                    # Still in import-only phase; accumulate the imported file info.
                    self.pending_exports.append(
                        {"file_path": file_path, "table_name": table_name}
                    )
                else:
                    # Output extension was set while importing file; include it in the pending list.
                    self.pending_exports.append(
                        {"file_path": file_path, "table_name": table_name}
                    )
                    # Flush all pending files in order.
                    for pending in self.pending_exports:
                        self._export_file(pending["file_path"], pending["table_name"])
                    self.pending_exports.clear()
                    self.one_in_one_out = True
            else:
                # Already in one-in, one-out mode; export immediately after import.
                self._export_file(file_path, table_name)

            self.import_queue.task_done()

    def set_output_extension(self, output_ext: str) -> None:
        """
        Set the output extension. When this is called during the import phase,
        the system will finish the current file, then flush all previously imported files.
        """
        self.output_ext = output_ext

    def close_connection(self, cleanup_db_file: bool = False) -> None:
        """
        Closes the persistent DuckDB connection.
        Optionally, deletes the database file if cleanup_db_file is True.
        """
        self.conn.close()
        if cleanup_db_file and os.path.exists(self.db_path):
            os.remove(self.db_path)
