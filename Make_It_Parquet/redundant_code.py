#! usr/bin/env python3

        mode = "bulk_import"

        while not self.import_queue.empty() or not self.export_queue.empty():
            if mode == "bulk_import":
                # While output_ext is not yet received, import files as fast as possible
                while not self.import_queue.empty() and self.output_ext is None:
                    file_info = self.import_queue.get()
                    self.logger.info(f"Bulk importing {file_info['name']}")
                    try:
                        # Import the file – you'll have a method like import_file() in your import class
                        imported_data = self.import_class.import_file(file_info["path"])
                    except Exception as e:
                        self.logger.error(f"Error importing {file_info['name']}: {e}")
                        self.import_queue.task_done()
                        continue
                    # Once imported, add to the export queue (even though we cannot export yet).
                    self.export_queue.put(
                        {
                            "data": imported_data,
                            "file_info": file_info,
                            "output_extension": self.output_ext,  # likely still None here
                        }
                    )
                    self.import_queue.task_done()

                # Check if the output extension has been set;
                # if so, we switch to draining mode.
                if self.output_ext is not None:
                    mode = "draining"
                else:
                    # If no more files to import, break out.
                    if self.import_queue.empty():
                        break

            elif mode == "draining":
                self.logger.info(
                    "Output extension received. Draining the export queue..."
                )
                # Process the exports that have accumulated.
                while not self.export_queue.empty():
                    export_item = self.export_queue.get()
                    try:
                        # Process export through your export class.
                        self.export_class.export_file(export_item)
                        self.logger.info(f"Exported {export_item['file_info']['name']}")
                    except Exception as e:
                        self.logger.error(
                            f"Error exporting {export_item['file_info']['name']}: {e}"
                        )
                    self.export_queue.task_done()
                # After draining, switch to balanced mode.
                mode = "balanced"

            elif mode == "balanced":
                # Now process one import then one export at a time.
                if not self.import_queue.empty():
                    file_info = self.import_queue.get()
                    self.logger.info(f"Balanced import of {file_info['name']}")
                    try:
                        imported_data = self.import_class.import_file(file_info["path"])
                    except Exception as e:
                        self.logger.error(f"Error importing {file_info['name']}: {e}")
                        self.import_queue.task_done()
                        continue
                    # Immediately process export.
                    try:
                        self.export_class.export_file(
                            {
                                "data": imported_data,
                                "file_info": file_info,
                                "output_extension": self.output_ext,
                            }
                        )
                        self.logger.info(f"Balanced export of {file_info['name']}")
                    except Exception as e:
                        self.logger.error(
                            f"Error exporting (balanced) {file_info['name']}: {e}"
                        )
                    self.import_queue.task_done()
                else:
                    # If both queues are empty, break out.
                    break

