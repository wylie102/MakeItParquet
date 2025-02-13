# Structure

## cli_interface.py

### Settings class

- Class to parse CLI arguments and store them, also creates path object from the input path provided, and discerns and stores whether targat object is a file or directory.

- Settings also initiates an asynchronous logging process.

## DataTad.py

Settings class is imported from cli_interface.py

### Small main function

- Creates an instance of the Settings class.

- The settings class parses the CLI arguments, identifies the target file/folder and stores information about it.

- Next main calls create_conversion_manager and passes the settings instance into it. The correct conversion manager is created based on wether the target of the input path is a file or a directory.

## conversion_manager.py

### ConversionManagerClass (and sub classes)

- The conversion manager will scan and store information about the file(s) and use this to populate an input queue and an output queue and call the correct import and export classes.

- Import is started and the user input on the output format is requested asynchronously. Once output format is selected and validated output commands are queued and executed.

- Only one import or one export process is happening in DuckDB at any given time.

- Once the last file is converted the connection to DuckDB is terminated,  any temporary files are deleded, and logging is terminated before exit.

## converters.py

- contains various import and export classes needed for various import/export combinations. These should work of connections to DuckDB created as conn objects.

- Excel specific functions which are more complex are handled in excel_utilities.py

## excel_utilities.py

- Contains the specific functions and classes needed to handle excel input and output.
