# Structure

## cli_interface.py

### Settings class

- Parses CLI arguments and resolves the input path.
- Discerns whether the target is a file or directory.
- Initialises asynchronous logging for the application.

## Make-it-Parquet!.py

Settings class is imported from cli_interface.py

### Small main function

- Instantiates the Settings class to parse CLI arguments and determine the target type.
- Calls `create_conversion_manager` (which chooses either the file or directory conversion manager).
- Triggers the conversion process.

## conversion_manager.py

### Conversion Manager and Subclasses

- **BaseConversionManager**:
  - Handles common operations such as format validation, queue management, and dynamic generation of input/output classes.
- **FileConversionManager**:
  - Manages conversion for a single file. Validates the file extension and starts an asynchronous import process.
- **DirectoryConversionManager**:
  - Scans directories to group and order files (e.g. by size) for batch conversion.

*Note:* Legacy code is commented out to preserve ideas for future features as the design shifts toward a more robust object-oriented framework.

## converters.py

### Conversion Classes and Factories

- Defines the base connection classes:
  - **BaseInputConnection** and **BaseOutputConnection** encapsulate the Make-it-Parquet! connection logic.
- Provides specialised classes for each format:
  - Input: CSVInput, JSONInput, ParquetInput, TsvInput, TxtInput, ExcelInputUntyped, and ExcelInputTyped.
    - Output: CSVOutput, JSONOutput, ParquetOutput, TsvOutput, TxtOutput, and ExcelOutput.
- Legacy conversion approaches are present as commented-out code and will be integrated into the new OOP design as refactoring proceeds.

## excel_utilities.py

### Excel Utilities

- Provides functions to build Excel options clauses and SQL queries for reading Excel files.
- Offers functions to export Excel files while handling type inference and paginated exports.
- Manages loading of the Excel extension for Make-it-Parquet!.

These utilities support the converters and are key to handling the complexity of Excel interactions.
