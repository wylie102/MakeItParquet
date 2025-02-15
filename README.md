# Make-it-Parquet! - The Ultimate Multi-Format Converter

A conversion tool to convert between popular data storage file types (CSV/TXT/TSV, JSON, Parquet, Excel) Powered by DuckDB

## Features

- **Auto-detection:** Automatically detects the input file type based on its extension.
- **Multiple Formats:** If you don't want to make everything parquet (Why not?!) Make-it-Parquet! also supportsSupports CSV, TXT, TSV, JSON, Parquet, and Excel conversions in both directions.
- **Conversions Such as:** CSV to Excel, Excel to CSV, CSV to Parquet, and Parquet to JSON etc. etc. Make-it-Parquet! is the ultimate multi format converter ensuring you can get your data into any format you need.
- **Interactive Options:** Prompts for Excel sheet and range if not specified.
- **Directory Conversion:** When converting a directory, the tool always creates a subfolder (named after the output type) in the output destination to store the converted files.
- **Flexible Aliasing:** Easily alias commands (e.g., `mp` for general use).

## Prerequisites

- Python 3.7+
- [DuckDB Python package](https://duckdb.org/docs/api/python/reference/)

## Running the Script

Assuming you are using [uv run](https://github.com/your/uv-run) (or a similar tool), you can run the script with:

```bash
uv run /path/to/make_it_parquet.py [OPTIONS]
```

## Usage

The basic usage from the command line is as follows:

```bash
Usage: mp <input_path> [OPTIONS]
```

**Arguments:**

- `input_path`  
  Path to a file or directory containing files.

**Options:**

- `-i, --input_type TEXT`  
  Override auto-detection of input file type.  
  Allowed values: csv, txt, tsv, json, parquet, pq, excel, ex.
- `-o, --output_type TEXT`  
  Desired output file type.  
  Allowed values: csv, tsv, json, parquet, pq, excel, ex.
- `-op, --output-path TEXT`  
  Output file (if input is a single file) or directory (if input is a folder).  
  For directory input, a subfolder named after the output type is always created.
- `-s, --sheet TEXT`  
  For Excel input: sheet number or sheet name to import (e.g. 1 or "Sheet1").
- `-c, --range TEXT`  
  For Excel input: cell range to import (e.g. A1:B2).
- `-d, --delimiter TEXT`  
  Defines the delimiter for TXT export. Pass 't' for tab-separated, 'c' for comma-separated, or provide a literal value.  
  If not provided, the tool will prompt you.

## Examples

- **Convert a Single Excel File to Parquet:**

  ```bash
  mp /path/to/file.xlsx -i excel -o pq
  ```

- **Convert All Files in a Folder to CSV:**

  ```bash
  mp /path/to/folder -o csv
  ```

- **Convert an Excel File to CSV with a Specified Sheet and Range:**

  ```bash
  mp /path/to/file.xlsx -i excel -s 1 -c A2:E7 -o csv
  ```

- **Convert Any File Type to Parquet:**

  ```bash
  mp /path/to/file_or_folder -o pq
  ```

- **Convert a CSV File to JSON (auto-detecting the input type):**

  ```bash
  mp /path/to/file.csv -o json
  ```

- **Convert all Supported Files in a Directory to TXT Format and Specify a Delimiter for TXT Export:**

  ```bash
  mp /path/to/folder -o txt -d t
  ```

## Shell Configuration

To simplify usage, you can set up an alias using `uv run` in your shell configuration.

### Bash

Add the following line to your `~/.bashrc`:

```bash
alias mp='uv run /path/to/make_it_parquet.py'
```

Then reload your shell:

```bash
source ~/.bashrc
```

### Zsh

Add the following line to your `~/.zshrc`:

```zsh
alias mp='uv run /path/to/make_it_parquet.py'
```

Then reload your shell:

```zsh
source ~/.zshrc
```

### Fish

For Fish shell, add the following function to your `~/.config/fish/config.fish`:

```fish
function mp
    uv run /path/to/make_it_parquet.py $argv
end
```

Then reload your configuration:

```fish
source ~/.config/fish/config.fish
```

## How It Works

1. **Input Processing:**  
   - If the input path is a file, Make-It-Parquet! will convert the file to the specified output format by simply changing the extension.
   - If the input is a directory, it will scan the folder to determine the majority file type (using predefined naming mappings) and generate an output directory name accordingly.

2. **File Conversions:**  
   - Conversion functions, defined in the tool's `conversions.py` module, handle the actual file format conversions using DuckDB SQL commands.
   - For Excel conversions, if the number of rows exceeds an effective limit, the export is paginated into multiple files.
   - Whether converting CSV to Excel, Excel to CSV, CSV to Parquet, Parquet to JSON, or any other supported file type, the tool ensures a smooth process that lets you conveniently convert data files from one format to another.

3. **CLI Options:**  
   - Input and output types can be overridden by CLI options (`-i` and `-o`).
   - For Excel inputs, `-s` and `-c` allow you to specify the sheet and cell range.
   - For TXT exports, use the `-d` option to select the delimiter (or the tool will prompt if not supplied).

## Dependencies

- Python 3.7+
- [DuckDB](https://duckdb.org)

## Licence

Licensed under the MIT Licence. See `LICENSE` for more information.

## Acknowledgements

- Thanks to DuckDB for providing a robust SQL engine for on-the-fly file conversions.
- Special thanks to contributors and users who helped refine Make-It-Parquet!

---

### Supported Conversions

Below is a comprehensive list of supported conversions:

- convert csv to txt
- convert csv to tsv
- convert csv to json
- convert csv to parquet
- convert csv to excel
- convert txt to csv
- convert txt to tsv
- convert txt to json
- convert txt to parquet
- convert txt to excel
- convert tsv to csv
- convert tsv to txt
- convert tsv to json
- convert tsv to parquet
- convert tsv to excel
- convert json to csv
- convert json to txt
- convert json to tsv
- convert json to parquet
- convert json to excel
- convert parquet to csv
- convert parquet to txt
- convert parquet to tsv
- convert parquet to json
- convert parquet to excel
- convert excel to csv
- convert excel to txt
- convert excel to tsv
- convert excel to json
- convert excel to parquet

Happy converting!
