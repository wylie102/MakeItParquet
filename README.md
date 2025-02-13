# DataTad

A conversion tool to convert between popular data storage file types (CSV/TXT/TSV, JSON, Parquet, Excel) using DuckDB's Python API.

## Features

- **Auto-detection:** Automatically detects the input file type based on its extension.
- **Multiple Formats:** Supports CSV, TXT, TSV, JSON, Parquet, and Excel.
- **Interactive Options:** Prompts for Excel sheet and range if not specified.
- **Directory Conversion:** When converting a directory, the tool always creates a subfolder (named after the output type) in the output destination to store the converted files.
- **Flexible Aliasing:** Easily alias commands (e.g., `dt` for general use and `mip` for "make it Parquet").
- **Flexible Aliasing:** Easily alias commands (e.g., `dt` for general use and `mip` for "make it Parquet").

## Prerequisites

- Python 3.7+
- [DuckDB Python package](https://duckdb.org/docs/api/python/reference/)

Note: DuckDB is not yet available via pip. Please refer to the [official DuckDB documentation](https://duckdb.org/docs) for installation instructions or build it from source.

## Running the Script

Assuming you are using [uv run](https://github.com/your/uv-run) (or a similar tool), you can run the script with:

```bash
uv run /path/to/DataTad.py [OPTIONS]
```

## Usage

The basic usage from the command line is as follows:

```bash
Usage: dt <input_path> [OPTIONS]
Usage: dt <input_path> [OPTIONS]

Arguments:
  input_path              Path to a file or directory containing files.

Options:
  -i, --input_type TEXT   Override auto-detection of input file type.
                          Allowed values: csv, txt, tsv, json, parquet, pq, excel, ex.
  -o, --output_type TEXT  Desired output file type.
                          Allowed values: csv, tsv, json, parquet, pq, excel, ex.
  -op, --output-path TEXT
                          Output file (if input is a single file) or directory
                          (if input is a folder). For directory input, a subfolder
                          named after the output type is always created.
  -s, --sheet TEXT        For Excel input: sheet number or sheet name to import (e.g. 1 or "Sheet1").
  -c, --range TEXT        For Excel input: cell range to import (e.g. A1:B2).
  -d, --delimiter TEXT    Defines the delimiter for TXT export. Pass 't' for tab-separated, 'c' for comma-separated, or provide a literal value. If not provided, the tool will prompt you.
```

## Examples

- **Convert a Single Excel File to Parquet:**

  ```bash
  dt /path/to/file.xlsx -i excel -o pq
  dt /path/to/file.xlsx -i excel -o pq
  ```

- **Convert All Files in a Folder to CSV:**

  ```bash
  dt /path/to/folder -o csv
  dt /path/to/folder -o csv
  ```

- **Convert an Excel File to CSV with a Specified Sheet and Range:**

  ```bash
  dt /path/to/file.xlsx -i excel -s 1 -c A2:E7 -o csv
  ```

- **Convert Any File Type to Parquet Using the "mip" Alias:**

  ```bash
  mip /path/to/file_or_folder
  ```

- **Convert a CSV File to JSON (auto-detecting the input type):**

  ```bash
  dt /path/to/file.csv -o json
  ```

- **Convert all Supported Files in a Directory to TXT Format and Specify a Delimiter for TXT Export:**

  ```bash
  dt /path/to/folder -o txt -d t
  ```

## Shell Configuration

To simplify usage, you can set up aliases using `uv run` in your shell configuration.

### Bash

Add the following lines to your `~/.bashrc`:

```bash
alias dt='uv run /path/to/DataTad.py'
alias mip='uv run /path/to/DataTad.py -o pq'
```

Then reload your shell:

```bash
source ~/.bashrc
```

### Zsh

Add the following lines to your `~/.zshrc`:

```zsh
alias dt='uv run /path/to/duckconverter.py'
alias mip='uv run /path/to/duckconverter.py -o pq'
```

Then reload your shell:

```zsh
source ~/.zshrc
```

### Fish

For Fish shell, add the following functions to your `~/.config/fish/config.fish`:

```fish
function dt
    uv run /path/to/DataTad.py $argv
end

function mip
    uv run /path/to/DataTad.py -o pq $argv
end
```

Then reload your configuration:

```fish
source ~/.config/fish/config.fish
```

## How It Works

1. **Input Processing:**  
   - If the input path is a file, DataTad will convert the file to the specified output format by simply changing the extension.
   - If the input is a directory, it will scan the folder to determine the majority file type (using predefined naming mappings) and generate an output directory name accordingly.

2. **File Conversions:**  
   - Conversion functions, defined in the tool's `conversions.py` module, handle the actual file format conversions using DuckDB SQL commands.
   - For Excel conversions, if the number of rows exceeds an effective limit, the export is paginated into multiple files.

3. **CLI Options:**  
   - Input and output types can be overridden by CLI options (`-i` and `-o`).
   - For Excel inputs, `-s` and `-c` allow you to specify the sheet and cell range.
   - For TXT exports, use the `-d` option to select the delimiter (or the tool will prompt if not supplied).

## Dependencies

- Python 3.6+
- [DuckDB](https://duckdb.org)

## Licence

Distributed under the MIT Licence. See `LICENSE` for more information.

## Acknowledgements

- Thanks to DuckDB for providing a robust SQL engine for on-the-fly file conversions.
- Special thanks to contributors and users who helped refine DataTad.

Happy converting!

