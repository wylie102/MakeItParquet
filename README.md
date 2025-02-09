# DuckConvert

A conversion tool to convert between popular data storage file types (CSV/TXT/TSV, JSON, Parquet, Excel) using DuckDB's Python API.

## Features

- **Auto-detection:** Automatically detects the input file type based on its extension.
- **Multiple Formats:** Supports CSV, TXT, TSV, JSON, Parquet, and Excel.
- **Interactive Options:** Prompts for Excel sheet and range if not specified.
- **Directory Conversion:** When converting a directory, the tool always creates a subfolder (named after the output type) in the output destination to store the converted files.
- **Flexible Aliasing:** Easily alias commands (e.g., `dc` for general use and `mip` for "make it Parquet").

## Prerequisites

- Python 3.7+
- [DuckDB Python package](https://duckdb.org/docs/api/python/reference/)

Install DuckDB via pip:

```bash
pip install duckdb
```

## Running the Script

Assuming you are using [uv run](https://github.com/your/uv-run) (or a similar tool), you can run the script with:

```bash
uv run /path/to/duckconverter.py [OPTIONS]
```

## Usage

```bash
Usage: dc <input_path> [OPTIONS]

Arguments:
  input_path              Path to a file or directory containing files.

Options:
  -i, --input-type TEXT   Override auto-detection of input file type.
                          Allowed values: csv, txt, tsv, json, parquet, pq, excel, ex.
  -o, --output-type TEXT  Desired output file type.
                          Allowed values: csv, tsv, json, parquet, pq, excel, ex.
  -op, --output-path TEXT
                          Output file (if input is a single file) or directory
                          (if input is a folder). For directory input, a subfolder
                          named after the output type is always created.
  -s, --sheet TEXT        For Excel input: sheet number or sheet name to import (e.g. 1 or "Sheet1").
  -c, --range TEXT        For Excel input: cell range to import (e.g. A1:B2).
```

## Examples

- **Convert a Single Excel File to Parquet:**

  ```bash
  dc /path/to/file.xlsx -i excel -o pq
  ```

- **Convert All Files in a Folder to CSV:**

  ```bash
  dc /path/to/folder -o csv
  ```

- **Convert an Excel File to CSV with a Specified Sheet and Range:**

  ```bash
  dc /path/to/file.xlsx -i excel -s 1 -c A2:E7 -o csv
  ```

- **Convert Any File Type to Parquet Using the "mip" Alias:**

  ```bash
  mip /path/to/file_or_folder
  ```

## Shell Configuration

To simplify usage, you can set up aliases using `uv run` in your shell configuration.

### Bash

Add the following lines to your `~/.bashrc`:

```bash
alias dc='uv run /path/to/duckconverter.py'
alias mip='uv run /path/to/duckconverter.py -o pq'
```

Then reload your shell:

```bash
source ~/.bashrc
```

### Zsh

Add the following lines to your `~/.zshrc`:

```zsh
alias dc='uv run /path/to/duckconverter.py'
alias mip='uv run /path/to/duckconverter.py -o pq'
```

Then reload your shell:

```zsh
source ~/.zshrc
```

### Fish

For Fish shell, add the following functions to your `~/.config/fish/config.fish`:

```fish
function dc
    uv run /path/to/duckconverter.py $argv
end

function mip
    uv run /path/to/duckconverter.py -o pq $argv
end
```

Then reload your configuration:

```fish
source ~/.config/fish/config.fish
```

## Credits and Acknowledgements

- **DuckDB:** This tool leverages the [DuckDB Python API](https://duckdb.org/docs/api/python/reference/) for efficient data conversion. Please refer to DuckDB’s documentation for more details.
- **Contributors:** This project was developed with community contributions in mind. Feel free to submit issues or pull requests.

## License

This project is licensed under the MIT License. See the LICENSE section below for details.

