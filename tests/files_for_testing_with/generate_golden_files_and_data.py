import json
import os
import duckdb
from pathlib import Path

# Define a permanent directory for the sample files.
SAMPLE_DIR = (
    Path(
        "/Users/wylie/Desktop/Projects/DuckConvert/tests/files_for_testing_with/generate_golden_files"
    ).parent
    / "sample_files"
)
SAMPLE_DIR.mkdir(exist_ok=True)


def create_sample_files():
    """Creates permanent test files in various formats using a single DuckDB connection."""
    file_paths = {
        "txt": SAMPLE_DIR / "sample.txt",
        "csv": SAMPLE_DIR / "sample.csv",
        "tsv": SAMPLE_DIR / "sample.tsv",
        "parquet": SAMPLE_DIR / "sample.parquet",
        "json": SAMPLE_DIR / "sample.json",
        "xlsx": SAMPLE_DIR / "sample.xlsx",
    }

    # Define dataset queries.
    small_query = "SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob'"
    small_data = duckdb.sql(small_query)
    medium_data = duckdb.sql(
        "SELECT range AS id, 'Person_' || range AS name FROM range(100)"
    )
    large_data = duckdb.sql(
        "SELECT range AS id, 'Person_' || range AS name, range * 10 AS value FROM range(10000)"
    )

    # Use the relation API to write files with converted file paths.
    small_data.write_csv(str(file_paths["csv"]), header=True)
    medium_data.write_csv(str(file_paths["tsv"]), header=True, sep="\t")
    large_data.write_csv(str(file_paths["txt"]), header=False, sep="\t")
    large_data.write_parquet(str(file_paths["parquet"]))

    # For JSON and Excel output, install & load the Excel extension.
    with duckdb.connect(database=":memory:") as con:
        con.install_extension("excel")
        con.load_extension("excel")
        con.execute(
            f"COPY ({small_query}) TO '{str(file_paths['json'])}' (FORMAT JSON)"
        )
        con.execute(
            f"COPY ({small_query}) TO '{str(file_paths['xlsx'])}' (FORMAT XLSX)"
        )
    return file_paths


def generate_golden_info(file_paths):
    info = {}
    for file_type, path in file_paths.items():
        stat = os.stat(path)
        info[file_type] = {
            "file_size": stat.st_size,
            "file_name": path.name,
            "file_extension": path.suffix,
            "is_file": path.is_file(),
        }
    return info


if __name__ == "__main__":
    files = create_sample_files()
    golden_info = generate_golden_info(files)
    # Pretty-print the golden dictionary to the console.
    print(json.dumps(golden_info, indent=2))
