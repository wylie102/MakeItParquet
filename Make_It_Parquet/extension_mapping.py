#! /usr/bin/env python3

import Make_It_Parquet.converters as conv

# Allowed file extensions.
ALLOWED_FILE_EXTENSIONS = {".csv", "tsv", "txt", ".json", ".parquet", ".xlsx"}

# Alias to extension map.
ALIAS_TO_EXTENSION_MAP: dict[str, str] = {
    "csv": ".csv",
    "txt": ".txt",
    "tsv": ".tsv",
    "json": ".json",
    "js": ".json",
    "parquet": ".parquet",
    "pq": ".parquet",
    "excel": ".xlsx",
    "ex": ".xlsx",
    "xlsx": ".xlsx",
}

# Reverse the alias to extension map.
EXTENSION_TO_ALIAS_MAP: dict[str, str] = {
    v: k for k, v in ALIAS_TO_EXTENSION_MAP.items()
}

# Import class map.
import_class_map = {
    ".csv": conv.CSVInput,
    ".tsv": conv.TsvInput,
    ".txt": conv.TxtInput,
    ".json": conv.JSONInput,
    ".parquet": conv.ParquetInput,
}
