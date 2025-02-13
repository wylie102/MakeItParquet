# Benchmarking os.stat() vs os.scandir().stat() for retrieving file sizes

import os
import time
from pathlib import Path

# Create a test directory and files
test_dir = "test_directory"
os.makedirs(test_dir, exist_ok=True)
file_paths = [f"{test_dir}/file{i}.txt" for i in range(10000)]

# Create test files with some dummy content
for file in file_paths:
    with open(file, "w") as f:
        f.write("Sample content")

# Measure performance using os.stat() for each file individually
start_time = time.time()
file_info_stat = [(Path(file).name, os.stat(file).st_size) for file in file_paths]
time_stat = time.time() - start_time

# Measure performance using os.scandir() (which caches file attributes)
start_time = time.time()
with os.scandir(test_dir) as entries:
    file_info_scandir = [(entry.name, entry.stat().st_size) for entry in entries if entry.is_file()]
time_scandir = time.time() - start_time

# Cleanup test files and directory
for file in file_paths:
    os.remove(file)
os.rmdir(test_dir)

# Return results
print(f"os.stat() on each file: {time_stat} seconds")
print(f"os.scandir() with .stat(): {time_scandir} seconds")
