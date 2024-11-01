import os
import bz2
import argparse


def decompress_bz2_file(path: str):
    decompressed_file_path = path[:-4]

    with bz2.open(path, 'rb') as compressed_file:
        with open(decompressed_file_path, 'wb') as decompressed_file:
            decompressed_file.write(compressed_file.read())


# Set up argument parsing for topdir
parser = argparse.ArgumentParser(
    description="Decompress bz2 files in specified directory.")
parser.add_argument('topdir', type=str,
                    help="Top directory containing subdirectories with bz2 files")
args = parser.parse_args()
top_dir = args.topdir

# Count the number of subdirectories in topdir
subdir_count = sum([1 for entry in os.scandir(top_dir) if entry.is_dir()])

finished = 0

for dirpath, _, files in os.walk(top_dir):
    for file_name in files:
        if file_name.endswith('bz2'):
            file_path = os.path.join(dirpath, file_name)
            decompress_bz2_file(path=file_path)
            # print(f"Decompress: {file_path}")
            os.remove(file_path)
    finished += 1
    print(f"Finished with {finished}/{subdir_count} dirs")
