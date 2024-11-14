import os
import sys
import subprocess
import glob
import shutil

def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py <number>")
        sys.exit(1)

    number = sys.argv[1]

    data_dir = './data'
    dbgen_dir = './data/TPC-H V3.0.1/dbgen'

    # Step 1: Delete all files ending with .tbl in ./data
    tbl_files = glob.glob(os.path.join(data_dir, '*.tbl'))
    for f in tbl_files:
        print(f"Deleting {f}")
        os.remove(f)

    # Step 2: Run dbgen command
    dbgen_executable = './dbgen'  # Changed from absolute to relative path
    cmd = [dbgen_executable, '-s', number, '-v']
    print(f"Running command: {' '.join(cmd)} in directory {dbgen_dir}")
    try:
        subprocess.run(cmd, check=True, cwd=dbgen_dir)
    except subprocess.CalledProcessError as e:
        print(f"Error running dbgen: {e}")
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"dbgen executable not found: {e}")
        sys.exit(1)

    # Step 3: Move .tbl files from dbgen_dir to data_dir
    dbgen_tbl_files = glob.glob(os.path.join(dbgen_dir, '*.tbl'))
    for f in dbgen_tbl_files:
        dest = os.path.join(data_dir, os.path.basename(f))
        print(f"Moving {f} to {dest}")
        shutil.move(f, dest)

if __name__ == '__main__':
    main()
