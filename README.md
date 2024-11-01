# testing-duckdb
Scripts and test setup for the preparatory master's project fall 2024.

## Building the python package version of DuckDB
You should ideally do this from a virtual environment, so that you can have multiple versions of DuckDB ready at the same time (e.g. one with and one without materialization). 

To build DuckDB with JSON extension and the Python package, run (from the top of the `duckdb` repository):

```sh
GEN=ninja BUILD_JSON=1 BUILD_PYTHON=1 make OVERRIDE_GIT_DESCRIBE="v1.0.0"
```

Then, to install teh python package, run
```sh
pip install ./tools/pythonpkg
```


## Decompressing files
the `decomp.py` file will decompress a directory of subdirectories of `.bz2` files. The script cam be run from the terminal:
```sh
python decomp.py /path/to/topdir
```
