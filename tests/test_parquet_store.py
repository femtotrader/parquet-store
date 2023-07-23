from os import path
from parquet_store import ParquetStore
from pathlib import Path


def test_parquet_store():
    print(Path().absolute())
    path = Path(".")
    ps = ParquetStore(path)
    print(ps.libraries)
    lib = ps.library["path.to.symbols"]
    print(lib)
    symbols = lib.symbols
    print(symbols)
    data = lib.read("SYMBOL")
    print(data)
