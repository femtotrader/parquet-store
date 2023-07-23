#!/usr/bin env python3

import os
from pathlib import Path
import pandas as pd

EXTENSION = ".parquet"


class ParquetLibrary:
    def __init__(self, root_path, key, engine) -> None:
        self.root_path = root_path
        self.key = key.replace(".", "/")
        self.engine = engine

    @property
    def _path(self):
        return self.root_path / Path(self.key)

    @property
    def symbols(self):
        symbols = (self.root_path / self.key).glob(f"*{EXTENSION}")
        symbols = [symbol.stem for symbol in symbols]
        return pd.Series(symbols)

    def _symbol_path(self, symbol):
        return self._path / f"{symbol}{EXTENSION}"

    def read(self, symbol):
        return pd.read_parquet(self._symbol_path(symbol), engine=self.engine)

    def write(self, df, symbol):
        return df.to_parquet(self._symbol_path(symbol), engine=self.engine)


class LibraryAccessor:
    def __init__(self, root_path, engine) -> None:
        self.root_path = root_path
        self.engine = engine

    def __getitem__(self, key):
        return ParquetLibrary(self.root_path, key, self.engine)


class ParquetStore:
    def __init__(self, root_path, engine="fastparquet") -> None:
        self.root_path = Path(root_path)
        self.engine = engine

    @property
    def libraries(self):
        libs = set()
        n_root = len(self.root_path.parts)
        for root, dirs, files in os.walk(self.root_path):
            root = Path(root)
            if len(dirs) == 0:
                for file in files:
                    file = Path(file)
                    lib = root.parts[n_root:]
                    lib = ".".join(lib)
                    libs.add(lib)
        return pd.Series(list(libs))

    def create_library(self, lib):
        lib = lib.replace(".", "/")
        lib = self.root_path / Path(lib)
        return lib.mkdir(parents=True, exist_ok=True)

    @property
    def library(self):
        return LibraryAccessor(self.root_path, engine=self.engine)
