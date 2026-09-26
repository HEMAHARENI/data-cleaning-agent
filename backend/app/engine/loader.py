"""File loading with automatic Pandas -> Dask switching by file size."""
from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import pandas as pd

from ..config import settings

SUPPORTED = {".csv", ".tsv", ".txt", ".parquet", ".json", ".jsonl", ".xlsx", ".xls"}
# Read text formats as raw strings: type inference is done by the normalizer, identically for both engines.
_CSV_KW = dict(dtype=object, keep_default_na=False, na_values=[], skipinitialspace=False)


def choose_engine(size_bytes: int, requested: str = "auto", threshold_mb: float | None = None) -> tuple[str, str]:
    thr = settings.dask_threshold_mb if threshold_mb is None else threshold_mb
    size_mb = size_bytes / (1024 * 1024)
    if requested in ("pandas", "dask"):
        return requested, f"Engine forced to {requested} by configuration ({size_mb:.1f} MB file)."
    if size_mb > thr:
        return "dask", f"{size_mb:.1f} MB exceeds the {thr:g} MB threshold -> distributed Dask cluster."
    return "pandas", f"{size_mb:.1f} MB is within the {thr:g} MB threshold -> in-memory Pandas."


def _sep(ext: str) -> str:
    return "\t" if ext == ".tsv" else ","


def load_pandas(path: Path, nrows: int | None = None) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in (".csv", ".tsv", ".txt"):
        return pd.read_csv(path, sep=_sep(ext), nrows=nrows, **_CSV_KW)
    if ext == ".parquet":
        df = pd.read_parquet(path)
        return df.head(nrows) if nrows else df
    if ext in (".json", ".jsonl"):
        df = pd.read_json(path, lines=ext == ".jsonl", dtype=False)
        return df.head(nrows) if nrows else df
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(path, dtype=object, keep_default_na=False, na_values=[], nrows=nrows)
    raise ValueError(f"Unsupported file type: {ext}")


def _blocksize(path: Path) -> int:
    """Aim for >= 2 partitions per worker thread so the whole cluster stays busy, capped by DQ_DASK_BLOCKSIZE."""
    from dask.utils import parse_bytes

    cap = parse_bytes(settings.dask_blocksize)
    slots = max(1, settings.dask_local_workers * settings.dask_threads_per_worker) * 2
    return int(max(8 * 1024 * 1024, min(cap, path.stat().st_size / slots)))


def load(path: Path, engine: str):
    ext = path.suffix.lower()
    if ext not in SUPPORTED:
        raise ValueError(f"Unsupported file type '{ext}'. Supported: {', '.join(sorted(SUPPORTED))}")
    if engine == "pandas":
        return load_pandas(path)
    if ext in (".csv", ".tsv", ".txt"):
        return dd.read_csv(str(path), sep=_sep(ext), blocksize=_blocksize(path), **_CSV_KW)
    if ext == ".parquet":
        return dd.read_parquet(str(path))
    # JSON / Excel have no chunked reader: load once, then distribute.
    pdf = load_pandas(path)
    nparts = max(1, int(path.stat().st_size / (64 * 1024 * 1024)))
    return dd.from_pandas(pdf, npartitions=nparts)


def preview(path: Path, n: int = 25) -> pd.DataFrame:
    return load_pandas(path, nrows=n)
