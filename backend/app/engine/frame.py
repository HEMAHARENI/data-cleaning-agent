"""Small helpers that let every cleaning step run unchanged on Pandas or Dask.

The pattern used across the engine: write the per-column logic as a plain
Pandas function, then run it either directly (Pandas) or per partition via
``map_partitions`` (Dask). Aggregates are collected lazily and resolved in a
single ``dask.compute`` call so a Dask job makes one pass per step, not one
pass per column.
"""
from __future__ import annotations

from typing import Any, Callable

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd

# Keep object dtype for text in Dask so Pandas and Dask partitions behave identically.
dask.config.set({"dataframe.convert-string": False})

NULL_TOKENS = frozenset(
    {"", "na", "n/a", "nan", "null", "none", "nil", "-", "--", "?", "missing", "undefined", "#n/a", "n.a.", "not available",
     "unknown", "error", "err", "#error", "#value!", "#ref!", "#div/0!", "#name?", "#num!", "invalid"}
)


def is_dask(obj: Any) -> bool:
    return isinstance(obj, (dd.DataFrame, dd.Series))


def compute(*objs: Any) -> tuple:
    """Resolve any mix of lazy Dask objects and plain values in one pass."""
    if any(is_dask(o) or hasattr(o, "__dask_graph__") for o in objs):
        return dask.compute(*objs)
    return objs


def nrows(df) -> int:
    return int(compute(df.shape[0])[0]) if is_dask(df) else int(len(df))


def apply_series(s, fn: Callable[[pd.Series], pd.Series], meta_dtype: Any = object):
    """Run a Pandas Series -> Series function on a Pandas or Dask series."""
    if is_dask(s):
        return s.map_partitions(fn, meta=pd.Series([], dtype=meta_dtype, name=s.name))
    return fn(s)


def apply_frame(df, fn: Callable[[pd.DataFrame], pd.DataFrame], meta: pd.DataFrame | None = None):
    if is_dask(df):
        return df.map_partitions(fn, meta=meta if meta is not None else df._meta)
    return fn(df)


def persist(df):
    """Materialise a Dask frame on the cluster so later steps don't recompute the whole graph."""
    return df.persist() if is_dask(df) else df


def head(df, n: int = 5000) -> pd.DataFrame:
    """Cheap sample for type inference: first rows of the first partitions."""
    if is_dask(df):
        return df.head(n, npartitions=min(df.npartitions, 4), compute=True)
    return df.head(n)


def is_text(dtype) -> bool:
    return pd.api.types.is_object_dtype(dtype) or pd.api.types.is_string_dtype(dtype)


def is_numeric(dtype) -> bool:
    return pd.api.types.is_numeric_dtype(dtype) and not pd.api.types.is_bool_dtype(dtype)


def to_py(v: Any) -> Any:
    """Make numpy / pandas scalars JSON-serialisable."""
    if v is None:
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        return None if np.isnan(v) or np.isinf(v) else round(float(v), 6)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if isinstance(v, (pd.Timestamp,)):
        return None if pd.isna(v) else v.isoformat()
    if v is pd.NaT:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v


def null_mask(s: pd.Series) -> pd.Series:
    """Null-token aware missing mask (treats 'N/A', '', 'null', ... as missing)."""
    if is_text(s.dtype):
        low = s.astype(str).str.strip().str.lower()
        return s.isna() | low.isin(NULL_TOKENS)
    return s.isna()
