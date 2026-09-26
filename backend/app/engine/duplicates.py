"""Duplicate detection: exact rows, business-key duplicates and near-duplicates.

Near-duplicates are found with a canonical row signature (case-folded,
punctuation/whitespace stripped text, rounded numbers). This catches
"John  Smith, JOHN@X.COM" vs "john smith, john@x.com" and scales to Dask
because it is a hash-based drop_duplicates rather than a pairwise comparison.
"""
from __future__ import annotations

from typing import Any

import pandas as pd

from .frame import apply_frame, compute, is_dask, nrows, persist, to_py

SIG = "__dq_signature__"


def is_code(nunique: int, all_integers: bool, non_null: int) -> bool:
    """A numeric column of categorical codes: few distinct whole numbers, each repeated many times."""
    return all_integers and nunique <= 12 and non_null >= 100 * max(nunique, 1)


def is_id_like(col: str) -> bool:
    c = str(col).lower()
    return c == "id" or c.endswith("_id") or c.endswith("_key") or c in {"uuid", "guid"}


def _signature_fn(cols: list[str]):
    def fn(part: pd.DataFrame) -> pd.DataFrame:
        pieces = []
        for c in cols:
            s = part[c]
            if pd.api.types.is_float_dtype(s.dtype):
                s = s.round(2)
            # Missing values must become "" explicitly: pandas' str dtype would propagate NaN into the signature.
            txt = s.astype(object).where(s.notna(), "").astype(str)
            if not pd.api.types.is_numeric_dtype(s.dtype):
                txt = txt.str.casefold().str.replace(r"[^0-9a-z@.]", "", regex=True)
            pieces.append(txt)
        sig = pieces[0] if pieces else pd.Series("", index=part.index)
        for p in pieces[1:]:
            sig = sig + "|" + p
        out = part.copy()
        out[SIG] = sig
        return out

    return fn


def _examples(df: pd.DataFrame, subset: list[str] | None, limit: int = 5) -> list[dict]:
    dup_mask = df.duplicated(subset=subset, keep=False)
    if not dup_mask.any():
        return []
    sample = df[dup_mask].head(limit * 2)
    return [{k: to_py(v) for k, v in row.items() if k != SIG} for row in sample.to_dict(orient="records")]


def deduplicate(df, cfg: dict) -> tuple[Any, dict]:
    issues, report = [], {"examples": {}}
    n0 = nrows(df)
    user_keys = [k for k in (cfg.get("key_columns") or []) if k in df.columns]

    if cfg.get("exact", True):
        if not is_dask(df):
            report["examples"]["exact"] = _examples(df, None)
        df = persist(df.drop_duplicates())
        n1 = nrows(df)
        if n0 - n1:
            issues.append(dict(step="duplicates", column="*", issue="exact duplicate rows", count=n0 - n1, action="removed (kept first)"))
    else:
        n1 = n0

    n2 = n1
    if user_keys:
        if not is_dask(df):
            report["examples"]["key"] = _examples(df, user_keys)
        df = persist(df.drop_duplicates(subset=user_keys, keep="first"))
        n2 = nrows(df)
        if n1 - n2:
            issues.append(dict(step="duplicates", column=", ".join(user_keys), issue="duplicate business keys", count=n1 - n2, action="removed (kept first)"))

    n3 = n2
    if cfg.get("near", True):
        # ID columns stay in the signature by default so distinct transactions that happen to share
        # item/qty/date are never merged. Opt in to ignore_id_columns when re-entered records get new IDs.
        ignore_ids = cfg.get("ignore_id_columns", False)
        sig_cols = user_keys or [
            c for c in df.columns
            if not (ignore_ids and is_id_like(c))
        ]
        if sig_cols:
            meta = df._meta.assign(**{SIG: pd.Series([], dtype=object)}) if is_dask(df) else None
            with_sig = apply_frame(df, _signature_fn(sig_cols), meta)
            if not is_dask(with_sig):
                report["examples"]["near"] = _examples(with_sig, [SIG])
            df = persist(with_sig.drop_duplicates(subset=[SIG], keep="first").drop(columns=[SIG]))
            n3 = nrows(df)
            if n2 - n3:
                issues.append(dict(step="duplicates", column="*", issue="near-duplicate rows (differ only in case/spacing/punctuation)", count=n2 - n3, action="removed (kept first)"))

    report.update(rows_before=n0, rows_after=n3, removed=n0 - n3, issues=issues)
    return df, report


def duplicate_rows(df) -> int:
    n = nrows(df)
    return n - nrows(df.drop_duplicates())


__all__ = ["deduplicate", "duplicate_rows", "compute"]
