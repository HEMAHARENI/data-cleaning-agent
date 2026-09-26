"""Profiling, rule-based validation and the data-quality score."""
from __future__ import annotations

from typing import Any

import pandas as pd

from .frame import compute, head, is_dask, is_numeric, is_text, null_mask, nrows, persist, to_py

RULE_TYPES = ("not_null", "unique", "range", "regex", "allowed_values", "min_length", "less_or_equal")


# -------------------------------------------------------------------- profile
def profile(df, raw: bool = False) -> dict:
    n = nrows(df)
    cols = list(df.columns)
    lazy: list[Any] = []
    for c in cols:
        s = df[c]
        if raw and is_text(s.dtype):
            lazy.append(s.map_partitions(null_mask, meta=(c, bool)).sum() if is_dask(s) else null_mask(s).sum())
        else:
            lazy.append(s.isna().sum())
        lazy.append(s.nunique_approx() if is_dask(s) else s.nunique(dropna=True))
    num_cols = [c for c in cols if is_numeric(df[c].dtype)]
    for c in num_cols:
        lazy.extend([df[c].min(), df[c].max(), df[c].mean()])
    vals = compute(*lazy)
    sample = head(df, 2000)
    out_cols, missing_total = [], 0
    for i, c in enumerate(cols):
        nulls, uniq = int(vals[2 * i]), int(vals[2 * i + 1])
        missing_total += nulls
        entry = {
            "name": str(c),
            "dtype": str(df[c].dtype),
            "missing": nulls,
            "missing_pct": round(100 * nulls / n, 2) if n else 0.0,
            "unique": uniq,
            "unique_approx": is_dask(df),
            "sample": [to_py(v) for v in sample[c].dropna().astype(object).unique()[:4].tolist()] if c in sample else [],
        }
        if c in num_cols:
            j = 2 * len(cols) + 3 * num_cols.index(c)
            entry.update(min=to_py(vals[j]), max=to_py(vals[j + 1]), mean=to_py(vals[j + 2]))
        out_cols.append(entry)
    cells = n * len(cols)
    return {"rows": n, "n_columns": len(cols), "cells": cells, "missing_cells": missing_total, "columns": out_cols}


# ------------------------------------------------------------------ validation
def _rule_mask(df, r: dict):
    c = r["column"]
    s = df[c]
    kind = r["rule"]
    if kind == "not_null":
        return s.isna()
    if kind == "range":
        lo, hi = r.get("min"), r.get("max")
        if pd.api.types.is_datetime64_any_dtype(s.dtype):
            lo = pd.Timestamp(lo) if lo not in (None, "") else None
            hi = pd.Timestamp(hi) if hi not in (None, "") else None
        else:
            lo = float(lo) if lo not in (None, "") else None
            hi = float(hi) if hi not in (None, "") else None
        m = s.isna() & False
        if lo is not None:
            m = m | (s < lo)
        if hi is not None:
            m = m | (s > hi)
        return m.fillna(False)
    if kind == "regex":
        pat = r.get("pattern", ".*")
        return (~s.isna()) & ~s.astype(str).str.fullmatch(pat).fillna(False)
    if kind == "allowed_values":
        allowed = [str(v).strip() for v in (r.get("values") or [])]
        return (~s.isna()) & ~s.astype(str).isin(allowed)
    if kind == "min_length":
        return (~s.isna()) & (s.astype(str).str.len() < int(r.get("min", 1)))
    if kind == "less_or_equal":  # cross-column: column <= other (e.g. pickup <= dropoff)
        other = df[r["other"]]
        return (s > other).fillna(False)
    if kind == "unique":
        if is_dask(df):
            return None  # counted, not row-masked, on Dask
        return (~s.isna()) & s.duplicated(keep="first")
    raise ValueError(f"Unknown rule type {kind}")


def _describe(r: dict) -> str:
    k = r["rule"]
    if k == "range":
        lo = r.get("min") if r.get("min") not in (None, "") else "-∞"
        hi = r.get("max") if r.get("max") not in (None, "") else "∞"
        return f"{r['column']} in [{lo}, {hi}]"
    if k == "regex":
        return f"{r['column']} matches /{r.get('pattern')}/"
    if k == "allowed_values":
        return f"{r['column']} in {{{', '.join(map(str, r.get('values') or []))}}}"
    if k == "min_length":
        return f"len({r['column']}) >= {r.get('min')}"
    if k == "less_or_equal":
        return f"{r['column']} <= {r.get('other')}"
    return f"{r['column']} is {k.replace('_', ' ')}"


def validate(df, rules: list[dict], quarantine: bool = True) -> tuple[Any, Any, dict]:
    n = nrows(df)
    results, masks = [], []
    valid_rules = []
    for r in rules or []:
        if r.get("column") not in df.columns:
            results.append({"rule": _describe(r) if "column" in r else str(r), "column": r.get("column"), "type": r.get("rule"),
                            "passed": False, "failed": None, "error": "column not found after cleaning"})
            continue
        if r.get("rule") == "less_or_equal" and r.get("other") not in df.columns:
            results.append({"rule": str(r), "column": r.get("column"), "type": r.get("rule"), "passed": False, "failed": None, "error": "comparison column not found"})
            continue
        if r.get("rule") not in RULE_TYPES:
            results.append({"rule": str(r), "column": r.get("column"), "type": r.get("rule"), "passed": False, "failed": None, "error": "unknown rule"})
            continue
        valid_rules.append(r)

    lazy = []
    for r in valid_rules:
        m = _rule_mask(df, r)
        masks.append(m)
        if m is None:
            s = df[r["column"]]
            lazy.append((~s.isna()).sum() - s.nunique_approx())
        else:
            lazy.append(m.sum())
    counts = compute(*lazy) if lazy else ()

    for r, m, cnt in zip(valid_rules, masks, counts):
        cnt = max(0, int(cnt))
        entry = {"rule": _describe(r), "column": r["column"], "type": r["rule"], "passed": cnt == 0, "failed": cnt,
                 "failed_pct": round(100 * cnt / n, 3) if n else 0.0}
        if cnt and m is not None and not is_dask(df):
            entry["examples"] = [to_py(v) for v in df.loc[m, r["column"]].head(5).tolist()]
        results.append(entry)

    rejected = None
    if quarantine and any(m is not None for m in masks):
        reason = None
        any_mask = None
        for r, m in zip(valid_rules, masks):
            if m is None:
                continue
            label = _describe(r)
            piece = m.map({True: label + "; ", False: ""}, meta=(None, object)) if is_dask(m) else m.map({True: label + "; ", False: ""})
            reason = piece if reason is None else reason + piece
            any_mask = m if any_mask is None else (any_mask | m)
        rejected = df[any_mask].assign(dq_failed_rules=reason[any_mask])
        df = df[~any_mask]
        df, rejected = persist(df), persist(rejected)

    n_rej = nrows(rejected) if rejected is not None else 0
    return df, rejected, {"results": results, "rows_valid": nrows(df), "rows_rejected": n_rej}


def builtin_checks(df) -> list[dict]:
    """Post-conditions on the final output."""
    n_valid = nrows(df)
    nulls_left = int(compute(df.isna().sum().sum())[0])
    dups_left = n_valid - nrows(df.drop_duplicates())
    return [
        {"rule": "no missing values remain", "column": "*", "type": "builtin", "passed": nulls_left == 0, "failed": nulls_left},
        {"rule": "no exact duplicate rows remain", "column": "*", "type": "builtin", "passed": dups_left == 0, "failed": dups_left},
    ]


# ---------------------------------------------------------------------- score
def score(rows: int, n_cols: int, missing: int, dup_rows: int, invalid_cells: int) -> dict:
    cells = max(rows * n_cols, 1)
    completeness = 1 - missing / cells
    uniqueness = 1 - dup_rows / max(rows, 1)
    validity = 1 - min(invalid_cells, cells) / cells
    overall = 0.4 * completeness + 0.3 * uniqueness + 0.3 * validity
    pct = lambda x: round(100 * max(0.0, x), 2)  # noqa: E731
    return {"completeness": pct(completeness), "uniqueness": pct(uniqueness), "validity": pct(validity), "overall": pct(overall)}
