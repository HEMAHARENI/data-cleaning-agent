"""Outlier detection on numeric columns: IQR, z-score or robust MAD.

Actions:
  cap    - winsorise values to the detection bounds (default)
  null   - set outliers to missing so the imputation step fills them
  remove - drop rows containing any outlier
  flag   - keep values, add a boolean `dq_outlier` column
"""
from __future__ import annotations

import re
from typing import Any

import numpy as np
import pandas as pd

from .duplicates import is_code, is_id_like
from .frame import compute, is_dask, is_numeric, persist, to_py

FLAG = "dq_outlier"


_NOT_MEASURES = re.compile(r"(^|_)(lat|lon|lng|latitude|longitude|zip|zipcode|postcode|postal|pin|pincode|phone|year|code)(_|$)")


def numeric_columns(df, exclude: list[str] | None = None) -> list[str]:
    """Numeric measures only: IDs, coordinates, postcodes, phone numbers and years are not outlier candidates."""
    exclude = set(exclude or [])
    return [c for c in df.columns if is_numeric(df[c].dtype) and not is_id_like(c) and not _NOT_MEASURES.search(str(c).lower())
            and c not in exclude and c != FLAG]


def _log_columns(df, cols: list[str]) -> set[str]:
    """Strongly skewed columns (prices, counts, durations) are judged on a signed-log scale: sign(x)*log1p(|x|).
    This keeps legitimately large values (airport fares, big hosts) while still catching absurd ones, and
    tolerates a few negatives such as refunds."""
    if not cols:
        return set()
    lazy = []
    for c in cols:
        x = df[c].astype("float64")
        lazy += [x.mean(), (x ** 2).mean(), (x ** 3).mean()]
    vals = compute(*lazy)
    out = set()
    for i, c in enumerate(cols):
        m1, m2, m3 = vals[3 * i: 3 * i + 3]
        var = m2 - m1 ** 2
        if var and var > 0 and not np.isnan(var):
            if abs((m3 - 3 * m1 * m2 + 2 * m1 ** 3) / var ** 1.5) > 2:
                out.add(c)
    return out


def _slog(x):
    return np.sign(x) * np.log1p(np.abs(x))


def _unslog(v: float) -> float:
    return float(np.sign(v) * np.expm1(abs(v)))


def _bounds(df, cols: list[str], method: str, k: float) -> dict[str, tuple[float, float]]:
    if not cols:
        return {}
    if method == "zscore":
        means = [df[c].mean() for c in cols]
        stds = [df[c].std() for c in cols]
        vals = compute(*means, *stds)
        m, s = vals[: len(cols)], vals[len(cols):]
        return {c: (m[i] - k * s[i], m[i] + k * s[i]) for i, c in enumerate(cols) if s[i] and not np.isnan(s[i])}
    if method == "mad":
        meds = compute(*[df[c].quantile(0.5) for c in cols])
        mads = compute(*[(df[c] - meds[i]).abs().quantile(0.5) for i, c in enumerate(cols)])
        out = {}
        for i, c in enumerate(cols):
            if mads[i] and not np.isnan(mads[i]):
                spread = k * mads[i] / 0.6745
                out[c] = (meds[i] - spread, meds[i] + spread)
        return out
    # IQR (default). Dask quantiles are approximate (t-digest), which is fine for bounds.
    q1s = compute(*[df[c].quantile(0.25) for c in cols])
    q3s = compute(*[df[c].quantile(0.75) for c in cols])
    out = {}
    for i, c in enumerate(cols):
        iqr = q3s[i] - q1s[i]
        if iqr and not np.isnan(iqr):
            out[c] = (q1s[i] - k * iqr, q3s[i] + k * iqr)
    return out


def detect_and_treat(df, cfg: dict) -> tuple[Any, dict]:
    method = cfg.get("method", "iqr")
    action = cfg.get("action", "cap")
    default_k = {"iqr": 3.0, "zscore": 3.0, "mad": 3.5}.get(method, 3.0)
    k = float(cfg.get("threshold") or default_k)
    cols = numeric_columns(df, cfg.get("exclude_columns"))
    # Integer columns with only a handful of values are codes (payment_type 1-6, vendor 1/2), not measurements.
    if cols:
        lazy = []
        for c in cols:
            lazy += [df[c].nunique_approx() if is_dask(df) else df[c].nunique(), ((df[c].dropna() % 1) == 0).all(), df[c].count()]
        vals = compute(*lazy)
        codes = [c for i, c in enumerate(cols) if is_code(int(vals[3 * i]), bool(vals[3 * i + 1]), int(vals[3 * i + 2]))]
        cols = [c for c in cols if c not in codes]
    else:
        codes = []
    log_cols = _log_columns(df, cols) if cfg.get("log_skewed", True) else set()
    if log_cols:
        logged = df.assign(**{c: _slog(df[c].astype("float64")) for c in log_cols})
        bounds = _bounds(logged, cols, method, k)
        for c in log_cols:
            if c in bounds:
                lo, hi = bounds[c]
                bounds[c] = (_unslog(lo), _unslog(hi))
    else:
        bounds = _bounds(df, cols, method, k)
    skipped = [c for c in cols if c not in bounds]

    # Whole-number columns get whole-number bounds so capped values stay valid integers.
    if bounds:
        int_flags = compute(*[((df[c].dropna() % 1) == 0).all() for c in bounds])
        for c, is_int in zip(list(bounds), int_flags):
            if bool(is_int):
                lo, hi = bounds[c]
                bounds[c] = (float(np.ceil(lo)), float(np.floor(hi)))
    masks = {c: (df[c] < lo) | (df[c] > hi) for c, (lo, hi) in bounds.items()}
    counts = dict(zip(masks.keys(), compute(*[m.sum() for m in masks.values()]))) if masks else {}

    issues, per_col = [], {}
    for c, (lo, hi) in bounds.items():
        n = int(counts.get(c, 0))
        per_col[c] = {"lower": to_py(lo), "upper": to_py(hi), "outliers": n, "log_scale": c in log_cols}
        if n:
            scale = ", log scale" if c in log_cols else ""
            issues.append(dict(step="outliers", column=c, issue=f"values outside [{lo:,.2f}, {hi:,.2f}] ({method}, k={k:g}{scale})", count=n, action=action))

    active = {c: m for c, m in masks.items() if counts.get(c, 0)}
    if active:
        if action == "cap":
            df = df.assign(**{c: df[c].clip(lower=bounds[c][0], upper=bounds[c][1]) for c in active})
        elif action == "null":
            df = df.assign(**{c: df[c].mask(active[c]) for c in active})
        elif action in ("remove", "flag"):
            any_mask = None
            for m in active.values():
                m = m.fillna(False)
                any_mask = m if any_mask is None else (any_mask | m)
            if action == "remove":
                df = df[~any_mask]
            else:
                df = df.assign(**{FLAG: any_mask.astype(bool)})
        df = persist(df)

    rows_removed = 0
    if action == "remove" and active:
        rows_removed = sum(int(counts[c]) for c in active)  # upper bound; exact count recomputed by pipeline
    return df, {
        "method": method, "threshold": k, "action": action, "columns": per_col,
        "skipped_constant": skipped, "skipped_codes": codes, "issues": issues,
        "total_outliers": int(sum(int(v) for v in counts.values())),
        "rows_removed_upper_bound": rows_removed,
    }


__all__ = ["detect_and_treat", "numeric_columns", "FLAG", "is_dask", "pd"]
