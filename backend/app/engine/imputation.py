"""Missing-value handling.

1. Columns whose missing ratio exceeds `drop_threshold` are dropped.
2. Every other column is imputed. With strategy "auto":
     numeric   -> median if |skew| > 1 else mean (rounded for integer columns)
     datetime  -> median timestamp
     boolean / category / text -> mode
   Per-column overrides: mean | median | mode | constant:<value> | ffill | drop_rows | knn | none
   KNN runs on Pandas only; on Dask it falls back to median (noted in the report).
"""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .duplicates import is_code, is_id_like
from .frame import compute, is_dask, is_numeric, nrows, persist, to_py


def _skew_lazy(s):
    """Skewness from raw moments so it's computed in the same single pass on Dask."""
    x = s.astype("float64")
    return x.mean(), (x ** 2).mean(), (x ** 3).mean()


def _skew(m1, m2, m3) -> float:
    var = m2 - m1 ** 2
    if not var or var <= 0 or np.isnan(var):
        return 0.0
    return float((m3 - 3 * m1 * m2 + 2 * m1 ** 3) / var ** 1.5)


def _mode_lazy(s):
    # Top candidates only (cheap on Dask); ties are broken deterministically in _pick_mode.
    return s.dropna().value_counts().nlargest(20)


def _pick_mode(vc):
    if not len(vc):
        return None
    top = vc[vc == vc.max()]
    return sorted(top.index, key=str)[0]


def impute(df, cfg: dict) -> tuple[Any, dict]:
    issues, per_col = [], {}
    overrides: dict[str, str] = cfg.get("column_strategies") or {}
    default = cfg.get("strategy", "auto")
    drop_thr = float(cfg.get("drop_threshold", 0.6))
    n = nrows(df)

    missing = dict(zip(df.columns, compute(*[df[c].isna().sum() for c in df.columns])))
    to_drop = [c for c, m in missing.items() if n and m / n > drop_thr and overrides.get(c) != "none" and c not in (cfg.get("protect_columns") or [])]
    if to_drop:
        df = df.drop(columns=to_drop)
        for c in to_drop:
            issues.append(dict(step="imputation", column=c, issue=f"{missing[c] / n:.0%} missing (> {drop_thr:.0%} threshold)", count=int(missing[c]), action="column dropped"))

    targets = [c for c in df.columns if missing.get(c, 0) > 0]
    num_targets = [c for c in targets if is_numeric(df[c].dtype)]
    code_cols: set[str] = set()
    if num_targets:
        lazy_codes = []
        for c in num_targets:
            lazy_codes += [df[c].nunique_approx() if is_dask(df) else df[c].nunique(), ((df[c].dropna() % 1) == 0).all(), df[c].count()]
        cv = compute(*lazy_codes)
        code_cols = {c for i, c in enumerate(num_targets) if is_id_like(c) or is_code(int(cv[3 * i]), bool(cv[3 * i + 1]), int(cv[3 * i + 2]))}
    plan: dict[str, str] = {}
    for c in targets:
        strat = overrides.get(c, default)
        dt = df[c].dtype
        if strat == "auto":
            if is_numeric(dt) and c in code_cols:
                strat = "mode"  # vendor 1/2, payment_type 1-6: an average is not a valid code
            elif is_numeric(dt):
                strat = "auto_numeric"
            elif pd.api.types.is_datetime64_any_dtype(dt):
                strat = "median"
            else:
                strat = "mode"
        if strat == "knn" and (is_dask(df) or not is_numeric(dt)):
            per_col.setdefault(c, {})["note"] = "KNN needs Pandas + numeric column; fell back to median/mode"
            strat = "auto_numeric" if is_numeric(dt) else "mode"
        if strat in ("mean", "median", "auto_numeric") and not (is_numeric(dt) or pd.api.types.is_datetime64_any_dtype(dt)):
            strat = "mode"
        plan[c] = strat

    # Gather every statistic we need in one pass.
    lazy: dict[tuple[str, str], Any] = {}
    for c, strat in plan.items():
        s = df[c]
        if strat == "auto_numeric":
            m1, m2, m3 = _skew_lazy(s)
            lazy[(c, "m1")], lazy[(c, "m2")], lazy[(c, "m3")] = m1, m2, m3
            lazy[(c, "median")] = s.quantile(0.5)
            lazy[(c, "is_int")] = ((s.dropna() % 1) == 0).all()
        elif strat == "mean":
            lazy[(c, "mean")] = s.mean()
            lazy[(c, "is_int")] = ((s.dropna() % 1) == 0).all()
        elif strat == "median":
            if pd.api.types.is_datetime64_any_dtype(s.dtype):
                lazy[(c, "median")] = s.dropna().astype("int64").quantile(0.5)
            else:
                lazy[(c, "median")] = s.quantile(0.5)
        elif strat == "mode":
            lazy[(c, "mode")] = _mode_lazy(s)
    keys = list(lazy)
    stats = dict(zip(keys, compute(*[lazy[k] for k in keys])))

    fills: dict[str, Any] = {}
    knn_cols: list[str] = []
    ffill_cols: list[str] = []
    drop_row_cols: list[str] = []
    for c, strat in plan.items():
        info: dict[str, Any] = per_col.setdefault(c, {})
        value: Any = None
        label = strat
        if strat == "auto_numeric":
            sk = _skew(stats[(c, "m1")], stats[(c, "m2")], stats[(c, "m3")])
            if abs(sk) > 1:
                value, label = stats[(c, "median")], f"median (skew {sk:.2f})"
            else:
                value, label = stats[(c, "m1")], f"mean (skew {sk:.2f})"
            if bool(stats[(c, "is_int")]) and value is not None and not np.isnan(value):
                value = float(round(value))
        elif strat == "mean":
            value = stats[(c, "mean")]
            if bool(stats[(c, "is_int")]):
                value = float(round(value))
        elif strat == "median":
            value = stats[(c, "median")]
            if pd.api.types.is_datetime64_any_dtype(df[c].dtype) and value is not None and not pd.isna(value):
                value = pd.Timestamp(int(value))
        elif strat == "mode":
            mode = stats[(c, "mode")]
            value = _pick_mode(mode)
            non_null = n - int(missing[c])
            top_share = (int(mode.max()) / non_null) if len(mode) and non_null else 0
            if value is None:
                value, label = "Unknown", "constant 'Unknown' (no values)"
            elif not (is_numeric(df[c].dtype) or pd.api.types.is_bool_dtype(df[c].dtype) or str(df[c].dtype) == "boolean") and top_share < 0.01:
                # Free text such as names: the most common value is meaningless as a fill, so mark it explicitly.
                value, label = "Unknown", "constant 'Unknown' (free text, no dominant value)"
            else:
                label = "mode"
        elif strat.startswith("constant:"):
            raw = strat.split(":", 1)[1]
            value = raw
            if is_numeric(df[c].dtype):
                try:
                    value = float(raw)
                except ValueError:
                    pass
            label = f"constant '{raw}'"
        elif strat == "knn":
            knn_cols.append(c)
            label = "KNN (k=5)"
        elif strat == "ffill":
            ffill_cols.append(c)
            label = "forward fill"
        elif strat == "drop_rows":
            drop_row_cols.append(c)
            label = "rows dropped"
        elif strat == "none":
            label = "left missing"
        if value is not None:
            fills[c] = value
        info.update(strategy=label, fill_value=to_py(value) if value is not None else None, filled=int(missing[c]))
        issues.append(dict(step="imputation", column=c, issue="missing values", count=int(missing[c]), action=f"imputed with {label}" + (f" = {to_py(value)}" if value is not None else "")))

    if fills:
        df = df.fillna(fills)
    if ffill_cols:
        df = df.assign(**{c: df[c].ffill().bfill() for c in ffill_cols})
    if drop_row_cols:
        df = df.dropna(subset=drop_row_cols)
    if knn_cols:
        from sklearn.impute import KNNImputer

        num_cols = [c for c in df.columns if is_numeric(df[c].dtype)]
        imputed = KNNImputer(n_neighbors=5).fit_transform(df[num_cols])
        knn_df = pd.DataFrame(imputed, columns=num_cols, index=df.index)
        for c in knn_cols:
            df[c] = knn_df[c]
    df = persist(df)
    return df, {"dropped_columns": to_drop, "columns": per_col, "issues": issues,
                "cells_imputed": int(sum(v.get("filled", 0) for v in per_col.values() if v.get("strategy") not in ("left missing",)))}
