"""End-to-end data-quality pipeline.

load -> profile(before) -> normalize -> duplicates -> rule validation/quarantine -> outliers
     -> imputation -> finalize types -> built-in checks -> profile(after) -> score -> write

User rules run on the *observed* values, before outlier capping or imputation can alter them:
otherwise a negative fare capped up to $1.34 would silently pass a "fare >= 0" rule.
"""
from __future__ import annotations

import copy
import json
import time
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from . import cluster
from .duplicates import deduplicate, duplicate_rows
from .frame import compute, head, is_dask, nrows, persist, to_py
from .imputation import impute
from .loader import choose_engine, load
from .normalize import normalize
from .outliers import FLAG, detect_and_treat
from .quality import builtin_checks, profile, score, validate

DEFAULT_CONFIG: dict[str, Any] = {
    "engine": "auto",  # auto | pandas | dask
    "normalize": {"standardize_columns": True, "coerce_types": True, "canonicalize_categories": True},
    "duplicates": {"exact": True, "near": True, "key_columns": []},
    "outliers": {"enabled": True, "method": "iqr", "threshold": None, "action": "cap", "exclude_columns": []},
    "imputation": {"enabled": True, "strategy": "auto", "drop_threshold": 0.6, "column_strategies": {}},
    "validation": {"rules": [], "quarantine": True},
    "output": {"format": "csv"},
}

Progress = Callable[[int, str, str], None]


def merge_config(user: dict | None) -> dict:
    cfg = copy.deepcopy(DEFAULT_CONFIG)
    for k, v in (user or {}).items():
        if isinstance(v, dict) and isinstance(cfg.get(k), dict):
            cfg[k].update(v)
        else:
            cfg[k] = v
    return cfg


def _finalize_types(df):
    """Float columns that hold only whole numbers and no gaps become int64."""
    floats = [c for c in df.columns if pd.api.types.is_float_dtype(df[c].dtype)]
    if not floats:
        return df
    checks = compute(*[((~df[c].isna()).all() & ((df[c] % 1) == 0).all()) for c in floats])
    ints = [c for c, ok in zip(floats, checks) if bool(ok)]
    if ints:
        df = df.astype({c: "int64" for c in ints})
    bools = [c for c in df.columns if str(df[c].dtype) == "boolean"]
    if bools:
        has_na = compute(*[df[c].isna().any() for c in bools])
        df = df.astype({c: "bool" for c, na in zip(bools, has_na) if not bool(na)})
    return df


def _write(df, path: Path, fmt: str) -> Path:
    if fmt == "parquet":
        target = path.with_suffix(".parquet")
        if is_dask(df):
            target = path.with_name(path.stem + "_parquet")
            df.to_parquet(str(target), write_index=False)
        else:
            df.to_parquet(target, index=False)
        return target
    target = path.with_suffix(".csv")
    if is_dask(df):
        df.to_csv(str(target), single_file=True, index=False)
    else:
        df.to_csv(target, index=False)
    return target


def run_pipeline(src: Path, out_dir: Path, user_cfg: dict | None = None, progress: Progress | None = None) -> dict:
    cfg = merge_config(user_cfg)
    progress = progress or (lambda p, s, m: None)
    out_dir.mkdir(parents=True, exist_ok=True)
    timings: dict[str, float] = {}
    t_all = time.perf_counter()

    def stage(name: str):
        class _T:
            def __enter__(self_):
                self_.t = time.perf_counter()

            def __exit__(self_, *a):
                timings[name] = round(time.perf_counter() - self_.t, 3)
        return _T()

    size = src.stat().st_size
    engine, reason = choose_engine(size, cfg.get("engine", "auto"))
    progress(3, "load", reason)
    if engine == "dask":
        client = cluster.get_client()
        progress(5, "load", f"Connected to Dask cluster ({len(client.scheduler_info().get('workers', {}))} workers)")

    with stage("load"):
        df = persist(load(src, engine))
        if is_dask(df):
            from distributed import wait

            wait(df)  # persist() is asynchronous; wait so the load timing is real
        n_parts = df.npartitions if is_dask(df) else 1
    progress(10, "load", f"Loaded with {engine} ({n_parts} partition{'s' if n_parts != 1 else ''})")

    with stage("profile_before"):
        before = profile(df, raw=True)
        raw_dups = duplicate_rows(df)
    progress(20, "profile", f"Profiled {before['rows']:,} rows x {before['n_columns']} columns; {before['missing_cells']:,} missing cells, {raw_dups:,} exact duplicate rows")

    issues: list[dict] = []
    steps: dict[str, Any] = {}

    with stage("normalize"):
        df, rep = normalize(df, cfg["normalize"])
        df = persist(df)
    steps["normalize"] = rep
    issues += rep["issues"]
    progress(38, "normalize", f"Normalized: {len(rep['issues'])} issue types fixed, {rep['cells_changed']:,} cells changed")

    with stage("duplicates"):
        df, rep = deduplicate(df, cfg["duplicates"])
    steps["duplicates"] = rep
    issues += rep["issues"]
    progress(52, "duplicates", f"Removed {rep['removed']:,} duplicate rows")

    rules = cfg["validation"].get("rules") or []
    with stage("rules"):
        df, rejected, rule_rep = validate(df, rules, cfg["validation"].get("quarantine", True))
    if rules:
        progress(58, "duplicates", f"Checked {len(rules)} rule(s) on observed values; {rule_rep['rows_rejected']:,} rows quarantined")

    if cfg["outliers"].get("enabled", True):
        with stage("outliers"):
            df, rep = detect_and_treat(df, cfg["outliers"])
        steps["outliers"] = rep
        issues += rep["issues"]
        progress(66, "outliers", f"Found {rep['total_outliers']:,} outlier values ({rep['method']}, action={rep['action']})")

    if cfg["imputation"].get("enabled", True):
        protect = [r.get("column") for r in cfg["validation"].get("rules") or []]
        with stage("imputation"):
            df, rep = impute(df, {**cfg["imputation"], "protect_columns": protect})
        steps["imputation"] = rep
        issues += rep["issues"]
        progress(78, "imputation", f"Imputed {rep['cells_imputed']:,} cells; dropped {len(rep['dropped_columns'])} sparse column(s)")

    with stage("validate"):
        df = persist(_finalize_types(df))
        results = builtin_checks(df) + rule_rep["results"]
        vrep = {"results": results, "rows_valid": nrows(df), "rows_rejected": rule_rep["rows_rejected"],
                "passed": sum(1 for r in results if r["passed"]), "total": len(results),
                "note": "Rules are evaluated on observed values before outlier treatment and imputation."}
    steps["validation"] = vrep
    progress(88, "validate", f"{vrep['passed']}/{vrep['total']} checks passed on observed data; {vrep['rows_rejected']:,} rows quarantined")

    with stage("profile_after"):
        after = profile(df)
    # Score inputs (definitions documented in README).
    norm_invalid = sum(i["count"] for i in steps["normalize"]["issues"]
                       if i["column"] != "*" and "null-like" not in i["issue"] and not i["issue"].startswith("stored as text"))
    outl = steps.get("outliers", {}).get("total_outliers", 0)
    score_before = score(before["rows"], before["n_columns"], before["missing_cells"], raw_dups, norm_invalid + outl)
    remaining_flags = 0
    if FLAG in df.columns:
        remaining_flags = int(compute(df[FLAG].sum())[0])
    rule_fail_left = sum((r.get("failed") or 0) for r in vrep["results"] if r["type"] not in ("builtin",) and not cfg["validation"].get("quarantine", True))
    dups_left = next(r["failed"] for r in vrep["results"] if r["rule"].startswith("no exact duplicate"))
    score_after = score(after["rows"], after["n_columns"], after["missing_cells"], dups_left, remaining_flags + rule_fail_left)
    progress(92, "write", "Writing validated dataset")

    fmt = cfg["output"].get("format", "csv")
    with stage("write"):
        cleaned_path = _write(df, out_dir / "cleaned", fmt)
        rejected_path = None
        if rejected is not None and vrep["rows_rejected"]:
            rejected_path = _write(rejected, out_dir / "quarantine", "csv")
        preview_df = head(df, 50)
    timings["total"] = round(time.perf_counter() - t_all, 3)

    report = {
        "engine": engine,
        "engine_reason": reason,
        "file_size_mb": round(size / 1024 / 1024, 2),
        "partitions": n_parts,
        "config": cfg,
        "score_before": score_before,
        "score_after": score_after,
        "profile_before": before,
        "profile_after": after,
        "steps": steps,
        "issues": issues,
        "summary": {
            "rows_in": before["rows"],
            "rows_out": after["rows"],
            "rows_quarantined": vrep["rows_rejected"],
            "duplicates_removed": steps["duplicates"]["removed"],
            "outliers_treated": steps.get("outliers", {}).get("total_outliers", 0),
            "cells_imputed": steps.get("imputation", {}).get("cells_imputed", 0),
            "cells_normalized": steps["normalize"]["cells_changed"],
            "columns_in": before["n_columns"],
            "columns_out": after["n_columns"],
        },
        "preview": {
            "columns": [str(c) for c in preview_df.columns],
            "rows": [[to_py(v) for v in row] for row in preview_df.astype(object).itertuples(index=False, name=None)],
        },
        "outputs": {"cleaned": cleaned_path.name, "quarantine": rejected_path.name if rejected_path else None, "report": "report.json"},
        "timings_s": timings,
    }
    (out_dir / "report.json").write_text(json.dumps(report, indent=2, default=str))
    progress(100, "done", f"Finished in {timings['total']:.1f}s — quality score {score_before['overall']} -> {score_after['overall']}")
    return report
