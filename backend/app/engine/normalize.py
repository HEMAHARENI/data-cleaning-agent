"""Normalization: column names, whitespace, null tokens, type coercion and
canonicalisation of inconsistent categorical spellings.

Runs first so that duplicate detection, outlier detection and imputation all
see consistent, correctly-typed values.
"""
from __future__ import annotations

import re
from typing import Any

import numpy as np
import pandas as pd

from .frame import NULL_TOKENS, apply_series, compute, head, is_dask, is_text

BOOL_TRUE = {"true", "yes", "y", "t", "1"}
BOOL_FALSE = {"false", "no", "n", "f", "0"}
_EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


# ---------------------------------------------------------------- column names
def snake_case(name: Any) -> str:
    s = str(name).strip()
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s).strip("_").lower()
    if not s:
        s = "column"
    if s[0].isdigit():
        s = f"c_{s}"
    return s


def standardize_columns(columns) -> dict[str, str]:
    mapping, seen = {}, {}
    for c in columns:
        base = snake_case(c)
        n = seen.get(base, 0)
        seen[base] = n + 1
        mapping[c] = base if n == 0 else f"{base}_{n + 1}"
    return mapping


# ---------------------------------------------------- partition-level functions
def clean_text(s: pd.Series) -> pd.Series:
    """Trim, collapse internal whitespace and turn null-like tokens into NaN."""
    out = s.astype(object).copy()
    mask = out.notna()
    if mask.any():
        out[mask] = out[mask].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
        low = out.astype(str).str.lower()
        out[mask & low.isin(NULL_TOKENS)] = np.nan
    return out


def parse_numeric(s: pd.Series) -> pd.Series:
    txt = s.astype(str).str.strip()
    neg = txt.str.match(r"^\(.*\)$")
    txt = (
        txt.str.replace(r"[\$€£₹¥,\s]", "", regex=True)
        .str.replace(r"^\((.*)\)$", r"\1", regex=True)
        .str.rstrip("%")
    )
    num = pd.to_numeric(txt, errors="coerce").astype("float64")
    num = num.where(~neg.fillna(False).astype(bool), -num)
    num[s.isna().to_numpy()] = np.nan
    return num


def parse_datetime(s: pd.Series) -> pd.Series:
    txt = s.astype(object)
    try:
        out = pd.to_datetime(txt, errors="coerce", format="ISO8601")
    except (ValueError, TypeError):
        out = pd.Series(pd.NaT, index=s.index)
    if getattr(out.dt, "tz", None) is not None:
        out = out.dt.tz_localize(None)
    remaining = out.isna() & txt.notna()
    if remaining.any():
        try:
            extra = pd.to_datetime(txt[remaining], errors="coerce", format="mixed")
            if getattr(extra.dt, "tz", None) is not None:
                extra = extra.dt.tz_localize(None)
            out = out.astype("datetime64[ns]")
            out[remaining] = extra.astype("datetime64[ns]")
        except (ValueError, TypeError):
            pass
    return out.astype("datetime64[ns]")


def parse_bool(s: pd.Series) -> pd.Series:
    low = s.astype(str).str.strip().str.lower()
    out = pd.Series(pd.NA, index=s.index, dtype="boolean")
    out[low.isin(BOOL_TRUE).to_numpy()] = True
    out[low.isin(BOOL_FALSE).to_numpy()] = False
    return out


def lower_text(s: pd.Series) -> pd.Series:
    out = s.astype(object).copy()
    m = out.notna()
    out[m] = out[m].astype(str).str.lower()
    return out


def canon_key(v: str) -> str:
    return re.sub(r"[^0-9a-z]", "", str(v).casefold())


# ------------------------------------------------------------- type inference
def infer_type(sample: pd.Series) -> str:
    vals = clean_text(sample).dropna()
    if len(vals) == 0:
        return "empty"
    low = vals.astype(str).str.lower()
    words = BOOL_TRUE | BOOL_FALSE
    has_word = low.isin(words - {"0", "1"}).any()  # "6.12" or "2" are numbers, not booleans
    if has_word and low.isin(words).mean() >= 0.95:
        return "boolean"
    if parse_numeric(vals).notna().mean() >= 0.9:
        return "numeric"
    if low.str.fullmatch(r"\d+").mean() < 0.5 and low.str.contains(r"\d").mean() >= 0.9:
        if parse_datetime(vals).notna().mean() >= 0.9:
            return "datetime"
    if low.str.match(_EMAIL_RE).mean() >= 0.9:
        return "email"
    nunique = vals.nunique()
    if nunique <= 100 and (nunique / len(vals) <= 0.2 or nunique <= 20):
        return "category"
    return "text"


# ------------------------------------------------------------------- the step
def normalize(df, cfg: dict) -> tuple[Any, dict]:
    issues: list[dict] = []
    report: dict[str, Any] = {"renamed": {}, "column_types": {}, "canonical_maps": {}}

    if cfg.get("standardize_columns", True):
        mapping = standardize_columns(df.columns)
        renamed = {k: v for k, v in mapping.items() if k != v}
        if renamed:
            df = df.rename(columns=mapping)
            report["renamed"] = {str(k): v for k, v in renamed.items()}
            issues.append(dict(step="normalize", column="*", issue="non-standard column names", count=len(renamed), action="renamed to snake_case"))

    sample = head(df, 5000)
    text_cols = [c for c in df.columns if is_text(df[c].dtype)]
    for c in df.columns:
        if c not in text_cols:
            dt = df[c].dtype
            report["column_types"][c] = (
                "numeric" if pd.api.types.is_numeric_dtype(dt) and not pd.api.types.is_bool_dtype(dt)
                else "datetime" if pd.api.types.is_datetime64_any_dtype(dt)
                else "boolean" if pd.api.types.is_bool_dtype(dt) else str(dt)
            )

    coerce = cfg.get("coerce_types", True)
    canon = cfg.get("canonicalize_categories", True)
    new_cols: dict[str, Any] = {}
    lazy_counts: dict[tuple[str, str], Any] = {}
    kinds: dict[str, str] = {}

    for c in text_cols:
        orig = df[c]
        cleaned = apply_series(orig, clean_text, object)
        o_str = orig.astype(str)
        lazy_counts[(c, "whitespace")] = ((~orig.isna()) & (~cleaned.isna()) & (o_str != cleaned.astype(str))).sum()
        lazy_counts[(c, "null_tokens")] = ((~orig.isna()) & cleaned.isna()).sum()
        kind = infer_type(sample[c]) if coerce else "text"
        kinds[c] = kind
        report["column_types"][c] = kind
        final = cleaned
        if kind == "numeric":
            final = apply_series(cleaned, parse_numeric, "float64")
        elif kind == "datetime":
            final = apply_series(cleaned, parse_datetime, "datetime64[ns]")
        elif kind == "boolean":
            final = apply_series(cleaned, parse_bool, "boolean")
        elif kind == "email":
            final = apply_series(cleaned, lower_text, object)
            lazy_counts[(c, "case")] = ((~cleaned.isna()) & (cleaned.astype(str) != final.astype(str))).sum()
        if kind in ("numeric", "datetime", "boolean"):
            lazy_counts[(c, "invalid")] = ((~cleaned.isna()) & final.isna()).sum()
        if kind in ("category", "text") and canon:
            lazy_counts[(c, "value_counts")] = cleaned.value_counts() if kind == "category" else None
        new_cols[c] = final

    keys = [k for k, v in lazy_counts.items() if v is not None]
    values = compute(*[lazy_counts[k] for k in keys])
    results = dict(zip(keys, values))

    for c in text_cols:
        ws = int(results.get((c, "whitespace"), 0))
        nt = int(results.get((c, "null_tokens"), 0))
        if ws:
            issues.append(dict(step="normalize", column=c, issue="leading/trailing or repeated whitespace", count=ws, action="trimmed"))
        if nt:
            issues.append(dict(step="normalize", column=c, issue="null-like tokens (N/A, '', null, ...)", count=nt, action="converted to missing"))
        inv = int(results.get((c, "invalid"), 0))
        if inv:
            issues.append(dict(step="normalize", column=c, issue=f"values not parseable as {kinds[c]}", count=inv, action="set to missing (imputed later)"))
        if kinds[c] in ("numeric", "datetime", "boolean"):
            issues.append(dict(step="normalize", column=c, issue=f"stored as text, inferred {kinds[c]}", count=1, action=f"coerced to {kinds[c]}"))
        cs = int(results.get((c, "case"), 0))
        if cs:
            issues.append(dict(step="normalize", column=c, issue="inconsistent email casing", count=cs, action="lower-cased"))

        vc = results.get((c, "value_counts"))
        if vc is not None and len(vc):
            groups: dict[str, list[tuple[str, int]]] = {}
            for val, cnt in vc.items():
                groups.setdefault(canon_key(val), []).append((val, int(cnt)))
            mapping, changed = {}, 0
            for variants in groups.values():
                if len(variants) > 1:
                    # Most frequent spelling wins; ties prefer properly-cased forms ("New York" over "NEW YORK").
                    canonical = max(variants, key=lambda x: (x[1], x[0] != x[0].upper() and x[0] != x[0].lower(), x[0]))[0]
                    for val, cnt in variants:
                        if val != canonical:
                            mapping[val] = canonical
                            changed += cnt
            if mapping:
                m = dict(mapping)
                new_cols[c] = apply_series(new_cols[c], lambda s, m=m: s.replace(m), object)
                report["canonical_maps"][c] = {str(k): str(v) for k, v in list(mapping.items())[:25]}
                issues.append(dict(step="normalize", column=c, issue=f"{len(mapping)} inconsistent spellings/casings of the same category", count=changed, action="mapped to most frequent form"))

    if new_cols:
        df = df.assign(**new_cols)
    report["issues"] = issues
    report["cells_changed"] = int(sum(i["count"] for i in issues if i["column"] != "*" and not i["issue"].startswith("stored as text")))
    return df, report
