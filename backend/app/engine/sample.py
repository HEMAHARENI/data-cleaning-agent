"""Synthetic messy customer dataset for demos and tests.

Injects: messy headers, whitespace, casing variants, null tokens, mixed date
formats, currency-formatted numbers, text booleans, unparseable values,
outliers, exact duplicates and near-duplicates.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

FIRST = ["John", "Priya", "Wei", "Maria", "Ahmed", "Sara", "Arjun", "Emma", "Lucas", "Aisha", "Kenji", "Olivia", "Ravi", "Chloe", "Diego"]
LAST = ["Smith", "Sharma", "Chen", "Garcia", "Khan", "Lee", "Iyer", "Brown", "Silva", "Patel", "Tanaka", "Wilson", "Nair", "Martin", "Lopez"]
CITIES = ["New York", "Chennai", "Bengaluru", "London", "Singapore", "Sydney", "Berlin", "Toronto"]
PLANS = ["Basic", "Premium", "Enterprise"]


def _variants(values: np.ndarray, rng: np.random.Generator, rate: float) -> np.ndarray:
    out = values.astype(object).copy()
    idx = rng.random(len(out)) < rate
    kinds = rng.integers(0, 4, len(out))
    for i in np.flatnonzero(idx):
        v = out[i]
        out[i] = [v.upper(), v.lower(), f"  {v} ", v.replace(" ", "  ")][kinds[i]]
    return out


def generate(rows: int = 5000, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    n = rows
    first = rng.choice(FIRST, n)
    last = rng.choice(LAST, n)
    names = np.char.add(np.char.add(first.astype(str), " "), last.astype(str))
    emails = np.char.add(np.char.add(np.char.lower(first.astype(str)), "."), np.char.lower(last.astype(str)))
    emails = np.char.add(np.char.add(emails, rng.integers(1, 9999, n).astype(str)), "@example.com")

    dates = pd.Timestamp("2021-01-01") + pd.to_timedelta(rng.integers(0, 1500, n), unit="D")
    fmt = rng.integers(0, 3, n)
    iso = dates.strftime("%Y-%m-%d").to_numpy()
    us = dates.strftime("%m/%d/%Y").to_numpy()
    txt = dates.strftime("%b %d %Y").to_numpy()
    signup = np.where(fmt == 0, iso, np.where(fmt == 1, us, txt)).astype(object)

    age = rng.normal(38, 11, n).clip(18, 80).round().astype(int).astype(object)
    income = rng.lognormal(10.9, 0.45, n).round(-2)
    income_s = np.where(rng.random(n) < 0.5, [f"${v:,.0f}" for v in income], income.astype(int).astype(str)).astype(object)
    active = rng.choice(["Yes", "No", "TRUE", "false", "y", "N"], n).astype(object)
    rating = rng.normal(3.9, 0.7, n).clip(1, 5).round(1).astype(object)

    df = pd.DataFrame({
        "Customer ID": np.arange(100001, 100001 + n).astype(str),
        "Full Name": _variants(names, rng, 0.15),
        "Email Address": np.where(rng.random(n) < 0.1, np.char.upper(emails), emails).astype(object),
        "City": _variants(rng.choice(CITIES, n), rng, 0.2),
        "Plan Type": _variants(rng.choice(PLANS, n, p=[0.5, 0.35, 0.15]), rng, 0.1),
        "SignupDate": signup,
        "Age": age,
        "Annual Income": income_s,
        "Is Active": active,
        "Rating": rating,
        "Legacy Notes": np.where(rng.random(n) < 0.85, "", "migrated from v1").astype(object),
    })

    def poke(col: str, rate: float, values: list):
        m = rng.random(n) < rate
        df.loc[m, col] = rng.choice(values, m.sum())

    poke("Age", 0.06, ["", "N/A", "null", "unknown"])
    poke("Age", 0.01, ["250", "-5", "999"])
    poke("Annual Income", 0.05, ["", "NA", "-"])
    poke("Annual Income", 0.008, ["$9,999,999", "12000000"])
    poke("City", 0.04, ["", "none", "?"])
    poke("Rating", 0.12, ["", "n/a"])
    poke("SignupDate", 0.03, ["", "not a date", "32/13/2022"])
    poke("Is Active", 0.03, ["", "maybe"])

    # Exact duplicates (~3%) and near-duplicates (~2%, same record re-imported with case/space changes).
    dup = df.sample(frac=0.03, random_state=seed)
    near = df.sample(frac=0.02, random_state=seed + 1).copy()
    near["Full Name"] = near["Full Name"].astype(str).str.upper()
    near["Email Address"] = " " + near["Email Address"].astype(str).str.upper() + " "
    out = pd.concat([df, dup, near], ignore_index=True)
    return out.sample(frac=1.0, random_state=seed).reset_index(drop=True)


def write_sample(path: Path, rows: int = 5000, seed: int = 42, chunk: int = 500_000) -> Path:
    """Writes in chunks so multi-GB demo files don't need to fit in memory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    written, part = 0, 0
    while written < rows:
        n = min(chunk, rows - written)
        df = generate(n, seed + part)
        if part:  # keep IDs unique across chunks
            df["Customer ID"] = (df["Customer ID"].astype(int) + part * 10_000_000).astype(str)
        df.to_csv(path, mode="w" if part == 0 else "a", header=part == 0, index=False)
        written += n
        part += 1
    return path
