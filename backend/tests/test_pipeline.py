"""Engine tests. Every behavioural test runs on both Pandas and Dask."""
from __future__ import annotations

import os

os.environ.setdefault("DQ_DASK_PROCESSES", "false")  # in-process workers keep the test suite fast

import numpy as np
import pandas as pd
import pytest

from app.engine.loader import choose_engine
from app.engine.normalize import canon_key, parse_bool, parse_numeric, snake_case, standardize_columns
from app.engine.pipeline import run_pipeline
from app.engine.sample import write_sample

ENGINES = ["pandas", "dask"]


@pytest.fixture(scope="session")
def messy_csv(tmp_path_factory):
    path = tmp_path_factory.mktemp("data") / "messy.csv"
    df = pd.DataFrame({
        "Customer ID": ["1", "2", "3", "3", "1", "5", "6", "7", "8", "9", "10", "11"],
        " Full Name ": ["Ann Lee", "  Bob  Ray ", "Cy Dee", "Cy Dee", "ANN LEE", "Eve Fox", "Gus Hal", "Ivy Jo", "Kim Lo", "Max Ng", "Ola Pe", "Quin Ro"],
        "City": ["Chennai", "chennai", "CHENNAI", "CHENNAI", "Chennai", "London", "london", "N/A", "London", "London", "", "Chennai"],
        "Age": ["30", "41", "35", "35", "30", "n/a", "29", "38", "33", "400", "31", "36"],
        "Income": ["$50,000", "62000", "55,000", "55,000", "$50,000", "58000", "", "61000", "57000", "59000", "60000", "56000"],
        "Joined": ["2024-01-05", "01/02/2023", "Mar 3 2022", "Mar 3 2022", "2024-01-05", "2023-06-01", "bad", "2022-02-02", "2021-12-12", "2020-01-01", "2023-03-03", "2022-07-07"],
        "Active": ["Yes", "no", "TRUE", "TRUE", "Yes", "N", "y", "false", "Yes", "No", "yes", "no"],
        "Mostly Empty": ["", "", "", "", "", "", "", "", "", "x", "", ""],
    })
    df.to_csv(path, index=False)
    return path


# ---------------------------------------------------------------- unit tests
def test_snake_case_and_dedupe():
    assert snake_case(" Full Name ") == "full_name"
    assert snake_case("SignupDate") == "signup_date"
    assert snake_case("2nd col") == "c_2nd_col"
    assert standardize_columns(["A b", "a_b"]) == {"A b": "a_b", "a_b": "a_b_2"}


def test_parsers():
    s = pd.Series(["$1,200", "(300)", "12%", "abc", None], dtype=object)
    out = parse_numeric(s).tolist()
    assert out[:3] == [1200.0, -300.0, 12.0] and np.isnan(out[3]) and np.isnan(out[4])
    b = parse_bool(pd.Series(["Yes", "n", "TRUE", "maybe"], dtype=object))
    assert b.tolist()[:3] == [True, False, True] and pd.isna(b.iloc[3])
    assert canon_key("New-York ") == canon_key("new york")


def test_engine_switch_threshold():
    assert choose_engine(50 * 1024 * 1024, "auto", 100)[0] == "pandas"
    assert choose_engine(150 * 1024 * 1024, "auto", 100)[0] == "dask"
    assert choose_engine(10, "dask", 100)[0] == "dask"


# ---------------------------------------------------------- end-to-end tests
@pytest.mark.parametrize("engine", ENGINES)
def test_full_pipeline(messy_csv, tmp_path, engine):
    rules = [{"column": "age", "rule": "range", "min": 0, "max": 120}]
    rep = run_pipeline(messy_csv, tmp_path, {"engine": engine, "validation": {"rules": rules}})
    out = pd.read_csv(tmp_path / "cleaned.csv")

    assert rep["engine"] == engine
    assert list(out.columns) == ["customer_id", "full_name", "city", "age", "income", "joined", "active"]
    # exact duplicate (row 3 twice) removed; near-duplicate "ANN LEE" (same id, different casing) removed
    assert rep["steps"]["duplicates"]["removed"] == 2
    # age=400 breaks the 0-120 rule on the *observed* value, so it's quarantined rather than silently capped
    assert rep["steps"]["validation"]["rows_rejected"] == 1
    assert len(out) == 9
    # categories unified, null tokens imputed
    assert set(out["city"]) == {"Chennai", "London"}
    assert out.isna().sum().sum() == 0
    # types coerced
    assert pd.api.types.is_numeric_dtype(out["income"]) and pd.api.types.is_numeric_dtype(out["age"])
    assert set(out["active"].astype(str)) <= {"True", "False"}
    assert out["age"].max() <= 120
    # sparse column dropped
    assert "mostly_empty" in rep["steps"]["imputation"]["dropped_columns"]
    assert rep["score_after"]["overall"] > rep["score_before"]["overall"]


@pytest.mark.parametrize("engine", ENGINES)
def test_quarantine(messy_csv, tmp_path, engine):
    cfg = {"engine": engine, "outliers": {"enabled": False},
           "validation": {"rules": [{"column": "age", "rule": "range", "min": 0, "max": 120}], "quarantine": True}}
    rep = run_pipeline(messy_csv, tmp_path, cfg)
    q = pd.read_csv(tmp_path / "quarantine.csv")
    assert rep["steps"]["validation"]["rows_rejected"] == 1
    assert q["age"].tolist() == [400]
    assert "age in [0, 120]" in q["dq_failed_rules"].iloc[0]


def test_pandas_and_dask_agree(tmp_path):
    src = write_sample(tmp_path / "sample.csv", rows=3000, seed=7)
    a = run_pipeline(src, tmp_path / "p", {"engine": "pandas"})
    b = run_pipeline(src, tmp_path / "d", {"engine": "dask"})
    assert a["summary"] == b["summary"]
    pa = pd.read_csv(tmp_path / "p" / "cleaned.csv").sort_values("customer_id").reset_index(drop=True)
    pb = pd.read_csv(tmp_path / "d" / "cleaned.csv").sort_values("customer_id").reset_index(drop=True)
    pd.testing.assert_frame_equal(pa, pb)


@pytest.mark.parametrize("method,action", [("zscore", "remove"), ("mad", "flag"), ("iqr", "null")])
def test_outlier_modes(messy_csv, tmp_path, method, action):
    rep = run_pipeline(messy_csv, tmp_path, {"engine": "pandas", "outliers": {"method": method, "action": action, "threshold": 2.5}})
    out = pd.read_csv(tmp_path / "cleaned.csv")
    assert rep["steps"]["outliers"]["columns"]["age"]["outliers"] >= 1
    if action == "remove":
        assert 400 not in out["age"].tolist()
    if action == "flag":
        assert out["dq_outlier"].sum() >= 1
    if action == "null":
        assert out["age"].max() < 400 and out["age"].isna().sum() == 0


def test_near_duplicates_respect_ids(tmp_path):
    """Distinct transactions that share every non-ID value must not be merged (regression: dirty_cafe_sales)."""
    src = tmp_path / "tx.csv"
    pd.DataFrame({"Transaction ID": [f"TXN_{i}" for i in range(6)], "Item": ["Coffee"] * 6, "Quantity": ["2"] * 6,
                  "Payment": ["Cash", "cash", "ERROR", "UNKNOWN", "Cash", "Card"]}).to_csv(src, index=False)
    rep = run_pipeline(src, tmp_path / "o", {"engine": "pandas"})
    assert rep["summary"]["rows_out"] == 6
    out = pd.read_csv(tmp_path / "o" / "cleaned.csv")
    assert not set(out["payment"]) & {"ERROR", "UNKNOWN", "cash"}
    rep2 = run_pipeline(src, tmp_path / "o2", {"engine": "pandas", "duplicates": {"ignore_id_columns": True}})
    assert rep2["summary"]["rows_out"] < 6


def test_skewed_and_geo_columns(tmp_path):
    rng = np.random.default_rng(0)
    n = 2000
    src = tmp_path / "geo.csv"
    pd.DataFrame({"latitude": rng.normal(40.7, 0.05, n).tolist()[:-1] + [40.5], "nights": rng.geometric(0.3, n),
                  "host_name": [f"host{i}" for i in range(n - 5)] + [""] * 5}).to_csv(src, index=False)
    rep = run_pipeline(src, tmp_path / "o", {"engine": "pandas"})
    cols = rep["steps"]["outliers"]["columns"]
    assert "latitude" not in cols
    assert cols["nights"]["log_scale"] and cols["nights"]["outliers"] < n * 0.01
    assert rep["steps"]["imputation"]["columns"]["host_name"]["fill_value"] == "Unknown"


@pytest.mark.parametrize("engine", ENGINES)
def test_taxi_style_regressions(tmp_path, engine):
    """Regressions found on NYC yellow-taxi data: timestamps keep trips distinct, digit codes aren't booleans,
    codes are imputed with the mode, and the cross-column rule quarantines time-travelling trips."""
    rng = np.random.default_rng(1)
    n = 3000
    pickup = pd.Timestamp("2021-01-01") + pd.to_timedelta(rng.integers(0, 30 * 86400, n), unit="s")
    dropoff = pickup + pd.to_timedelta(rng.integers(60, 3600, n), unit="s")
    dropoff = dropoff.to_series().reset_index(drop=True)
    dropoff.iloc[:5] = pickup[:5] - pd.Timedelta(minutes=10)
    df = pd.DataFrame({
        "VendorID": rng.choice(["1", "2", "2"], n), "pickup": pickup.astype(str), "dropoff": dropoff.astype(str),
        "RatecodeID": rng.choice(["1"] * 30 + ["2", "5"], n), "fare": ["8.0"] * n,
    })
    df.loc[10:40, "VendorID"] = ""
    src = tmp_path / "taxi.csv"
    df.to_csv(src, index=False)
    rules = [{"column": "pickup", "rule": "less_or_equal", "other": "dropoff"}]
    rep = run_pipeline(src, tmp_path / "o", {"engine": engine, "validation": {"rules": rules}})
    assert rep["steps"]["normalize"]["column_types"]["ratecode_id"] == "numeric"
    assert rep["steps"]["duplicates"]["removed"] == 0
    assert rep["steps"]["imputation"]["columns"]["vendor_id"]["fill_value"] == 2
    assert rep["steps"]["validation"]["rows_rejected"] == 5


@pytest.mark.parametrize("engine", ENGINES)
def test_taxi_style_regressions(tmp_path, engine):
    """Regressions found on NYC yellow-taxi data: timestamps keep trips distinct, digit codes aren't booleans,
    codes are imputed with the mode, and the cross-column rule quarantines time-travelling trips."""
    rng = np.random.default_rng(1)
    n = 3000
    pickup = pd.Series(pd.Timestamp("2021-01-01") + pd.to_timedelta(rng.integers(0, 30 * 86400, n), unit="s"))
    dropoff = pickup + pd.to_timedelta(rng.integers(60, 3600, n), unit="s")
    dropoff.iloc[:5] = pickup.iloc[:5] - pd.Timedelta(minutes=10)
    df = pd.DataFrame({
        "VendorID": rng.choice(["1", "2", "2"], n), "pickup": pickup.astype(str), "dropoff": dropoff.astype(str),
        "RatecodeID": rng.choice(["1"] * 30 + ["2", "5"], n), "fare": ["8.0"] * n,
    })
    df.loc[10:40, "VendorID"] = ""
    src = tmp_path / "taxi.csv"
    df.to_csv(src, index=False)
    rules = [{"column": "pickup", "rule": "less_or_equal", "other": "dropoff"}]
    rep = run_pipeline(src, tmp_path / "o", {"engine": engine, "validation": {"rules": rules}})
    assert rep["steps"]["normalize"]["column_types"]["ratecode_id"] == "numeric"
    assert rep["steps"]["duplicates"]["removed"] == 0
    assert rep["steps"]["imputation"]["columns"]["vendor_id"]["fill_value"] == 2
    assert rep["steps"]["validation"]["rows_rejected"] == 5


def test_rules_see_observed_values_not_capped_ones(tmp_path):
    """Regression (NYC taxi): outlier capping must not turn a negative total into a positive one that passes 'total >= 0'."""
    src = tmp_path / "t.csv"
    totals = [str(v) for v in np.random.default_rng(3).lognormal(3, 0.4, 500).round(2)] + ["-52.8", "-11.3"]
    pd.DataFrame({"total": totals, "tolls": ["0"] * 480 + ["6.12"] * 22}).to_csv(src, index=False)
    rep = run_pipeline(src, tmp_path / "o", {"engine": "pandas", "validation": {"rules": [{"column": "total", "rule": "range", "min": 0}]}})
    assert rep["steps"]["validation"]["rows_rejected"] == 2
    assert rep["steps"]["normalize"]["column_types"]["tolls"] == "numeric"
