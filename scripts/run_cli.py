"""Run the pipeline without the web UI.

    python scripts/run_cli.py data/messy_customers.csv --out data/cli_run
    python scripts/run_cli.py data/big.csv --engine dask --rules rules.json
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from app.engine.pipeline import run_pipeline  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("src")
    p.add_argument("--out", default="data/cli_run")
    p.add_argument("--engine", default="auto", choices=["auto", "pandas", "dask"])
    p.add_argument("--rules", help="JSON file with a list of validation rules")
    a = p.parse_args()
    rules = json.loads(Path(a.rules).read_text()) if a.rules else []
    rep = run_pipeline(Path(a.src), Path(a.out), {"engine": a.engine, "validation": {"rules": rules}},
                       progress=lambda pct, step, msg: print(f"[{pct:3d}%] {step:<10} {msg}"))
    print(json.dumps({"engine": rep["engine"], "before": rep["score_before"], "after": rep["score_after"], **rep["summary"]}, indent=2))
    if rep["engine"] == "dask":
        from app.engine.cluster import shutdown
        shutdown()


if __name__ == "__main__":  # guard required: Dask worker processes re-import this module
    main()
