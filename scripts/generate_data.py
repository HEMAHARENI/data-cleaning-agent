"""Generate a messy demo CSV.

    python scripts/generate_data.py --rows 5000                     # small, Pandas path
    python scripts/generate_data.py --rows 1200000 --out big.csv    # ~115 MB, triggers Dask
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from app.engine.sample import write_sample  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--rows", type=int, default=5000)
p.add_argument("--out", default="data/messy_customers.csv")
p.add_argument("--seed", type=int, default=42)
a = p.parse_args()
path = write_sample(Path(a.out), rows=a.rows, seed=a.seed)
print(f"wrote {path} ({path.stat().st_size / 1024 / 1024:.1f} MB)")
