"""Runtime settings, all overridable through environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    # Where uploads, job outputs and reports are written.
    data_dir: Path = field(default_factory=lambda: Path(os.getenv("DQ_DATA_DIR", PROJECT_ROOT / "data")))
    # Files larger than this are processed on a Dask cluster instead of Pandas.
    dask_threshold_mb: float = float(os.getenv("DQ_DASK_THRESHOLD_MB", "100"))
    # If set (e.g. tcp://scheduler:8786) jobs use that external cluster; otherwise a LocalCluster is started.
    dask_scheduler: str | None = os.getenv("DASK_SCHEDULER_ADDRESS") or None
    dask_local_workers: int = int(os.getenv("DQ_DASK_WORKERS", "2"))
    dask_threads_per_worker: int = int(os.getenv("DQ_DASK_THREADS", "2"))
    dask_local_processes: bool = _bool("DQ_DASK_PROCESSES", True)
    dask_blocksize: str = os.getenv("DQ_DASK_BLOCKSIZE", "64MB")
    max_upload_mb: int = int(os.getenv("DQ_MAX_UPLOAD_MB", "4096"))
    max_parallel_jobs: int = int(os.getenv("DQ_MAX_PARALLEL_JOBS", "2"))
    frontend_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "frontend")


settings = Settings()
