"""Background job runner with progress tracking (in-memory registry + on-disk outputs)."""
from __future__ import annotations

import logging
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .config import settings
from .engine.loader import choose_engine
from .engine.pipeline import run_pipeline

log = logging.getLogger(__name__)


@dataclass
class Job:
    id: str
    dataset_id: str
    filename: str
    config: dict
    status: str = "queued"  # queued | running | completed | failed
    progress: int = 0
    step: str = "queued"
    engine: str | None = None
    logs: list[dict] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    error: str | None = None
    summary: dict | None = None
    score_before: dict | None = None
    score_after: dict | None = None

    def public(self) -> dict[str, Any]:
        d = asdict(self)
        d.pop("config", None)
        return d


class JobManager:
    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._reports: dict[str, dict] = {}
        self._lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=settings.max_parallel_jobs, thread_name_prefix="dq-job")

    def out_dir(self, job_id: str) -> Path:
        return settings.data_dir / "jobs" / job_id

    def submit(self, dataset_id: str, src: Path, filename: str, config: dict) -> Job:
        job = Job(id=uuid.uuid4().hex[:12], dataset_id=dataset_id, filename=filename, config=config)
        with self._lock:
            self._jobs[job.id] = job
        self._pool.submit(self._run, job, src)
        return job

    def _progress(self, job: Job):
        def cb(pct: int, step: str, message: str) -> None:
            job.progress, job.step = pct, step
            job.logs.append({"t": round(time.time() - (job.started_at or time.time()), 2), "step": step, "message": message})
        return cb

    def _run(self, job: Job, src: Path) -> None:
        job.status, job.started_at = "running", time.time()
        try:
            job.engine, _ = choose_engine(src.stat().st_size, job.config.get("engine", "auto"))
            report = run_pipeline(src, self.out_dir(job.id), job.config, self._progress(job))
            job.engine = report["engine"]
            job.summary = report["summary"]
            job.score_before, job.score_after = report["score_before"], report["score_after"]
            with self._lock:
                self._reports[job.id] = report
            job.status = "completed"
        except Exception as exc:  # surface every failure to the UI
            log.exception("Job %s failed", job.id)
            job.status, job.error = "failed", f"{type(exc).__name__}: {exc}"
            job.logs.append({"t": round(time.time() - job.started_at, 2), "step": "error", "message": job.error})
            job.logs.append({"t": 0, "step": "trace", "message": traceback.format_exc(limit=3)})
        finally:
            job.finished_at = time.time()

    def get(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    def report(self, job_id: str) -> dict | None:
        return self._reports.get(job_id)

    def list(self) -> list[dict]:
        return [j.public() for j in sorted(self._jobs.values(), key=lambda j: j.created_at, reverse=True)]

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)


jobs = JobManager()
