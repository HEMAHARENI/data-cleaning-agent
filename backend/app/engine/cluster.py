"""Lazy Dask cluster management.

The cluster is only started the first time a job needs Dask. If
DASK_SCHEDULER_ADDRESS is set, jobs connect to that external cluster
(see docker-compose.yml); otherwise a LocalCluster is spun up in-process.
"""
from __future__ import annotations

import logging
import threading

from ..config import settings

log = logging.getLogger(__name__)
# P2P shuffle logs a WARNING for every routine shuffle; keep the logs readable.
logging.getLogger("distributed.shuffle._scheduler_plugin").setLevel(logging.ERROR)
_client = None
_lock = threading.Lock()


def get_client():
    global _client
    with _lock:
        if _client is not None and _client.status == "running":
            return _client
        from distributed import Client, LocalCluster

        if settings.dask_scheduler:
            log.info("Connecting to external Dask scheduler at %s", settings.dask_scheduler)
            _client = Client(settings.dask_scheduler, timeout="20s", set_as_default=True)
        else:
            log.info("Starting LocalCluster (%s workers x %s threads)", settings.dask_local_workers, settings.dask_threads_per_worker)
            cluster = LocalCluster(
                n_workers=settings.dask_local_workers,
                threads_per_worker=settings.dask_threads_per_worker,
                processes=settings.dask_local_processes,
                dashboard_address=":8787",
                silence_logs=logging.WARNING,
            )
            _client = Client(cluster, set_as_default=True)
        return _client


def cluster_info() -> dict:
    if _client is None or _client.status != "running":
        return {
            "status": "idle",
            "mode": "external" if settings.dask_scheduler else "local",
            "note": "Cluster starts automatically on the first job above the threshold.",
            "threshold_mb": settings.dask_threshold_mb,
        }
    info = _client.scheduler_info()
    workers = info.get("workers", {})
    return {
        "status": "running",
        "mode": "external" if settings.dask_scheduler else "local",
        "scheduler": info.get("address"),
        "workers": len(workers),
        "threads": sum(w.get("nthreads", 0) for w in workers.values()),
        "memory_gb": round(sum(w.get("memory_limit", 0) for w in workers.values()) / 1e9, 2),
        "dashboard": _client.dashboard_link,
        "threshold_mb": settings.dask_threshold_mb,
    }


def shutdown() -> None:
    global _client
    with _lock:
        if _client is not None:
            try:
                cluster = getattr(_client, "cluster", None)
                _client.close()
                if cluster is not None:
                    cluster.close()
            except Exception:  # pragma: no cover - best effort on shutdown
                pass
            _client = None
