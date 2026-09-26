"""FastAPI app: REST API + static frontend."""
from __future__ import annotations

import json
import logging
import re
import shutil
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .config import settings
from .engine import cluster
from .engine.frame import to_py
from .engine.loader import SUPPORTED, choose_engine, preview
from .engine.normalize import infer_type, standardize_columns
from .engine.pipeline import DEFAULT_CONFIG
from .engine.sample import write_sample
from .jobs import jobs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
UPLOADS = settings.data_dir / "uploads"


@asynccontextmanager
async def lifespan(_: FastAPI):
    UPLOADS.mkdir(parents=True, exist_ok=True)
    yield
    jobs.shutdown()
    cluster.shutdown()


app = FastAPI(title="DataGuard — automated data quality", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ------------------------------------------------------------------ helpers
def _safe_name(name: str) -> str:
    name = Path(name or "upload.csv").name
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)[:120] or "upload.csv"


COMPRESSED = (".gz", ".bz2", ".xz", ".zip")


def _compression(name: str) -> str:
    low = name.lower()
    return next((ext for ext in COMPRESSED if low.endswith(ext)), "")


def _decompress(src: Path, kind: str, target: Path, limit: int) -> Path:
    """Unpack uploads like data.csv.gz server-side so the engine choice uses the real (uncompressed) size
    and Dask can split the file into partitions (compressed files can't be split)."""
    import bz2
    import gzip
    import lzma
    import zipfile

    if kind == ".zip":
        with zipfile.ZipFile(src) as zf:
            members = [m for m in zf.infolist() if not m.is_dir() and Path(m.filename).suffix.lower() in SUPPORTED]
            if len(members) != 1:
                raise HTTPException(400, "Zip must contain exactly one data file (.csv, .tsv, .parquet, .json, .xlsx)")
            target = target.with_name(_safe_name(members[0].filename))
            opener = lambda: zf.open(members[0])  # noqa: E731
            return _copy_limited(opener, target, limit, src)
    opener = {".gz": lambda: gzip.open(src, "rb"), ".bz2": lambda: bz2.open(src, "rb"), ".xz": lambda: lzma.open(src, "rb")}[kind]
    return _copy_limited(opener, target, limit, src)


def _copy_limited(opener, target: Path, limit: int, src: Path) -> Path:
    written = 0
    with opener() as fin, target.open("wb") as fout:
        while chunk := fin.read(16 * 1024 * 1024):
            written += len(chunk)
            if written > limit:
                raise HTTPException(413, f"Uncompressed file exceeds {settings.max_upload_mb} MB limit")
            fout.write(chunk)
    src.unlink(missing_ok=True)
    return target


def _dataset_dir(dataset_id: str) -> Path:
    if not re.fullmatch(r"[a-f0-9]{12}", dataset_id):
        raise HTTPException(404, "Unknown dataset")
    d = UPLOADS / dataset_id
    if not d.exists():
        raise HTTPException(404, "Unknown dataset")
    return d


def _describe_dataset(dataset_id: str, path: Path) -> dict[str, Any]:
    size = path.stat().st_size
    engine, reason = choose_engine(size)
    pv = preview(path, 200)
    mapping = standardize_columns(pv.columns)
    columns = [
        {"original": str(c), "name": mapping[c], "inferred_type": infer_type(pv[c]) if pv[c].dtype == object else str(pv[c].dtype)}
        for c in pv.columns
    ]
    head = pv.head(15).astype(object)
    meta = {
        "dataset_id": dataset_id,
        "filename": path.name,
        "size_bytes": size,
        "size_mb": round(size / 1024 / 1024, 2),
        "engine": engine,
        "engine_reason": reason,
        "threshold_mb": settings.dask_threshold_mb,
        "columns": columns,
        "preview": {"columns": [str(c) for c in head.columns],
                    "rows": [[to_py(v) for v in r] for r in head.itertuples(index=False, name=None)]},
    }
    (path.parent / "meta.json").write_text(json.dumps(meta, default=str))
    return meta


def _dataset_file(dataset_id: str) -> Path:
    d = _dataset_dir(dataset_id)
    files = [p for p in d.iterdir() if p.suffix.lower() in SUPPORTED and p.name != "meta.json"]
    if not files:
        raise HTTPException(404, "Dataset file missing")
    return files[0]


# ---------------------------------------------------------------------- API
@app.get("/api/health")
def health() -> dict:
    return {"status": "ok", "dask": cluster.cluster_info(), "threshold_mb": settings.dask_threshold_mb}


@app.get("/api/config/defaults")
def defaults() -> dict:
    return {"config": DEFAULT_CONFIG, "threshold_mb": settings.dask_threshold_mb,
            "rule_types": ["not_null", "unique", "range", "regex", "allowed_values", "min_length"],
            "supported_formats": sorted(SUPPORTED)}


@app.post("/api/datasets")
async def upload(file: UploadFile = File(...)) -> dict:
    name = _safe_name(file.filename or "upload.csv")
    compressed = _compression(name)
    inner = name[: -len(compressed)] if compressed else name
    if compressed == ".zip" and Path(inner).suffix.lower() not in SUPPORTED:
        inner = inner + ".csv"  # data.zip -> assume it holds a CSV; checked after extraction
    if Path(inner).suffix.lower() not in SUPPORTED:
        raise HTTPException(400, f"Unsupported file type. Use one of: {', '.join(sorted(SUPPORTED))} (optionally .gz/.bz2/.xz/.zip compressed)")
    dataset_id = uuid.uuid4().hex[:12]
    d = UPLOADS / dataset_id
    d.mkdir(parents=True)
    dest = d / name
    limit = settings.max_upload_mb * 1024 * 1024
    written = 0
    with dest.open("wb") as out:
        while chunk := await file.read(8 * 1024 * 1024):
            written += len(chunk)
            if written > limit:
                out.close()
                shutil.rmtree(d, ignore_errors=True)
                raise HTTPException(413, f"File exceeds {settings.max_upload_mb} MB upload limit")
            out.write(chunk)
    try:
        if compressed:
            dest = _decompress(dest, compressed, d / inner, limit)
        return _describe_dataset(dataset_id, dest)
    except HTTPException:
        shutil.rmtree(d, ignore_errors=True)
        raise
    except Exception as exc:
        shutil.rmtree(d, ignore_errors=True)
        raise HTTPException(400, f"Could not read file: {exc}") from exc


@app.post("/api/datasets/sample")
def sample(rows: int = Query(5000, ge=100, le=5_000_000)) -> dict:
    dataset_id = uuid.uuid4().hex[:12]
    path = write_sample(UPLOADS / dataset_id / f"messy_customers_{rows}.csv", rows=rows)
    return _describe_dataset(dataset_id, path)


@app.get("/api/datasets/{dataset_id}")
def get_dataset(dataset_id: str) -> dict:
    meta = _dataset_dir(dataset_id) / "meta.json"
    return json.loads(meta.read_text())


class JobRequest(BaseModel):
    dataset_id: str
    config: dict[str, Any] = Field(default_factory=dict)


@app.post("/api/jobs", status_code=202)
def create_job(req: JobRequest) -> dict:
    src = _dataset_file(req.dataset_id)
    engine = req.config.get("engine", "auto")
    if engine not in ("auto", "pandas", "dask"):
        raise HTTPException(400, "engine must be auto, pandas or dask")
    job = jobs.submit(req.dataset_id, src, src.name, req.config)
    return job.public()


@app.get("/api/jobs")
def list_jobs() -> list[dict]:
    return jobs.list()


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str) -> dict:
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Unknown job")
    return job.public()


@app.get("/api/jobs/{job_id}/report")
def job_report(job_id: str) -> JSONResponse:
    rep = jobs.report(job_id)
    if rep is None:
        path = jobs.out_dir(job_id) / "report.json"
        if not re.fullmatch(r"[a-f0-9]{12}", job_id) or not path.exists():
            raise HTTPException(404, "Report not available (job unknown or not finished)")
        rep = json.loads(path.read_text())
    return JSONResponse(json.loads(json.dumps(rep, default=str)))


@app.get("/api/jobs/{job_id}/download/{kind}")
def download(job_id: str, kind: str) -> FileResponse:
    if not re.fullmatch(r"[a-f0-9]{12}", job_id):
        raise HTTPException(404, "Unknown job")
    out = jobs.out_dir(job_id)
    report_path = out / "report.json"
    if not report_path.exists():
        raise HTTPException(404, "Job has no outputs yet")
    outputs = json.loads(report_path.read_text())["outputs"]
    name = outputs.get(kind)
    if kind not in ("cleaned", "quarantine", "report") or not name:
        raise HTTPException(404, f"No '{kind}' output for this job")
    path = out / name
    if path.is_dir():  # Dask parquet output is a directory -> zip it
        path = Path(shutil.make_archive(str(path), "zip", root_dir=path))
    return FileResponse(path, filename=f"{kind}_{job_id}{path.suffix}")


# ------------------------------------------------------------------ frontend
if settings.frontend_dir.exists():
    app.mount("/", StaticFiles(directory=settings.frontend_dir, html=True), name="frontend")
