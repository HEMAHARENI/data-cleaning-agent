from __future__ import annotations

import io
import os
import time

os.environ.setdefault("DQ_DASK_PROCESSES", "false")

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402


def _wait(client, job_id, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        j = client.get(f"/api/jobs/{job_id}").json()
        if j["status"] in ("completed", "failed"):
            return j
        time.sleep(0.2)
    raise TimeoutError(job_id)


def test_upload_run_download():
    csv = "Name,Score\nA,1\nB,2\nB,2\nC,\nD,4\n"
    with TestClient(app) as c:
        r = c.post("/api/datasets", files={"file": ("t.csv", io.BytesIO(csv.encode()), "text/csv")})
        assert r.status_code == 200, r.text
        ds = r.json()
        assert ds["engine"] == "pandas" and [x["name"] for x in ds["columns"]] == ["name", "score"]
        job = c.post("/api/jobs", json={"dataset_id": ds["dataset_id"], "config": {}}).json()
        j = _wait(c, job["id"])
        assert j["status"] == "completed", j
        assert j["summary"]["duplicates_removed"] == 1
        body = c.get(f"/api/jobs/{job['id']}/download/cleaned").text
        assert body.splitlines()[0] == "name,score" and len(body.splitlines()) == 5
        assert c.get(f"/api/jobs/{job['id']}/report").json()["score_after"]["overall"] == 100.0


def test_rejects_bad_input():
    with TestClient(app) as c:
        assert c.post("/api/datasets", files={"file": ("x.exe", b"MZ", "application/octet-stream")}).status_code == 400
        assert c.get("/api/jobs/nope").status_code == 404
        assert c.post("/api/jobs", json={"dataset_id": "../../etc", "config": {}}).status_code == 404


def test_sample_endpoint_and_health():
    with TestClient(app) as c:
        ds = c.post("/api/datasets/sample?rows=500").json()
        assert len(ds["columns"]) == 11
        assert c.get("/api/health").json()["status"] == "ok"


def test_gzip_and_zip_uploads():
    import gzip
    import zipfile

    csv = b"Name,Score\nA,1\nB,2\n"
    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, "w") as zf:
        zf.writestr("inner/data.csv", csv)
    with TestClient(app) as c:
        r = c.post("/api/datasets", files={"file": ("t.csv.gz", io.BytesIO(gzip.compress(csv)), "application/gzip")})
        assert r.status_code == 200, r.text
        assert r.json()["filename"] == "t.csv" and r.json()["size_bytes"] == len(csv)
        r = c.post("/api/datasets", files={"file": ("bundle.zip", io.BytesIO(zbuf.getvalue()), "application/zip")})
        assert r.status_code == 200, r.text
        assert r.json()["filename"] == "data.csv"
