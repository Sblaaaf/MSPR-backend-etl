"""
Tests for the ETL service API routes.
Pipeline execution and DB calls are mocked to keep tests fast and isolated.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))

import os
os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "postgres")
os.environ.setdefault("DB_NAME", "healthai")
os.environ.setdefault("ETL_SCHEDULER_ENABLED", "false")

from main import app

client = TestClient(app)


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------

def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["service"] == "healthai_etl"
    assert "version" in body


# ---------------------------------------------------------------------------
# /etl/run
# ---------------------------------------------------------------------------

def test_etl_run_starts_pipeline(monkeypatch):
    monkeypatch.setattr("app.routes.get_run_en_cours", lambda: False)

    with patch("app.routes.run_pipeline"):
        resp = client.post("/etl/run")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "started"
    assert "message" in body


def test_etl_run_conflict_when_already_running(monkeypatch):
    monkeypatch.setattr("app.routes.get_run_en_cours", lambda: True)

    resp = client.post("/etl/run")
    assert resp.status_code == 409
    assert "en cours" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# /etl/status
# ---------------------------------------------------------------------------

MOCK_STATUS = {
    "en_cours": False,
    "run_id": "abc-123",
    "started_at": datetime(2026, 5, 31, 3, 0, 0).isoformat(),
    "finished_at": datetime(2026, 5, 31, 3, 0, 5).isoformat(),
    "statut": "succes",
    "nb_etl_total": 5,
    "nb_etl_succes": 5,
    "nb_etl_erreur": 0,
    "duree_secondes": 4.34,
    "declencheur": "scheduler",
}


def _make_mock_engine(row):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_result = MagicMock()
    mock_result.mappings.return_value.first.return_value = row
    mock_result.mappings.return_value.fetchone.return_value = row
    mock_result.mappings.return_value.fetchall.return_value = [row] if row else []
    mock_conn.execute.return_value = mock_result
    mock_engine.connect.return_value = mock_conn
    return mock_engine


def test_etl_status_returns_last_run(monkeypatch):
    monkeypatch.setattr("app.routes.get_run_en_cours", lambda: False)

    with patch("app.routes._get_engine", return_value=_make_mock_engine(MOCK_STATUS)):
        resp = client.get("/etl/status")

    assert resp.status_code == 200
    body = resp.json()
    assert body["en_cours"] is False
    assert body["statut"] == "succes"


def test_etl_status_no_run_yet(monkeypatch):
    monkeypatch.setattr("app.routes.get_run_en_cours", lambda: False)

    with patch("app.routes._get_engine", return_value=_make_mock_engine(None)):
        resp = client.get("/etl/status")

    assert resp.status_code == 200
    assert resp.json()["en_cours"] is False


# ---------------------------------------------------------------------------
# /etl/history
# ---------------------------------------------------------------------------

MOCK_HISTORY = [
    {**MOCK_STATUS, "run_id": "abc-123"},
    {**MOCK_STATUS, "run_id": "abc-124", "statut": "erreur", "nb_etl_erreur": 2},
]


def test_etl_history_returns_list(monkeypatch):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_result = MagicMock()
    mock_result.mappings.return_value.fetchall.return_value = MOCK_HISTORY
    mock_conn.execute.return_value = mock_result
    mock_engine.connect.return_value = mock_conn

    with patch("app.routes._get_engine", return_value=mock_engine):
        resp = client.get("/etl/history")

    assert resp.status_code == 200
    # response is {"runs": [...], "total": N}
    body = resp.json()
    assert "runs" in body
    assert isinstance(body["runs"], list)


# ---------------------------------------------------------------------------
# /datasets
# ---------------------------------------------------------------------------

def test_datasets_list(monkeypatch, tmp_path):
    (tmp_path / "aliments.csv").write_text("nom,calories\npoulet,165\n")
    (tmp_path / "exercises.json").write_text('[{"name":"squat"}]')

    with patch("app.routes.DATA_DIR", tmp_path):
        resp = client.get("/datasets")

    assert resp.status_code == 200
    body = resp.json()
    # response is {"datasets": [...], "total": N}
    assert "datasets" in body
    names = [d["filename"] for d in body["datasets"]]
    assert "aliments.csv" in names
    assert "exercises.json" in names


def test_datasets_upload_csv(monkeypatch, tmp_path):
    with patch("app.routes.DATA_DIR", tmp_path), patch("app.routes.run_pipeline"):
        resp = client.post(
            "/datasets/upload",
            files={"file": ("test.csv", b"col1,col2\n1,2\n", "text/csv")},
        )

    assert resp.status_code == 200
    body = resp.json()
    # response contains file info (name or message)
    assert "test.csv" in str(body)


def test_datasets_upload_invalid_extension(monkeypatch, tmp_path):
    with patch("app.routes.DATA_DIR", tmp_path):
        resp = client.post(
            "/datasets/upload",
            files={"file": ("malware.exe", b"bad content", "application/octet-stream")},
        )

    assert resp.status_code == 400


def test_datasets_delete_existing(monkeypatch, tmp_path):
    target = tmp_path / "to_delete.csv"
    target.write_text("data")

    with patch("app.routes.DATA_DIR", tmp_path):
        resp = client.delete("/datasets/to_delete.csv")

    assert resp.status_code == 200
    assert not target.exists()


def test_datasets_delete_not_found(monkeypatch, tmp_path):
    with patch("app.routes.DATA_DIR", tmp_path):
        resp = client.delete("/datasets/ghost.csv")

    assert resp.status_code == 404


def test_datasets_delete_path_traversal(monkeypatch, tmp_path):
    with patch("app.routes.DATA_DIR", tmp_path):
        resp = client.delete("/datasets/../../etc/passwd")

    assert resp.status_code in (400, 404)
