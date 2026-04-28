import csv
import io
import os
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from etl_pipeline import run_pipeline, get_run_en_cours

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
ALLOWED_EXTENSIONS = {".csv", ".json", ".xlsx"}
MAX_FILE_BYTES = 100 * 1024 * 1024  # 100 MB

# Simple in-memory rate limiter for upload / Kaggle endpoints
_upload_calls: dict[str, list[float]] = {}


def _check_rate(ip: str, limit: int = 10, window: int = 60) -> None:
    import time
    now = time.time()
    calls = [t for t in _upload_calls.get(ip, []) if now - t < window]
    if len(calls) >= limit:
        raise HTTPException(status_code=429, detail=f"Trop de requêtes — limite {limit}/{window}s")
    calls.append(now)
    _upload_calls[ip] = calls

router = APIRouter()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     os.getenv("DB_PORT",     "5432"),
    "dbname":   os.getenv("DB_NAME",     "healthai"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}


def _get_engine():
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    return create_engine(url, pool_pre_ping=True)


# ------------------------------------------------------------------
# Santé du service
# ------------------------------------------------------------------

@router.get("/health", tags=["monitoring"])
async def health_check():
    return {"status": "ok", "service": "healthai_etl", "version": "2.0.0"}


# ------------------------------------------------------------------
# Déclenchement du pipeline
# ------------------------------------------------------------------

@router.post("/etl/run", tags=["etl"], summary="Déclenche le pipeline ETL en arrière-plan")
async def run_etl(background_tasks: BackgroundTasks):
    if get_run_en_cours():
        raise HTTPException(status_code=409, detail="Un pipeline est déjà en cours d'exécution.")
    background_tasks.add_task(run_pipeline, "manuel")
    return {
        "status":  "started",
        "message": "Pipeline ETL démarré en arrière-plan. Consultez /etl/status pour suivre l'avancement.",
    }


# ------------------------------------------------------------------
# Statut du run en cours
# ------------------------------------------------------------------

@router.get("/etl/status", tags=["etl"], summary="Statut du pipeline (run en cours ou dernier run)")
async def etl_status():
    run_actif = get_run_en_cours()
    if run_actif:
        return {"en_cours": True, **run_actif}

    # Pas de run actif → retourner le dernier run depuis la base
    try:
        engine = _get_engine()
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT run_id, started_at, finished_at, statut,
                       nb_etl_total, nb_etl_succes, nb_etl_erreur,
                       duree_secondes, declencheur
                FROM etl_run_log
                ORDER BY started_at DESC
                LIMIT 1
            """)).mappings().fetchone()

        if row is None:
            return {"en_cours": False, "message": "Aucun run enregistré."}

        return {"en_cours": False, **dict(row)}

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {e}")


# ------------------------------------------------------------------
# Historique des runs
# ------------------------------------------------------------------

@router.get("/etl/history", tags=["etl"], summary="Historique des exécutions ETL")
async def etl_history(limit: int = 20):
    try:
        engine = _get_engine()
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT run_id, started_at, finished_at, statut,
                       nb_etl_total, nb_etl_succes, nb_etl_erreur,
                       duree_secondes, declencheur
                FROM etl_run_log
                ORDER BY started_at DESC
                LIMIT :limit
            """), {"limit": limit}).mappings().fetchall()

        return {"runs": [dict(r) for r in rows], "total": len(rows)}

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {e}")


# ------------------------------------------------------------------
# Rapport de qualité du dernier run
# ------------------------------------------------------------------

@router.get("/etl/quality", tags=["etl"], summary="Rapport qualité du dernier run ETL")
async def etl_quality_report() -> Any:
    try:
        engine = _get_engine()
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT run_id, started_at, statut, rapport_json
                FROM etl_run_log
                ORDER BY started_at DESC
                LIMIT 1
            """)).mappings().fetchone()

        if row is None:
            raise HTTPException(status_code=404, detail="Aucun rapport disponible.")

        rapport = row["rapport_json"]
        if isinstance(rapport, str):
            rapport = json.loads(rapport)

        return {
            "run_id":     row["run_id"],
            "started_at": row["started_at"],
            "statut":     row["statut"],
            "rapports":   rapport,
        }

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {e}")


# ------------------------------------------------------------------
# Métriques de qualité en base (vue synthétique par table)
# ------------------------------------------------------------------

@router.get("/etl/data-quality", tags=["etl"], summary="Métriques de qualité des données en base")
async def data_quality_metrics():
    queries: dict[str, str] = {
        "utilisateurs":         "SELECT COUNT(*) FROM utilisateur",
        "aliments":             "SELECT COUNT(*) FROM aliment",
        "exercices":            "SELECT COUNT(*) FROM exercice",
        "metriques":            "SELECT COUNT(*) FROM metrique_quotidienne",
        "utilisateurs_actifs":  "SELECT COUNT(*) FROM utilisateur WHERE actif = TRUE",
        "nulls_poids_users":    "SELECT COUNT(*) FROM utilisateur WHERE poids_initial_kg IS NULL",
        "nulls_calories_alim":  "SELECT COUNT(*) FROM aliment WHERE calories_100g IS NULL OR calories_100g = 0",
    }

    try:
        engine = _get_engine()
        result: dict[str, Any] = {}
        with engine.connect() as conn:
            for cle, sql in queries.items():
                val = conn.execute(text(sql)).scalar()
                result[cle] = val
        return result

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {e}")


# ------------------------------------------------------------------
# Gestion des fichiers datasets (upload / liste / suppression)
# ------------------------------------------------------------------

@router.get("/datasets", tags=["datasets"], summary="Lister les fichiers disponibles")
async def list_datasets():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    files = []
    for f in sorted(DATA_DIR.iterdir()):
        if f.is_file() and f.suffix.lower() in ALLOWED_EXTENSIONS:
            stat = f.stat()
            files.append({
                "filename": f.name,
                "size_bytes": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            })
    return {"datasets": files, "total": len(files)}


@router.post("/datasets/upload", tags=["datasets"], summary="Uploader un fichier dataset (CSV/JSON/XLSX)")
async def upload_dataset(
    request: Request,
    file: UploadFile = File(...),
    trigger_etl: bool = Form(default=True),
    background_tasks: BackgroundTasks = None,
):
    _check_rate(request.client.host, limit=10, window=60)

    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(400, f"Extension non supportée : {ext}. Acceptés : csv, json, xlsx")

    # Sécurité : pas de path traversal
    safe_name = Path(file.filename).name
    content = await file.read()

    if len(content) > MAX_FILE_BYTES:
        raise HTTPException(413, f"Fichier trop volumineux (max 100 Mo, reçu {len(content) // 1024 // 1024} Mo)")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    dest = DATA_DIR / safe_name
    dest.write_bytes(content)

    if trigger_etl and background_tasks is not None:
        background_tasks.add_task(run_pipeline, "upload")

    return {
        "uploaded": safe_name,
        "size_bytes": len(content),
        "etl_triggered": trigger_etl,
    }


@router.delete("/datasets/{filename}", tags=["datasets"], summary="Supprimer un fichier dataset")
async def delete_dataset(filename: str):
    if "/" in filename or ".." in filename:
        raise HTTPException(400, "Nom de fichier invalide")
    path = DATA_DIR / filename
    if not path.exists():
        raise HTTPException(404, "Fichier introuvable")
    path.unlink()
    return {"deleted": filename}


# ------------------------------------------------------------------
# Intégration Kaggle
# ------------------------------------------------------------------

def _kaggle_env() -> dict:
    """Retourne les variables d'env pour le CLI kaggle."""
    env = os.environ.copy()
    # Le CLI kaggle lit KAGGLE_USERNAME et KAGGLE_KEY en variable d'env
    return env


@router.get("/kaggle/search", tags=["kaggle"], summary="Rechercher des datasets Kaggle")
async def kaggle_search(request: Request, q: str = "", page: int = 1):
    _check_rate(request.client.host, limit=20, window=60)

    username = os.getenv("KAGGLE_USERNAME")
    kaggle_key = os.getenv("KAGGLE_KEY")
    if not username or not kaggle_key:
        raise HTTPException(503, "Kaggle non configuré — définir KAGGLE_USERNAME et KAGGLE_KEY")

    try:
        result = subprocess.run(
            ["kaggle", "datasets", "list", "--search", q or "health",
             "--page", str(page), "--csv", "--max-size", "500"],
            capture_output=True, text=True, env=_kaggle_env(), timeout=30,
        )
    except FileNotFoundError:
        raise HTTPException(503, "CLI kaggle introuvable — vérifier l'installation")
    except subprocess.TimeoutExpired:
        raise HTTPException(504, "Timeout Kaggle (>30s)")

    if result.returncode != 0:
        raise HTTPException(500, f"Kaggle error: {result.stderr[:300]}")

    reader = csv.DictReader(io.StringIO(result.stdout))
    datasets = [row for row in reader]
    return {"datasets": datasets, "page": page, "query": q}


@router.post("/kaggle/download", tags=["kaggle"], summary="Télécharger un dataset Kaggle et lancer l'ETL")
async def kaggle_download(
    request: Request,
    background_tasks: BackgroundTasks,
    dataset: str = Form(...),
    trigger_etl: bool = Form(default=True),
):
    _check_rate(request.client.host, limit=3, window=60)

    username = os.getenv("KAGGLE_USERNAME")
    kaggle_key = os.getenv("KAGGLE_KEY")
    if not username or not kaggle_key:
        raise HTTPException(503, "Kaggle non configuré — définir KAGGLE_USERNAME et KAGGLE_KEY")

    # Validation format owner/dataset-slug
    if "/" not in dataset or len(dataset.split("/")) != 2:
        raise HTTPException(400, "Format attendu : owner/dataset-slug")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        result = subprocess.run(
            ["kaggle", "datasets", "download", dataset,
             "--unzip", "--path", str(DATA_DIR)],
            capture_output=True, text=True, env=_kaggle_env(), timeout=120,
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(504, "Timeout téléchargement Kaggle (>120s)")

    if result.returncode != 0:
        raise HTTPException(500, f"Kaggle download error: {result.stderr[:300]}")

    if trigger_etl:
        background_tasks.add_task(run_pipeline, "kaggle")

    return {
        "downloaded": dataset,
        "output_dir": str(DATA_DIR),
        "etl_triggered": trigger_etl,
        "stdout": result.stdout[:500],
    }
