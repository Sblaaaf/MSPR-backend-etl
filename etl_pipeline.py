"""
=============================================================
 HealthAI Coach — Pipeline ETL
 Ingestion, nettoyage et chargement des datasets Kaggle
 Version : 2.0.0
=============================================================
 Sources traitées :
   1. Daily Food & Nutrition Dataset  → table aliment
   2. Gym Members Exercise Dataset    → tables utilisateur + metrique_quotidienne
   3. Fitness Tracker Dataset         → table metrique_quotidienne (complémentaire)
   4. Diet Recommendations Dataset    → table utilisateur + objectif
=============================================================
 Prérequis :
   pip install pandas sqlalchemy psycopg2-binary openpyxl
   python etl_pipeline.py
=============================================================
"""

from __future__ import annotations

import os
import sys
import logging
import hashlib
import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# =============================================================
# CONFIGURATION
# =============================================================

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     os.getenv("DB_PORT",     "5432"),
    "dbname":   os.getenv("DB_NAME",     "healthai"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
LOG_DIR  = Path("./logs")
LOG_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

# =============================================================
# SCHÉMAS DE VALIDATION PAR DATASET
# Chaque entrée définit les colonnes attendues et leurs règles.
# =============================================================

VALIDATION_SCHEMAS: dict[str, dict] = {
    "aliments": {
        "colonnes_requises": ["nom"],
        "colonnes_numeriques": ["calories_100g", "proteines_g", "glucides_g", "lipides_g"],
        "nb_lignes_min": 5,
        "plages": {
            "calories_100g": (0, 9000),
            "proteines_g":   (0, 100),
            "glucides_g":    (0, 100),
            "lipides_g":     (0, 100),
        },
    },
    "gym_members": {
        "colonnes_requises": ["poids_initial_kg"],
        "colonnes_numeriques": ["poids_initial_kg", "bpm_max", "bpm_repos", "calories_brulees"],
        "nb_lignes_min": 5,
        "plages": {
            "poids_initial_kg": (30, 300),
            "bpm_max":          (50, 300),
            "bpm_repos":        (30, 250),
            "calories_brulees": (0, 5000),
        },
    },
    "exercices": {
        "colonnes_requises": ["nom"],
        "colonnes_numeriques": [],
        "nb_lignes_min": 3,
        "plages": {},
    },
    "diet_recommendations": {
        "colonnes_requises": [],
        "colonnes_numeriques": [],
        "nb_lignes_min": 1,
        "plages": {},
    },
}

# =============================================================
# LOGGER
# =============================================================

def setup_logger() -> logging.Logger:
    log_file = LOG_DIR / f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = logging.getLogger("healthai_etl")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


logger = setup_logger()


# =============================================================
# CONNEXION BASE DE DONNÉES
# =============================================================

def get_engine():
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    try:
        engine = create_engine(url, echo=False, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Connexion PostgreSQL établie avec succès.")
        return engine
    except SQLAlchemyError as e:
        logger.error(f"Impossible de se connecter à la base : {e}")
        sys.exit(1)


def init_log_table(engine):
    """Crée la table etl_run_log si elle n'existe pas encore."""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS etl_run_log (
                id              SERIAL      PRIMARY KEY,
                run_id          UUID        NOT NULL DEFAULT gen_random_uuid(),
                started_at      TIMESTAMP   NOT NULL,
                finished_at     TIMESTAMP,
                statut          VARCHAR(20) NOT NULL DEFAULT 'en_cours',
                nb_etl_total    SMALLINT,
                nb_etl_succes   SMALLINT,
                nb_etl_erreur   SMALLINT,
                duree_secondes  NUMERIC(8,2),
                rapport_json    JSONB,
                declencheur     VARCHAR(50) NOT NULL DEFAULT 'manuel'
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_etl_run_started
            ON etl_run_log(started_at DESC)
        """))
        conn.commit()
    logger.info("Table etl_run_log vérifiée/créée.")


# =============================================================
# VALIDATION DES DATASETS
# =============================================================

def valider_dataset(df: pd.DataFrame, nom_schema: str) -> dict:
    """
    Valide un DataFrame contre son schéma de validation.
    Retourne un dict avec statut, erreurs et avertissements.
    """
    schema = VALIDATION_SCHEMAS.get(nom_schema, {})
    erreurs = []
    avertissements = []

    # 1. Nombre de lignes minimum
    nb_min = schema.get("nb_lignes_min", 1)
    if len(df) < nb_min:
        erreurs.append(f"Trop peu de lignes : {len(df)} < {nb_min} minimum requis")

    # 2. Colonnes requises
    for col in schema.get("colonnes_requises", []):
        if col not in df.columns:
            erreurs.append(f"Colonne requise manquante : '{col}'")

    # 3. Valeurs nulles dans les colonnes critiques
    for col in schema.get("colonnes_requises", []):
        if col in df.columns:
            nb_nulls = df[col].isnull().sum()
            if nb_nulls > 0:
                avertissements.append(f"'{col}' contient {nb_nulls} valeurs nulles")

    # 4. Plages de valeurs numériques
    for col, (min_val, max_val) in schema.get("plages", {}).items():
        if col in df.columns:
            serie = pd.to_numeric(df[col], errors="coerce")
            hors_plage = serie.notna() & ((serie < min_val) | (serie > max_val))
            nb_hors = int(hors_plage.sum())
            if nb_hors > 0:
                avertissements.append(
                    f"'{col}' : {nb_hors} valeurs hors plage [{min_val}, {max_val}]"
                )

    # 5. Doublons
    nb_doublons = int(df.duplicated().sum())
    if nb_doublons > 0:
        avertissements.append(f"{nb_doublons} lignes dupliquées détectées")

    statut = "ko" if erreurs else ("warning" if avertissements else "ok")
    resultat = {
        "schema":          nom_schema,
        "statut":          statut,
        "nb_lignes":       len(df),
        "erreurs":         erreurs,
        "avertissements":  avertissements,
        "timestamp":       datetime.now().isoformat(),
    }

    if erreurs:
        logger.error(f"[Validation {nom_schema}] ÉCHEC — {erreurs}")
    elif avertissements:
        logger.warning(f"[Validation {nom_schema}] OK avec avertissements — {avertissements}")
    else:
        logger.info(f"[Validation {nom_schema}] OK")

    return resultat


# =============================================================
# UTILITAIRES GÉNÉRIQUES
# =============================================================

def rapport_qualite(df: pd.DataFrame, nom_dataset: str) -> dict:
    rapport = {
        "dataset":          nom_dataset,
        "nb_lignes":        len(df),
        "nb_colonnes":      len(df.columns),
        "valeurs_nulles":   df.isnull().sum().to_dict(),
        "doublons":         int(df.duplicated().sum()),
        "types":            df.dtypes.astype(str).to_dict(),
        "timestamp":        datetime.now().isoformat(),
    }
    logger.info(
        f"[{nom_dataset}] {rapport['nb_lignes']} lignes | "
        f"{rapport['doublons']} doublons | "
        f"{sum(rapport['valeurs_nulles'].values())} valeurs nulles"
    )
    return rapport


def sauvegarder_rapport(rapports: list):
    path = LOG_DIR / f"rapport_qualite_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rapports, f, ensure_ascii=False, indent=2)
    logger.info(f"Rapport de qualité sauvegardé : {path}")


def sauvegarder_run_en_base(
    engine,
    run_id: str,
    started_at: datetime,
    rapports: list,
    duree: float,
    declencheur: str = "manuel",
):
    """Persiste le résultat du run ETL dans etl_run_log."""
    nb_ok  = sum(1 for r in rapports if r.get("statut") == "succès")
    nb_err = len(rapports) - nb_ok

    # Sérialiser en JSON (les valeurs Python doivent être JSON-compatibles)
    rapport_json = json.dumps(rapports, ensure_ascii=False, default=str)

    try:
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO etl_run_log
                        (run_id, started_at, finished_at, statut,
                         nb_etl_total, nb_etl_succes, nb_etl_erreur,
                         duree_secondes, rapport_json, declencheur)
                    VALUES
                        (:run_id, :started_at, :finished_at, :statut,
                         :total, :succes, :erreur,
                         :duree, cast(:rapport as jsonb), :declencheur)
                """),
                {
                    "run_id":      run_id,
                    "started_at":  started_at,
                    "finished_at": datetime.now(),
                    "statut":      "erreur" if nb_err > 0 else "succes",
                    "total":       len(rapports),
                    "succes":      nb_ok,
                    "erreur":      nb_err,
                    "duree":       round(duree, 2),
                    "rapport":     rapport_json,
                    "declencheur": declencheur,
                },
            )
            conn.commit()
        logger.info(f"Run {run_id} sauvegardé en base.")
    except Exception as e:
        logger.error(f"Impossible de sauvegarder le run en base : {e}")


def charger_fichier(nom_fichier: str, **kwargs) -> pd.DataFrame | None:
    """Charge un fichier CSV, JSON ou XLSX depuis DATA_DIR."""
    chemin = DATA_DIR / nom_fichier
    if not chemin.exists():
        logger.warning(f"Fichier introuvable : {chemin}")
        return None

    ext = chemin.suffix.lower()
    try:
        if ext == ".csv":
            df = pd.read_csv(chemin, **kwargs)
        elif ext == ".json":
            df = pd.read_json(chemin, **kwargs)
        elif ext in (".xlsx", ".xls"):
            df = pd.read_excel(chemin, engine="openpyxl", **kwargs)
        else:
            logger.error(f"Format non supporté : {ext}")
            return None
        logger.info(f"Fichier chargé : {nom_fichier} ({len(df)} lignes)")
        return df
    except Exception as e:
        logger.error(f"Erreur lors du chargement de {nom_fichier} : {e}")
        return None


def inserer_en_base(df: pd.DataFrame, table: str, engine, mode: str = "append"):
    if df.empty:
        logger.warning(f"DataFrame vide — aucune insertion dans {table}.")
        return 0
    try:
        df.to_sql(table, engine, if_exists=mode, index=False, method="multi", chunksize=500)
        logger.info(f"[{table}] {len(df)} lignes insérées (mode={mode}).")
        return len(df)
    except SQLAlchemyError as e:
        logger.error(f"Erreur insertion dans {table} : {e}")
        return 0


# =============================================================
# ETL 1 — ALIMENTS (Daily Food & Nutrition Dataset)
# =============================================================

def etl_aliments(engine) -> dict:
    logger.info("=" * 50)
    logger.info("ETL 1 : Aliments — début")

    with engine.connect() as conn:
        conn.execute(text("DELETE FROM ligne_repas"))
        conn.execute(text("DELETE FROM journal_repas"))
        conn.execute(text("DELETE FROM aliment"))
        conn.commit()
    logger.info("Données aliments et dépendances supprimées.")

    df = charger_fichier("daily_food_nutrition_dataset.csv", encoding="utf-8", on_bad_lines="skip")

    if df is None:
        logger.warning("Fichier absent — génération de données simulées pour démo.")
        df = _simuler_aliments()

    rapport = rapport_qualite(df, "aliments_brut")

    # Renommage des colonnes
    rename_map = {
        "Food_Item":         "nom",
        "Food":              "nom",
        "food":              "nom",
        "food_item":         "nom",
        "Calories (kcal)":   "calories_100g",
        "Calories":          "calories_100g",
        "calories":          "calories_100g",
        "Protein (g)":       "proteines_g",
        "Protein":           "proteines_g",
        "protein":           "proteines_g",
        "Carbohydrates (g)": "glucides_g",
        "Carbohydrates":     "glucides_g",
        "carbs":             "glucides_g",
        "Fat (g)":           "lipides_g",
        "Fat":               "lipides_g",
        "fat":               "lipides_g",
        "Fiber (g)":         "fibres_g",
        "Fiber":             "fibres_g",
        "fiber":             "fibres_g",
        "Sugars (g)":        "sucres_g",
        "Sugar":             "sucres_g",
        "sugar":             "sucres_g",
        "Sodium (mg)":       "sodium_mg",
        "Sodium":            "sodium_mg",
        "Category":          "categorie",
        "category":          "categorie",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    for col in ["calories_100g", "proteines_g", "glucides_g", "lipides_g", "fibres_g"]:
        if col not in df.columns:
            df[col] = 0.0

    # Validation
    rapport["validation"] = valider_dataset(df, "aliments")

    df = df.dropna(subset=["nom"])
    df["nom"] = df["nom"].str.strip().str.title()

    cols_num = ["calories_100g", "proteines_g", "glucides_g", "lipides_g", "fibres_g"]
    for col in cols_num:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].fillna(0.0).clip(lower=0)

    avant = len(df)
    df = df.drop_duplicates(subset=["nom"], keep="first")
    logger.info(f"Doublons supprimés : {avant - len(df)}")

    df["source_dataset"] = "kaggle_daily_food_nutrition"

    cols_finales = ["nom", "categorie", "calories_100g", "proteines_g",
                    "glucides_g", "lipides_g", "fibres_g", "sodium_mg",
                    "sucres_g", "source_dataset"]
    df = df[[c for c in cols_finales if c in df.columns]]

    rapport["apres_nettoyage"] = len(df)
    nb = inserer_en_base(df, "aliment", engine)
    rapport["statut"] = "succès"
    logger.info(f"ETL 1 terminé — {nb} aliments insérés.")
    return rapport


def _simuler_aliments() -> pd.DataFrame:
    data = [
        {"nom": "Poulet rôti",      "categorie": "Viandes",      "calories_100g": 165, "proteines_g": 31, "glucides_g": 0,  "lipides_g": 3.6, "fibres_g": 0},
        {"nom": "Riz blanc cuit",   "categorie": "Féculents",    "calories_100g": 130, "proteines_g": 2.7,"glucides_g": 28, "lipides_g": 0.3, "fibres_g": 0.4},
        {"nom": "Saumon frais",     "categorie": "Poissons",     "calories_100g": 208, "proteines_g": 20, "glucides_g": 0,  "lipides_g": 13,  "fibres_g": 0},
        {"nom": "Brocoli vapeur",   "categorie": "Légumes",      "calories_100g": 35,  "proteines_g": 2.4,"glucides_g": 7,  "lipides_g": 0.4, "fibres_g": 2.6},
        {"nom": "Œuf entier",       "categorie": "Œufs/Laitiers","calories_100g": 155, "proteines_g": 13, "glucides_g": 1.1,"lipides_g": 11,  "fibres_g": 0},
        {"nom": "Avoine",           "categorie": "Céréales",     "calories_100g": 389, "proteines_g": 17, "glucides_g": 66, "lipides_g": 7,   "fibres_g": 10},
        {"nom": "Banane",           "categorie": "Fruits",       "calories_100g": 89,  "proteines_g": 1.1,"glucides_g": 23, "lipides_g": 0.3, "fibres_g": 2.6},
        {"nom": "Lentilles cuites", "categorie": "Légumineuses", "calories_100g": 116, "proteines_g": 9,  "glucides_g": 20, "lipides_g": 0.4, "fibres_g": 7.9},
        {"nom": "Huile d'olive",    "categorie": "Corps gras",   "calories_100g": 884, "proteines_g": 0,  "glucides_g": 0,  "lipides_g": 100, "fibres_g": 0},
        {"nom": "Yaourt nature 0%", "categorie": "Laitiers",     "calories_100g": 59,  "proteines_g": 10, "glucides_g": 3.6,"lipides_g": 0.4, "fibres_g": 0},
    ]
    return pd.DataFrame(data)


# =============================================================
# ETL 2 — UTILISATEURS & MÉTRIQUES (Gym Members Exercise Dataset)
# =============================================================

def etl_utilisateurs_metriques(engine) -> dict:
    logger.info("=" * 50)
    logger.info("ETL 2 : Utilisateurs & Métriques — début")

    with engine.connect() as conn:
        conn.execute(text("DELETE FROM metrique_quotidienne"))
        conn.execute(text("DELETE FROM utilisateur"))
        conn.commit()
    logger.info("Données utilisateurs existantes supprimées.")

    df = charger_fichier("gym_members_exercise.csv", encoding="utf-8")

    if df is None:
        logger.warning("Fichier absent — génération de données simulées.")
        df = _simuler_gym_members()

    rapport = rapport_qualite(df, "gym_members_brut")

    rename_map = {
        "Age":              "age",
        "Gender":           "sexe",
        "Weight (kg)":      "poids_initial_kg",
        "Height (m)":       "taille_m",
        "Max_BPM":          "bpm_max",
        "Avg_BPM":          "bpm_repos",
        "Session_Duration (hours)": "duree_seance_h",
        "Calories_Burned":  "calories_brulees",
        "BMI":              "imc",
        "Fat_Percentage":   "body_fat_pct",
        "Workout_Type":     "type_sport",
        "Workout_Frequency (days/week)": "freq_semaine",
        "Experience_Level": "niveau",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    if "taille_m" in df.columns:
        df["taille_cm"] = (df["taille_m"] * 100).round(0).astype("Int64")
    else:
        df["taille_cm"] = None

    if "sexe" in df.columns:
        df["sexe"] = df["sexe"].str.lower().map({
            "male": "homme", "female": "femme",
            "homme": "homme", "femme": "femme",
            "m": "homme", "f": "femme",
        }).fillna("non_renseigne")

    for col in ["poids_initial_kg", "bpm_max", "bpm_repos", "calories_brulees", "imc", "body_fat_pct"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].clip(lower=0)

    # Validation avant filtrage
    rapport["validation"] = valider_dataset(df, "gym_members")

    if "poids_initial_kg" in df.columns:
        df = df[df["poids_initial_kg"].between(30, 300) | df["poids_initial_kg"].isna()]
    if "bpm_max" in df.columns:
        df = df[df["bpm_max"].between(50, 300) | df["bpm_max"].isna()]

    df = df.dropna(subset=["poids_initial_kg"])
    df = df.reset_index(drop=True)

    utilisateurs = []
    for i, row in df.iterrows():
        email = f"user_{i+1:05d}@healthai.demo"
        mdp_hash = hashlib.sha256(f"demo_{i}".encode()).hexdigest()
        utilisateurs.append({
            "nom":              f"User{i+1:05d}",
            "prenom":           "Demo",
            "email":            email,
            "mdp_hash":         mdp_hash,
            "sexe":             row.get("sexe", "non_renseigne"),
            "poids_initial_kg": row.get("poids_initial_kg"),
            "taille_cm":        row.get("taille_cm"),
            "abonnement":       "freemium",
            "imc":              row.get("imc"),
        })

    df_users = pd.DataFrame(utilisateurs)

    with engine.connect() as conn:
        try:
            result = conn.execute(text("SELECT email FROM utilisateur"))
            emails_existants = pd.DataFrame(result.mappings().all())
            df_users = df_users[~df_users["email"].isin(emails_existants["email"])]
        except Exception:
            pass

    nb_users = inserer_en_base(df_users, "utilisateur", engine)

    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT id, email FROM utilisateur WHERE email LIKE 'user_%@healthai.demo'"
        ))
        df_ids = pd.DataFrame(result.mappings().all())

    metriques = []
    for _, row_id in df_ids.iterrows():
        idx = int(row_id["email"].split("_")[1].split("@")[0]) - 1
        if idx >= len(df):
            continue
        row = df.iloc[idx]
        for j in range(30):
            date_j = datetime.now().date() - timedelta(days=29 - j)
            poids_j = row.get("poids_initial_kg", 70)
            if pd.notna(poids_j):
                variation = (j - 15) * 0.05
                poids_j = round(float(poids_j) + variation, 2)
            metriques.append({
                "utilisateur_id":  row_id["id"],
                "date_mesure":     date_j,
                "poids_kg":        poids_j if pd.notna(poids_j) else None,
                "bpm_repos":       int(row["bpm_repos"]) if pd.notna(row.get("bpm_repos")) else None,
                "bpm_max":         int(row["bpm_max"]) if pd.notna(row.get("bpm_max")) else None,
                "calories_brulees":float(row["calories_brulees"]) if pd.notna(row.get("calories_brulees")) else None,
                "body_fat_pct":    float(row["body_fat_pct"]) if pd.notna(row.get("body_fat_pct")) else None,
                "source":          "kaggle_gym_members",
            })

    df_metriques = pd.DataFrame(metriques)
    nb_metriques = inserer_en_base(df_metriques, "metrique_quotidienne", engine)

    rapport["utilisateurs_inseres"] = nb_users
    rapport["metriques_inserees"]   = nb_metriques
    rapport["statut"] = "succès"
    logger.info(f"ETL 2 terminé — {nb_users} users + {nb_metriques} métriques.")
    return rapport


def _simuler_gym_members() -> pd.DataFrame:
    import random
    random.seed(42)
    data = []
    for i in range(50):
        data.append({
            "age":              random.randint(20, 65),
            "sexe":             random.choice(["homme", "femme"]),
            "poids_initial_kg": round(random.uniform(55, 110), 1),
            "taille_m":         round(random.uniform(1.55, 1.95), 2),
            "bpm_max":          random.randint(150, 200),
            "bpm_repos":        random.randint(55, 90),
            "calories_brulees": round(random.uniform(200, 900), 1),
            "imc":              round(random.uniform(18.5, 35), 2),
            "body_fat_pct":     round(random.uniform(10, 35), 1),
        })
    return pd.DataFrame(data)


# =============================================================
# ETL 3 — EXERCICES (ExerciseDB — données simulées / JSON)
# =============================================================

def etl_exercices(engine) -> dict:
    logger.info("=" * 50)
    logger.info("ETL 3 : Exercices — début")

    with engine.connect() as conn:
        conn.execute(text("DELETE FROM exercice_muscle"))
        conn.execute(text("DELETE FROM exercice"))
        conn.commit()
    logger.info("Données exercices existantes supprimées.")

    df = charger_fichier("exercises.json")

    if df is None:
        logger.warning("Fichier exercises.json absent — génération de données simulées.")
        df = _simuler_exercices()

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else x)

    rapport = rapport_qualite(df, "exercices_brut")

    rename_map = {
        "name":        "nom",
        "bodyPart":    "type",
        "equipment":   "equipement",
        "gifUrl":      "image_url",
        "instructions":"instructions",
        "level":       "niveau",
        "target":      "muscle_principal",
        "secondaryMuscles": "muscles_secondaires",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Validation
    rapport["validation"] = valider_dataset(df, "exercices")

    df = df.dropna(subset=["nom"])
    df["nom"] = df["nom"].str.strip().str.title()

    if "niveau" in df.columns:
        df["niveau"] = df["niveau"].str.lower().map({
            "beginner":     "debutant",
            "intermediate": "intermediaire",
            "expert":       "avance",
            "debutant":     "debutant",
            "intermediaire":"intermediaire",
            "avance":       "avance",
        }).fillna("debutant")
    else:
        df["niveau"] = "debutant"

    if "type" in df.columns:
        df["type"] = df["type"].str.lower().map({
            "cardio":       "cardio",
            "back":         "musculation",
            "chest":        "musculation",
            "lower arms":   "musculation",
            "lower legs":   "musculation",
            "neck":         "stretching",
            "shoulders":    "musculation",
            "upper arms":   "musculation",
            "upper legs":   "musculation",
            "waist":        "musculation",
        }).fillna("musculation")
    else:
        df["type"] = "musculation"

    df = df.drop_duplicates(subset=["nom"])
    df["source_dataset"] = "exercisedb_api"

    cols_finales = ["nom", "type", "niveau", "equipement", "description",
                    "instructions", "image_url", "source_dataset"]
    df_exercices = df[[c for c in cols_finales if c in df.columns]]

    nb = inserer_en_base(df_exercices, "exercice", engine)

    if "muscle_principal" in df.columns:
        with engine.connect() as conn:
            result_ex = conn.execute(text("SELECT id, nom FROM exercice"))
            df_ex_ids = pd.DataFrame(result_ex.mappings().all())
            result_mu = conn.execute(text("SELECT id, nom FROM groupe_musculaire"))
            df_mu_ids = pd.DataFrame(result_mu.mappings().all())

        df_ex_ids["nom_lower"] = df_ex_ids["nom"].str.lower()
        assoc_rows = []

        for _, row in df.iterrows():
            nom_ex = str(row.get("nom", "")).title()
            match_ex = df_ex_ids[df_ex_ids["nom"] == nom_ex]
            if match_ex.empty:
                continue
            ex_id = int(match_ex.iloc[0]["id"])

            muscle_nom = str(row.get("muscle_principal", "")).lower()
            match_mu = df_mu_ids[df_mu_ids["nom"].str.lower() == muscle_nom]
            if not match_mu.empty:
                assoc_rows.append({
                    "exercice_id": ex_id,
                    "muscle_id":   int(match_mu.iloc[0]["id"]),
                    "role":        "principal"
                })

            muscles_secondaires = row.get("muscles_secondaires", [])
            if isinstance(muscles_secondaires, str):
                try:
                    import ast
                    muscles_secondaires = ast.literal_eval(muscles_secondaires)
                except Exception:
                    muscles_secondaires = []
            elif not isinstance(muscles_secondaires, list):
                muscles_secondaires = []

            for muscle_sec in muscles_secondaires:
                muscle_sec_nom = str(muscle_sec).lower()
                match_sec = df_mu_ids[df_mu_ids["nom"].str.lower() == muscle_sec_nom]
                if not match_sec.empty:
                    assoc_rows.append({
                        "exercice_id": ex_id,
                        "muscle_id":   int(match_sec.iloc[0]["id"]),
                        "role":        "secondaire"
                    })

        if assoc_rows:
            df_assoc = pd.DataFrame(assoc_rows).drop_duplicates()
            inserer_en_base(df_assoc, "exercice_muscle", engine)

    rapport["exercices_inseres"] = nb
    rapport["statut"] = "succès"
    logger.info(f"ETL 3 terminé — {nb} exercices insérés.")
    return rapport


def _simuler_exercices() -> pd.DataFrame:
    data = [
        {"nom": "Bench Press",  "type": "musculation", "niveau": "intermediaire", "equipement": "barbell",  "muscle_principal": "pectoraux",  "instructions": "Allongé sur le banc, poussez la barre vers le haut."},
        {"nom": "Squat",        "type": "musculation", "niveau": "debutant",      "equipement": "barbell",  "muscle_principal": "quadriceps", "instructions": "Descendez jusqu'à ce que les cuisses soient parallèles au sol."},
        {"nom": "Deadlift",     "type": "musculation", "niveau": "avance",        "equipement": "barbell",  "muscle_principal": "dorsaux",    "instructions": "Soulevez la barre depuis le sol en gardant le dos droit."},
        {"nom": "Pull Up",      "type": "musculation", "niveau": "intermediaire", "equipement": "barres",   "muscle_principal": "dorsaux",    "instructions": "Accrochez-vous à la barre et tirez-vous vers le haut."},
        {"nom": "Planche",      "type": "musculation", "niveau": "debutant",      "equipement": "aucun",    "muscle_principal": "abdominaux", "instructions": "Maintenez la position gainage 30-60 secondes."},
        {"nom": "Running",      "type": "cardio",      "niveau": "debutant",      "equipement": "aucun",    "muscle_principal": "quadriceps", "instructions": "Courez à un rythme confortable, dos droit."},
        {"nom": "Burpees",      "type": "hiit",        "niveau": "intermediaire", "equipement": "aucun",    "muscle_principal": "fessiers",   "instructions": "Enchaînez squat, planche, pompe, saut."},
        {"nom": "Bicep Curl",   "type": "musculation", "niveau": "debutant",      "equipement": "haltères", "muscle_principal": "biceps",     "instructions": "Fléchissez les coudes pour ramener les haltères aux épaules."},
        {"nom": "Tricep Dips",  "type": "musculation", "niveau": "debutant",      "equipement": "banc",     "muscle_principal": "triceps",    "instructions": "Descendez en fléchissant les coudes derrière vous."},
        {"nom": "Calf Raise",   "type": "musculation", "niveau": "debutant",      "equipement": "aucun",    "muscle_principal": "mollets",    "instructions": "Montez sur la pointe des pieds, tenez 1 seconde, redescendez."},
    ]
    return pd.DataFrame(data)


# =============================================================
# ETL 4 — OBJECTIFS UTILISATEURS (Diet Recommendations Dataset)
# =============================================================

def etl_objectifs_utilisateurs(engine) -> dict:
    logger.info("=" * 50)
    logger.info("ETL 4 : Association utilisateurs ↔ objectifs — début")

    df = charger_fichier("diet_recommendations.csv", encoding="utf-8")

    if df is None:
        logger.warning("Fichier absent — simulation des associations objectifs.")
        df = _simuler_objectifs()

    rapport = rapport_qualite(df, "diet_recommendations_brut")
    rapport["validation"] = valider_dataset(df, "diet_recommendations")

    objectif_map = {
        "weight loss":          "perte_de_poids",
        "muscle gain":          "prise_de_masse",
        "maintenance":          "maintien_forme",
        "sleep improvement":    "amelioration_sommeil",
        "endurance":            "endurance",
        "flexibility":          "flexibilite",
        "perte_de_poids":       "perte_de_poids",
        "prise_de_masse":       "prise_de_masse",
        "maintien_forme":       "maintien_forme",
    }

    if "Goal" in df.columns:
        df["objectif_libelle"] = df["Goal"].str.lower().map(objectif_map).fillna("maintien_forme")
    else:
        df["objectif_libelle"] = "maintien_forme"

    with engine.connect() as conn:
        result_users = conn.execute(text(
            "SELECT id FROM utilisateur WHERE email LIKE 'user_%@healthai.demo' ORDER BY id"
        ))
        df_users = pd.DataFrame(result_users.mappings().all())
        result_obj = conn.execute(text("SELECT id, libelle FROM objectif"))
        df_obj = pd.DataFrame(result_obj.mappings().all())

    if df_users.empty:
        logger.warning("Aucun utilisateur trouvé. ETL 4 ignoré.")
        rapport["statut"] = "succès"
        return rapport

    assoc_rows = []
    for i, (_, row_u) in enumerate(df_users.iterrows()):
        if i >= len(df):
            break
        libelle = df.iloc[i]["objectif_libelle"]
        match = df_obj[df_obj["libelle"] == libelle]
        if not match.empty:
            assoc_rows.append({
                "utilisateur_id": int(row_u["id"]),
                "objectif_id":    int(match.iloc[0]["id"]),
                "date_debut":     datetime.now().date(),
                "actif":          True,
            })

    nb = 0
    if assoc_rows:
        df_assoc = pd.DataFrame(assoc_rows).drop_duplicates(subset=["utilisateur_id", "objectif_id"])
        with engine.connect() as conn:
            for _, row in df_assoc.iterrows():
                conn.execute(text(
                    "DELETE FROM utilisateur_objectif WHERE utilisateur_id = :uid AND objectif_id = :oid"
                ), {"uid": int(row["utilisateur_id"]), "oid": int(row["objectif_id"])})
            conn.commit()
        nb = inserer_en_base(df_assoc, "utilisateur_objectif", engine)

    rapport["associations_inserees"] = nb
    rapport["statut"] = "succès"
    logger.info(f"ETL 4 terminé — {nb} associations objectifs insérées.")
    return rapport


def _simuler_objectifs() -> pd.DataFrame:
    import random
    random.seed(1)
    goals = ["perte_de_poids", "prise_de_masse", "maintien_forme",
             "amelioration_sommeil", "endurance", "flexibilite"]
    return pd.DataFrame({"objectif_libelle": [random.choice(goals) for _ in range(100)]})


# =============================================================
# ORCHESTRATEUR PRINCIPAL
# =============================================================

# Variable globale pour suivre l'état du run en cours
_run_en_cours: dict | None = None


# =============================================================
# ETL 5 — FICHIERS ADDITIONNELS (auto-détection Kaggle / uploads)
# =============================================================

# Fichiers déjà gérés par les ETL 1-4 — on ne les retraite pas
_FICHIERS_CATALOGUES = {
    "daily_food_nutrition_dataset.csv",
    "gym_members_exercise.csv",
    "exercises.json",
    "diet_recommendations.csv",
    "fitness_tracker.csv",
}

# Mots-clés pour détecter le type de données à partir des noms de colonnes
_SIGNES_ALIMENTS   = {"calorie", "protein", "fat", "carb", "fiber", "sugar",
                       "sodium", "nutrient", "food", "kcal", "vitamin",
                       "energy", "aliment", "nutrition"}
_SIGNES_EXERCICES  = {"exercise", "workout", "muscle", "equipment", "level",
                       "fitness", "gym", "sport", "movement", "difficulty"}
_SIGNES_UTILISATEURS = {"weight", "height", "bmi", "heart_rate", "age",
                         "gender", "sex", "member", "user", "poids", "taille"}


def _detecter_type_fichier(df: pd.DataFrame) -> str | None:
    """Retourne 'aliments', 'exercices', 'utilisateurs', ou None si non détecté."""
    cols = {c.lower().replace(" ", "_").replace("-", "_") for c in df.columns}
    score = {
        "aliments":     sum(1 for k in _SIGNES_ALIMENTS    if any(k in c for c in cols)),
        "exercices":    sum(1 for k in _SIGNES_EXERCICES   if any(k in c for c in cols)),
        "utilisateurs": sum(1 for k in _SIGNES_UTILISATEURS if any(k in c for c in cols)),
    }
    best, val = max(score.items(), key=lambda x: x[1])
    return best if val >= 2 else None


_RENAME_ALIMENTS = {
    "food_item": "nom", "food": "nom", "item": "nom", "name": "nom", "food_name": "nom",
    "calories": "calories_100g", "energy_kcal": "calories_100g", "energy": "calories_100g",
    "kcal": "calories_100g", "calories_kcal": "calories_100g",
    "protein": "proteines_g", "proteins": "proteines_g", "protein_g": "proteines_g",
    "carbohydrates": "glucides_g", "carbs": "glucides_g", "carb": "glucides_g",
    "carbohydrates_g": "glucides_g",
    "fat": "lipides_g", "fats": "lipides_g", "fat_g": "lipides_g", "total_fat": "lipides_g",
    "fiber": "fibres_g", "fibre": "fibres_g", "dietary_fiber": "fibres_g",
    "sugar": "sucres_g", "sugars": "sucres_g",
    "sodium": "sodium_mg",
    "category": "categorie", "food_category": "categorie", "type": "categorie",
}

_RENAME_EXERCICES = {
    "exercise_name": "nom", "exercise": "nom", "name": "nom", "workout": "nom",
    "type": "type", "exercise_type": "type", "category": "type",
    "level": "niveau", "difficulty": "niveau", "difficulty_level": "niveau",
    "equipment": "equipement", "equipment_needed": "equipement",
    "description": "description", "instructions": "instructions",
}


def etl_fichiers_additionnels(engine) -> dict:
    """Auto-détecte et importe les fichiers uploadés ou téléchargés depuis Kaggle."""
    rapport = {"dataset": "fichiers_additionnels", "fichiers": []}

    candidats = [
        f for f in DATA_DIR.iterdir()
        if f.is_file()
        and f.suffix.lower() in {".csv", ".json", ".xlsx"}
        and f.name not in _FICHIERS_CATALOGUES
    ]

    if not candidats:
        logger.info("[auto-detect] Aucun nouveau fichier à traiter.")
        rapport["statut"] = "succès"
        rapport["message"] = "Aucun nouveau fichier"
        return rapport

    logger.info(f"[auto-detect] {len(candidats)} fichier(s) candidat(s) détectés.")

    for filepath in candidats:
        try:
            df = charger_fichier(filepath.name)
            if df is None or df.empty:
                continue

            ftype = _detecter_type_fichier(df)
            if ftype is None:
                logger.warning(f"[auto-detect] {filepath.name} — type non identifié, ignoré.")
                continue

            logger.info(f"[auto-detect] {filepath.name} → {ftype}")

            # Normalise les noms de colonnes
            df.columns = [c.lower().strip().replace(" ", "_").replace("-", "_")
                          for c in df.columns]

            if ftype == "aliments":
                df = df.rename(columns={k: v for k, v in _RENAME_ALIMENTS.items() if k in df.columns})
                if "nom" not in df.columns:
                    logger.warning(f"[auto-detect] {filepath.name} — colonne 'nom' introuvable après renommage, ignoré.")
                    continue
                for col in ["calories_100g", "proteines_g", "glucides_g", "lipides_g", "fibres_g"]:
                    if col not in df.columns:
                        df[col] = 0.0
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).clip(lower=0)
                df = df.dropna(subset=["nom"]).drop_duplicates(subset=["nom"], keep="first")
                df["source_dataset"] = filepath.stem
                cols = ["nom", "categorie", "calories_100g", "proteines_g",
                        "glucides_g", "lipides_g", "fibres_g", "sodium_mg", "sucres_g", "source_dataset"]
                df = df[[c for c in cols if c in df.columns]]
                nb = inserer_en_base(df, "aliment", engine)
                rapport["fichiers"].append({"fichier": filepath.name, "type": "aliments", "inseres": nb})

            elif ftype == "exercices":
                df = df.rename(columns={k: v for k, v in _RENAME_EXERCICES.items() if k in df.columns})
                if "nom" not in df.columns:
                    logger.warning(f"[auto-detect] {filepath.name} — colonne 'nom' introuvable, ignoré.")
                    continue
                df = df.dropna(subset=["nom"]).drop_duplicates(subset=["nom"], keep="first")
                df["source_dataset"] = filepath.stem
                cols = ["nom", "type", "niveau", "equipement", "description", "instructions", "source_dataset"]
                df = df[[c for c in cols if c in df.columns]]
                nb = inserer_en_base(df, "exercice", engine)
                rapport["fichiers"].append({"fichier": filepath.name, "type": "exercices", "inseres": nb})

            elif ftype == "utilisateurs":
                logger.info(f"[auto-detect] {filepath.name} détecté comme données utilisateurs — importation métriques uniquement.")
                rapport["fichiers"].append({"fichier": filepath.name, "type": "utilisateurs", "inseres": 0,
                                            "note": "Import utilisateurs nécessite mapping manuel"})

        except Exception as e:
            logger.error(f"[auto-detect] Erreur sur {filepath.name} : {e}", exc_info=True)
            rapport["fichiers"].append({"fichier": filepath.name, "erreur": str(e)})

    rapport["statut"] = "succès"
    return rapport


def run_pipeline(declencheur: str = "manuel") -> dict:
    """
    Lance l'ensemble du pipeline ETL.
    Retourne un résumé du run (statut, durée, nb ETL ok/erreur).
    """
    global _run_en_cours

    run_id = str(uuid.uuid4())
    start = datetime.now()

    _run_en_cours = {
        "run_id":      run_id,
        "started_at":  start.isoformat(),
        "statut":      "en_cours",
        "declencheur": declencheur,
    }

    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║   HealthAI Coach — Pipeline ETL démarré  ║")
    logger.info(f"║   {start.strftime('%Y-%m-%d %H:%M:%S')} | {declencheur:<16}  ║")
    logger.info("╚══════════════════════════════════════════╝")

    engine = get_engine()
    init_log_table(engine)
    rapports = []

    etl_fonctions = [
        ("ETL 1 - Aliments",           etl_aliments),
        ("ETL 2 - Utilisateurs",       etl_utilisateurs_metriques),
        ("ETL 3 - Exercices",          etl_exercices),
        ("ETL 4 - Objectifs users",    etl_objectifs_utilisateurs),
        ("ETL 5 - Fichiers additionnels", etl_fichiers_additionnels),
    ]

    for nom, fn in etl_fonctions:
        try:
            rapport = fn(engine)
            rapport["statut"] = rapport.get("statut", "succès")
            rapports.append(rapport)
        except Exception as e:
            logger.error(f"ERREUR dans {nom} : {e}", exc_info=True)
            rapports.append({"dataset": nom, "statut": "erreur", "message": str(e)})

    duree = (datetime.now() - start).total_seconds()
    sauvegarder_rapport(rapports)
    sauvegarder_run_en_base(engine, run_id, start, rapports, duree, declencheur)

    nb_ok  = sum(1 for r in rapports if r.get("statut") == "succès")
    nb_err = len(rapports) - nb_ok

    _run_en_cours = None

    logger.info("╔══════════════════════════════════════════╗")
    logger.info(f"║   Pipeline terminé en {duree:.1f}s               ║")
    logger.info(f"║   {nb_ok} ETL réussis | {nb_err} erreurs               ║")
    logger.info("╚══════════════════════════════════════════╝")

    return {
        "run_id":          run_id,
        "statut":          "erreur" if nb_err > 0 else "succes",
        "nb_etl_succes":   nb_ok,
        "nb_etl_erreur":   nb_err,
        "duree_secondes":  round(duree, 2),
        "declencheur":     declencheur,
    }


def get_run_en_cours() -> dict | None:
    return _run_en_cours


if __name__ == "__main__":
    run_pipeline()
