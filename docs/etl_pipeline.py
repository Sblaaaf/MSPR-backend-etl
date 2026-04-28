"""
=============================================================
 HealthAI Coach — Pipeline ETL
 Ingestion, nettoyage et chargement des datasets Kaggle
 Version : 1.0.0
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

import os
import sys
import logging
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# =============================================================
# CONFIGURATION
# =============================================================

# Modifiez ces valeurs ou utilisez des variables d'environnement
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     os.getenv("DB_PORT",     "5432"),
    "dbname":   os.getenv("DB_NAME",     "healthai"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

# Dossier contenant les fichiers CSV/JSON téléchargés depuis Kaggle
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))

# Logs
LOG_DIR = Path("./logs")
LOG_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

# =============================================================
# LOGGER
# =============================================================

def setup_logger() -> logging.Logger:
    """Configure le logger avec sortie fichier + console."""
    log_file = LOG_DIR / f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = logging.getLogger("healthai_etl")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Handler fichier
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # Handler console
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
    """Crée et retourne le moteur SQLAlchemy."""
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    try:
        engine = create_engine(url, echo=False, pool_pre_ping=True)
        # Test de connexion
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Connexion PostgreSQL établie avec succès.")
        return engine
    except SQLAlchemyError as e:
        logger.error(f"Impossible de se connecter à la base : {e}")
        sys.exit(1)


# =============================================================
# UTILITAIRES GÉNÉRIQUES
# =============================================================

def rapport_qualite(df: pd.DataFrame, nom_dataset: str) -> dict:
    """Génère un rapport de qualité pour un DataFrame."""
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
    """Sauvegarde tous les rapports de qualité en JSON."""
    path = LOG_DIR / f"rapport_qualite_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rapports, f, ensure_ascii=False, indent=2)
    logger.info(f"Rapport de qualité sauvegardé : {path}")


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
            df = pd.read_excel(chemin, **kwargs)
        else:
            logger.error(f"Format non supporté : {ext}")
            return None
        logger.info(f"Fichier chargé : {nom_fichier} ({len(df)} lignes)")
        return df
    except Exception as e:
        logger.error(f"Erreur lors du chargement de {nom_fichier} : {e}")
        return None


def inserer_en_base(df: pd.DataFrame, table: str, engine, mode: str = "append"):
    """Insère un DataFrame dans une table PostgreSQL."""
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
    """
    Source  : daily-food-and-nutrition-dataset.csv (Kaggle)
    Cible   : table aliment
    """
    logger.info("=" * 50)
    logger.info("ETL 1 : Aliments — début")

    df = charger_fichier("daily_food_nutrition.csv", encoding="utf-8")

    # -- Fallback : génération de données simulées si fichier absent --
    if df is None:
        logger.warning("Fichier absent — génération de données simulées pour démo.")
        df = _simuler_aliments()

    rapport = rapport_qualite(df, "aliments_brut")

    # --- NETTOYAGE ---

    # 1. Renommage des colonnes (adapter selon les noms réels du CSV Kaggle)
    rename_map = {
        "Food":         "nom",
        "food":         "nom",
        "food_item":    "nom",
        "Calories":     "calories_100g",
        "calories":     "calories_100g",
        "Protein":      "proteines_g",
        "protein":      "proteines_g",
        "Carbohydrates":"glucides_g",
        "carbs":        "glucides_g",
        "Fat":          "lipides_g",
        "fat":          "lipides_g",
        "Fiber":        "fibres_g",
        "fiber":        "fibres_g",
        "Category":     "categorie",
        "category":     "categorie",
        "Sodium":       "sodium_mg",
        "Sugar":        "sucres_g",
        "sugar":        "sucres_g",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # 2. Colonnes obligatoires manquantes → valeur par défaut
    for col in ["calories_100g", "proteines_g", "glucides_g", "lipides_g", "fibres_g"]:
        if col not in df.columns:
            df[col] = 0.0

    # 3. Suppression des lignes sans nom d'aliment
    df = df.dropna(subset=["nom"])
    df["nom"] = df["nom"].str.strip().str.title()

    # 4. Valeurs numériques : remplacement des nulls et négatifs
    cols_num = ["calories_100g", "proteines_g", "glucides_g", "lipides_g", "fibres_g"]
    for col in cols_num:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].fillna(0.0).clip(lower=0)

    # 5. Doublons sur le nom
    avant = len(df)
    df = df.drop_duplicates(subset=["nom"], keep="first")
    logger.info(f"Doublons supprimés : {avant - len(df)}")

    # 6. Colonne source
    df["source_dataset"] = "kaggle_daily_food_nutrition"

    # 7. Sélection des colonnes finales
    cols_finales = ["nom", "categorie", "calories_100g", "proteines_g",
                    "glucides_g", "lipides_g", "fibres_g", "sodium_mg",
                    "sucres_g", "source_dataset"]
    df = df[[c for c in cols_finales if c in df.columns]]

    rapport["apres_nettoyage"] = len(df)
    nb = inserer_en_base(df, "aliment", engine)
    logger.info(f"ETL 1 terminé — {nb} aliments insérés.")
    return rapport


def _simuler_aliments() -> pd.DataFrame:
    """Génère un mini-dataset d'aliments simulés pour les tests."""
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
    """
    Source  : gym_members_exercise_tracking.csv (Kaggle)
    Cible   : tables utilisateur + metrique_quotidienne
    """
    logger.info("=" * 50)
    logger.info("ETL 2 : Utilisateurs & Métriques — début")

    df = charger_fichier("gym_members_exercise.csv", encoding="utf-8")

    if df is None:
        logger.warning("Fichier absent — génération de données simulées.")
        df = _simuler_gym_members()

    rapport = rapport_qualite(df, "gym_members_brut")

    # --- NETTOYAGE ---

    # Renommage (s'adapter aux colonnes réelles du CSV Kaggle)
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

    # Hauteur : convertir mètres → cm si nécessaire
    if "taille_m" in df.columns:
        df["taille_cm"] = (df["taille_m"] * 100).round(0).astype("Int64")
    else:
        df["taille_cm"] = None

    # Sexe : normalisation
    if "sexe" in df.columns:
        df["sexe"] = df["sexe"].str.lower().map({
            "male": "homme", "female": "femme",
            "homme": "homme", "femme": "femme",
            "m": "homme", "f": "femme",
        }).fillna("non_renseigne")

    # Valeurs numériques
    for col in ["poids_initial_kg", "bpm_max", "bpm_repos", "calories_brulees", "imc", "body_fat_pct"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].clip(lower=0)

    # Filtre outliers biométriques
    if "poids_initial_kg" in df.columns:
        df = df[df["poids_initial_kg"].between(30, 300) | df["poids_initial_kg"].isna()]
    if "bpm_max" in df.columns:
        df = df[df["bpm_max"].between(50, 300) | df["bpm_max"].isna()]

    df = df.dropna(subset=["poids_initial_kg"])
    df = df.reset_index(drop=True)

    # --- Création des utilisateurs fictifs ---
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

    # Supprimer les doublons email (si re-run)
    with engine.connect() as conn:
        try:
            emails_existants = pd.read_sql("SELECT email FROM utilisateur", conn)
            df_users = df_users[~df_users["email"].isin(emails_existants["email"])]
        except Exception:
            pass  # Table vide ou inexistante

    nb_users = inserer_en_base(df_users, "utilisateur", engine)

    # --- Récupération des IDs insérés ---
    with engine.connect() as conn:
        df_ids = pd.read_sql(
            "SELECT id, email FROM utilisateur WHERE email LIKE 'user_%@healthai.demo'",
            conn
        )

    # --- Création des métriques quotidiennes ---
    metriques = []
    for _, row_id in df_ids.iterrows():
        idx = int(row_id["email"].split("_")[1].split("@")[0]) - 1
        if idx >= len(df):
            continue
        row = df.iloc[idx]
        # On simule 30 jours de métriques par utilisateur
        for j in range(30):
            date_j = datetime.now().date() - timedelta(days=29 - j)
            poids_j = row.get("poids_initial_kg", 70)
            if pd.notna(poids_j):
                # Légère variation quotidienne simulée
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
    logger.info(f"ETL 2 terminé — {nb_users} users + {nb_metriques} métriques.")
    return rapport


def _simuler_gym_members() -> pd.DataFrame:
    """Génère des profils utilisateurs simulés."""
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
    """
    Source  : exercises.json (fork ExerciseDB sur GitHub)
    Cible   : tables exercice + exercice_muscle
    """
    logger.info("=" * 50)
    logger.info("ETL 3 : Exercices — début")

    df = charger_fichier("exercises.json")

    if df is None:
        logger.warning("Fichier exercises.json absent — génération de données simulées.")
        df = _simuler_exercices()

    rapport = rapport_qualite(df, "exercices_brut")

    # Renommage (s'adapter au format ExerciseDB)
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

    # Nettoyage
    df = df.dropna(subset=["nom"])
    df["nom"] = df["nom"].str.strip().str.title()

    # Normalisation niveau
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

    # Type d'exercice
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

    # --- Association exercice ↔ groupe musculaire ---
    if "muscle_principal" in df.columns:
        with engine.connect() as conn:
            df_ex_ids = pd.read_sql("SELECT id, nom FROM exercice", conn)
            df_mu_ids = pd.read_sql("SELECT id, nom FROM groupe_musculaire", conn)

        df_ex_ids["nom_lower"] = df_ex_ids["nom"].str.lower()
        assoc_rows = []

        for _, row in df.iterrows():
            nom_ex = str(row.get("nom", "")).title()
            match_ex = df_ex_ids[df_ex_ids["nom"] == nom_ex]
            if match_ex.empty:
                continue
            ex_id = int(match_ex.iloc[0]["id"])

            # Muscle principal
            muscle_nom = str(row.get("muscle_principal", "")).lower()
            match_mu = df_mu_ids[df_mu_ids["nom"].str.lower() == muscle_nom]
            if not match_mu.empty:
                assoc_rows.append({
                    "exercice_id": ex_id,
                    "muscle_id":   int(match_mu.iloc[0]["id"]),
                    "role":        "principal"
                })

        if assoc_rows:
            df_assoc = pd.DataFrame(assoc_rows).drop_duplicates()
            inserer_en_base(df_assoc, "exercice_muscle", engine)

    rapport["exercices_inseres"] = nb
    logger.info(f"ETL 3 terminé — {nb} exercices insérés.")
    return rapport


def _simuler_exercices() -> pd.DataFrame:
    data = [
        {"nom": "Bench Press",        "type": "musculation", "niveau": "intermediaire", "equipement": "barbell",    "muscle_principal": "pectoraux",       "instructions": "Allongé sur le banc, poussez la barre vers le haut."},
        {"nom": "Squat",              "type": "musculation", "niveau": "debutant",      "equipement": "barbell",    "muscle_principal": "quadriceps",       "instructions": "Descendez jusqu'à ce que les cuisses soient parallèles au sol."},
        {"nom": "Deadlift",           "type": "musculation", "niveau": "avance",        "equipement": "barbell",    "muscle_principal": "dorsaux",          "instructions": "Soulevez la barre depuis le sol en gardant le dos droit."},
        {"nom": "Pull Up",            "type": "musculation", "niveau": "intermediaire", "equipement": "barres",     "muscle_principal": "dorsaux",          "instructions": "Accrochez-vous à la barre et tirez-vous vers le haut."},
        {"nom": "Planche",            "type": "musculation", "niveau": "debutant",      "equipement": "aucun",      "muscle_principal": "abdominaux",       "instructions": "Maintenez la position gainage sur les coudes 30-60 secondes."},
        {"nom": "Running",            "type": "cardio",      "niveau": "debutant",      "equipement": "aucun",      "muscle_principal": "quadriceps",       "instructions": "Courez à un rythme confortable, dos droit."},
        {"nom": "Burpees",            "type": "hiit",        "niveau": "intermediaire", "equipement": "aucun",      "muscle_principal": "fessiers",         "instructions": "Enchaînez squat, planche, pompe, saut."},
        {"nom": "Bicep Curl",         "type": "musculation", "niveau": "debutant",      "equipement": "haltères",   "muscle_principal": "biceps",           "instructions": "Fléchissez les coudes pour ramener les haltères aux épaules."},
        {"nom": "Tricep Dips",        "type": "musculation", "niveau": "debutant",      "equipement": "banc",       "muscle_principal": "triceps",          "instructions": "Descendez en fléchissant les coudes derrière vous."},
        {"nom": "Calf Raise",         "type": "musculation", "niveau": "debutant",      "equipement": "aucun",      "muscle_principal": "mollets",          "instructions": "Montez sur la pointe des pieds, tenez 1 seconde, redescendez."},
    ]
    return pd.DataFrame(data)


# =============================================================
# ETL 4 — OBJECTIFS UTILISATEURS (Diet Recommendations Dataset)
# =============================================================

def etl_objectifs_utilisateurs(engine) -> dict:
    """
    Source  : diet_recommendations.csv (Kaggle)
    Cible   : table utilisateur_objectif
    """
    logger.info("=" * 50)
    logger.info("ETL 4 : Association utilisateurs ↔ objectifs — début")

    df = charger_fichier("diet_recommendations.csv", encoding="utf-8")

    if df is None:
        logger.warning("Fichier absent — simulation des associations objectifs.")
        df = _simuler_objectifs()

    rapport = rapport_qualite(df, "diet_recommendations_brut")

    # Mapping objectif
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
        df_users = pd.read_sql(
            "SELECT id FROM utilisateur WHERE email LIKE 'user_%@healthai.demo' ORDER BY id",
            conn
        )
        df_obj = pd.read_sql("SELECT id, libelle FROM objectif", conn)

    if df_users.empty:
        logger.warning("Aucun utilisateur trouvé. ETL 4 ignoré.")
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

    if assoc_rows:
        df_assoc = pd.DataFrame(assoc_rows).drop_duplicates(subset=["utilisateur_id", "objectif_id"])
        nb = inserer_en_base(df_assoc, "utilisateur_objectif", engine)
    else:
        nb = 0

    rapport["associations_inserees"] = nb
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

def run_pipeline():
    """Lance l'ensemble du pipeline ETL et sauvegarde les rapports."""
    start = datetime.now()
    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║   HealthAI Coach — Pipeline ETL démarré  ║")
    logger.info(f"║   {start.strftime('%Y-%m-%d %H:%M:%S')}                      ║")
    logger.info("╚══════════════════════════════════════════╝")

    engine = get_engine()
    rapports = []

    # -- Exécution de chaque ETL --
    etl_fonctions = [
        ("ETL 1 - Aliments",          etl_aliments),
        ("ETL 2 - Utilisateurs",       etl_utilisateurs_metriques),
        ("ETL 3 - Exercices",          etl_exercices),
        ("ETL 4 - Objectifs users",    etl_objectifs_utilisateurs),
    ]

    for nom, fn in etl_fonctions:
        try:
            rapport = fn(engine)
            rapport["statut"] = "succès"
            rapports.append(rapport)
        except Exception as e:
            logger.error(f"ERREUR dans {nom} : {e}", exc_info=True)
            rapports.append({"dataset": nom, "statut": "erreur", "message": str(e)})

    # -- Rapport final --
    duree = (datetime.now() - start).total_seconds()
    sauvegarder_rapport(rapports)

    logger.info("╔══════════════════════════════════════════╗")
    logger.info(f"║   Pipeline terminé en {duree:.1f}s               ║")
    nb_ok  = sum(1 for r in rapports if r.get("statut") == "succès")
    nb_err = len(rapports) - nb_ok
    logger.info(f"║   {nb_ok} ETL réussis | {nb_err} erreurs               ║")
    logger.info("╚══════════════════════════════════════════╝")


if __name__ == "__main__":
    run_pipeline()
