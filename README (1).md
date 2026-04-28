
POST    http://localhost:8002/etl/run


# HealthAI Coach — Backend Métier

Pipeline ETL + Base de données relationnelle pour la plateforme HealthAI Coach.

---

## Démarrage rapide (< 5 minutes)

```bash
# 1. Cloner / déposer les fichiers dans un dossier
mkdir healthai && cd healthai

# 2. Créer le dossier data et y déposer vos CSV Kaggle
mkdir data logs
# → Copiez vos fichiers dans ./data/
#   - daily_food_nutrition.csv
#   - gym_members_exercise.csv
#   - diet_recommendations.csv
#   - exercises.json

# 3. Lancer l'environnement complet
docker compose up -d

# 4. Vérifier les logs ETL
docker logs healthai_etl --follow
```

---

## Accès aux interfaces

| Service   | URL                          | Identifiants           |
|-----------|------------------------------|------------------------|
| pgAdmin   | http://localhost:5050         | admin@healthai.local / admin123 |
| PostgreSQL| localhost:5432               | postgres / postgres    |

---

## Structure des fichiers

```
healthai/
├── docker-compose.yml     ← Orchestration Docker
├── healthai_schema.sql    ← MPD PostgreSQL (schéma complet)
├── etl_pipeline.py        ← Pipeline ETL Python
├── data/                  ← Datasets Kaggle (à télécharger)
│   ├── daily_food_nutrition.csv
│   ├── gym_members_exercise.csv
│   ├── diet_recommendations.csv
│   └── exercises.json
└── logs/                  ← Logs d'exécution ETL + rapports qualité
```

---

## Sans Docker (Python direct)

```bash
pip install pandas sqlalchemy psycopg2-binary openpyxl

# Configurer les variables d'environnement
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=healthai
export DB_USER=postgres
export DB_PASSWORD=postgres
export DATA_DIR=./data

# Créer la BDD d'abord
psql -U postgres -c "CREATE DATABASE healthai;"
psql -U postgres -d healthai -f healthai_schema.sql

# Lancer le pipeline
python etl_pipeline.py
```

---

## Sources de données

| Dataset | Source | Table cible |
|---------|--------|-------------|
| Daily Food & Nutrition | [Kaggle](https://www.kaggle.com/datasets/adilshamim8/daily-food-and-nutrition-dataset) | `aliment` |
| Gym Members Exercise | [Kaggle](https://www.kaggle.com/datasets/valakhorasani/gym-members-exercise-dataset) | `utilisateur`, `metrique_quotidienne` |
| Diet Recommendations | [Kaggle](https://www.kaggle.com/datasets/ziya07/diet-recommendations-dataset) | `utilisateur_objectif` |
| ExerciseDB API | [GitHub](https://github.com/ExerciseDB/exercisedb-api) | `exercice`, `exercice_muscle` |

> **Note** : Si les fichiers sont absents, le pipeline génère automatiquement des données simulées pour permettre les tests.

---

## Tables créées

- `utilisateur` — profils utilisateurs
- `objectif` — référentiel des objectifs santé
- `utilisateur_objectif` — association utilisateur ↔ objectif
- `aliment` — catalogue nutritionnel
- `journal_repas` — journal alimentaire quotidien
- `ligne_repas` — détail des repas
- `exercice` — catalogue sportif
- `groupe_musculaire` — référentiel musculaire
- `exercice_muscle` — association exercice ↔ muscle
- `seance` — séances d'entraînement
- `seance_exercice` — exercices par séance
- `metrique_quotidienne` — données biométriques quotidiennes

---

## Vues analytiques disponibles

```sql
SELECT * FROM vue_apports_journaliers;   -- Calories/macros par utilisateur par jour
SELECT * FROM vue_progression_poids;     -- Évolution du poids dans le temps
SELECT * FROM vue_stats_activite;        -- Statistiques d'entraînement
SELECT * FROM vue_kpis_business;         -- KPIs direction (conversion, âge moyen…)
```
