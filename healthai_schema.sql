-- =============================================================
--  HealthAI Coach — MPD PostgreSQL
--  Modèle Physique de Données
--  Version : 1.0.0
--  Auteur   : Équipe projet MSPR
-- =============================================================
-- Usage :
--   psql -U postgres -d healthai -f healthai_schema.sql
-- =============================================================

-- -------------------------------------------------------------
-- 0. Nettoyage (si re-déploiement)
-- -------------------------------------------------------------
DROP TABLE IF EXISTS metrique_quotidienne  CASCADE;
DROP TABLE IF EXISTS seance_exercice       CASCADE;
DROP TABLE IF EXISTS seance                CASCADE;
DROP TABLE IF EXISTS exercice_muscle       CASCADE;
DROP TABLE IF EXISTS groupe_musculaire     CASCADE;
DROP TABLE IF EXISTS exercice              CASCADE;
DROP TABLE IF EXISTS ligne_repas           CASCADE;
DROP TABLE IF EXISTS journal_repas         CASCADE;
DROP TABLE IF EXISTS aliment               CASCADE;
DROP TABLE IF EXISTS utilisateur_objectif  CASCADE;
DROP TABLE IF EXISTS objectif              CASCADE;
DROP TABLE IF EXISTS utilisateur           CASCADE;

DROP TYPE IF EXISTS type_abonnement CASCADE;
DROP TYPE IF EXISTS type_sexe        CASCADE;
DROP TYPE IF EXISTS type_repas       CASCADE;
DROP TYPE IF EXISTS role_muscle      CASCADE;
DROP TYPE IF EXISTS niveau_exercice  CASCADE;
DROP TYPE IF EXISTS type_exercice    CASCADE;

-- -------------------------------------------------------------
-- 1. Types ENUM
-- -------------------------------------------------------------
CREATE TYPE type_abonnement AS ENUM ('freemium', 'premium', 'premium_plus');
CREATE TYPE type_sexe        AS ENUM ('homme', 'femme', 'autre', 'non_renseigne');
CREATE TYPE type_repas       AS ENUM ('petit_dejeuner', 'dejeuner', 'diner', 'collation');
CREATE TYPE role_muscle      AS ENUM ('principal', 'secondaire');
CREATE TYPE niveau_exercice  AS ENUM ('debutant', 'intermediaire', 'avance');
CREATE TYPE type_exercice    AS ENUM (
    'cardio', 'musculation', 'stretching',
    'yoga', 'pilates', 'hiit', 'autre'
);

-- -------------------------------------------------------------
-- 2. Table OBJECTIF  (référentiel)
-- -------------------------------------------------------------
CREATE TABLE objectif (
    id          SERIAL       PRIMARY KEY,
    libelle     VARCHAR(100) NOT NULL UNIQUE,
    description TEXT
);

-- Données de référence
INSERT INTO objectif (libelle, description) VALUES
    ('perte_de_poids',      'Réduire la masse graisseuse et le poids corporel'),
    ('prise_de_masse',      'Augmenter la masse musculaire'),
    ('amelioration_sommeil','Améliorer la qualité et la durée du sommeil'),
    ('maintien_forme',      'Maintenir son état de forme général'),
    ('endurance',           'Améliorer les capacités cardiovasculaires'),
    ('flexibilite',         'Améliorer la souplesse et la mobilité');

-- -------------------------------------------------------------
-- 3. Table UTILISATEUR
-- -------------------------------------------------------------
CREATE TABLE utilisateur (
    id               SERIAL          PRIMARY KEY,
    nom              VARCHAR(100)    NOT NULL,
    prenom           VARCHAR(100)    NOT NULL,
    email            VARCHAR(255)    NOT NULL UNIQUE,
    mdp_hash         VARCHAR(255)    NOT NULL,
    date_naissance   DATE,
    sexe             type_sexe       NOT NULL DEFAULT 'non_renseigne',
    poids_initial_kg NUMERIC(5,2)    CHECK (poids_initial_kg > 0),
    taille_cm        SMALLINT        CHECK (taille_cm BETWEEN 50 AND 300),
    abonnement       type_abonnement NOT NULL DEFAULT 'freemium',
    date_inscription TIMESTAMP       NOT NULL DEFAULT NOW(),
    actif            BOOLEAN         NOT NULL DEFAULT TRUE,
    -- Champs calculés mis à jour par trigger
    imc              NUMERIC(5,2),
    age              SMALLINT,
    created_at       TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- Index sur email pour les authentifications
CREATE INDEX idx_utilisateur_email ON utilisateur(email);
CREATE INDEX idx_utilisateur_abonnement ON utilisateur(abonnement);

-- Trigger : mettre à jour updated_at automatiquement
CREATE OR REPLACE FUNCTION maj_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_utilisateur_updated
    BEFORE UPDATE ON utilisateur
    FOR EACH ROW EXECUTE FUNCTION maj_updated_at();

-- -------------------------------------------------------------
-- 4. Table UTILISATEUR_OBJECTIF  (association M-N)
-- -------------------------------------------------------------
CREATE TABLE utilisateur_objectif (
    utilisateur_id INT  NOT NULL REFERENCES utilisateur(id) ON DELETE CASCADE,
    objectif_id    INT  NOT NULL REFERENCES objectif(id)    ON DELETE CASCADE,
    date_debut     DATE NOT NULL DEFAULT CURRENT_DATE,
    actif          BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (utilisateur_id, objectif_id)
);

-- -------------------------------------------------------------
-- 5. Table ALIMENT  (catalogue nutritionnel)
-- -------------------------------------------------------------
CREATE TABLE aliment (
    id              SERIAL         PRIMARY KEY,
    nom             VARCHAR(255)   NOT NULL,
    categorie       VARCHAR(100),
    calories_100g   NUMERIC(7,2)   CHECK (calories_100g >= 0),
    proteines_g     NUMERIC(6,2)   CHECK (proteines_g >= 0),
    glucides_g      NUMERIC(6,2)   CHECK (glucides_g >= 0),
    lipides_g       NUMERIC(6,2)   CHECK (lipides_g >= 0),
    fibres_g        NUMERIC(6,2)   CHECK (fibres_g >= 0),
    sodium_mg       NUMERIC(7,2),
    sucres_g        NUMERIC(6,2),
    source_dataset  VARCHAR(100)   DEFAULT 'kaggle_daily_food_nutrition',
    created_at      TIMESTAMP      NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_aliment_nom       ON aliment(nom);
CREATE INDEX idx_aliment_categorie ON aliment(categorie);

-- -------------------------------------------------------------
-- 6. Table JOURNAL_REPAS
-- -------------------------------------------------------------
CREATE TABLE journal_repas (
    id             SERIAL      PRIMARY KEY,
    utilisateur_id INT         NOT NULL REFERENCES utilisateur(id) ON DELETE CASCADE,
    date_repas     DATE        NOT NULL DEFAULT CURRENT_DATE,
    type_repas     type_repas  NOT NULL,
    notes          TEXT,
    created_at     TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_journal_user_date ON journal_repas(utilisateur_id, date_repas);

-- -------------------------------------------------------------
-- 7. Table LIGNE_REPAS  (détail d'un repas)
-- -------------------------------------------------------------
CREATE TABLE ligne_repas (
    id          SERIAL         PRIMARY KEY,
    journal_id  INT            NOT NULL REFERENCES journal_repas(id) ON DELETE CASCADE,
    aliment_id  INT            NOT NULL REFERENCES aliment(id),
    quantite_g  NUMERIC(7,2)   NOT NULL CHECK (quantite_g > 0),
    -- Champs dénormalisés pour perf analytique
    calories_calculees NUMERIC(8,2) GENERATED ALWAYS AS (quantite_g / 100.0) STORED
);

CREATE INDEX idx_ligne_journal ON ligne_repas(journal_id);
CREATE INDEX idx_ligne_aliment ON ligne_repas(aliment_id);

-- -------------------------------------------------------------
-- 8. Table GROUPE_MUSCULAIRE  (référentiel)
-- -------------------------------------------------------------
CREATE TABLE groupe_musculaire (
    id  SERIAL       PRIMARY KEY,
    nom VARCHAR(100) NOT NULL UNIQUE
);

INSERT INTO groupe_musculaire (nom) VALUES
    ('pectoraux'), ('dorsaux'), ('épaules'), ('biceps'), ('triceps'),
    ('quadriceps'), ('ischio-jambiers'), ('mollets'), ('fessiers'),
    ('abdominaux'), ('lombaires'), ('avant-bras'), ('trapèzes');

-- -------------------------------------------------------------
-- 9. Table EXERCICE  (catalogue sportif)
-- -------------------------------------------------------------
CREATE TABLE exercice (
    id            SERIAL          PRIMARY KEY,
    nom           VARCHAR(255)    NOT NULL,
    type          type_exercice   NOT NULL DEFAULT 'musculation',
    niveau        niveau_exercice NOT NULL DEFAULT 'debutant',
    equipement    VARCHAR(100),
    description   TEXT,
    instructions  TEXT,
    image_url     VARCHAR(500),
    source_dataset VARCHAR(100)   DEFAULT 'exercisedb_api',
    created_at    TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_exercice_type   ON exercice(type);
CREATE INDEX idx_exercice_niveau ON exercice(niveau);

-- -------------------------------------------------------------
-- 10. Table EXERCICE_MUSCLE  (association M-N)
-- -------------------------------------------------------------
CREATE TABLE exercice_muscle (
    exercice_id INT         NOT NULL REFERENCES exercice(id) ON DELETE CASCADE,
    muscle_id   INT         NOT NULL REFERENCES groupe_musculaire(id),
    role        role_muscle NOT NULL DEFAULT 'principal',
    PRIMARY KEY (exercice_id, muscle_id)
);

-- -------------------------------------------------------------
-- 11. Table SEANCE
-- -------------------------------------------------------------
CREATE TABLE seance (
    id                 SERIAL     PRIMARY KEY,
    utilisateur_id     INT        NOT NULL REFERENCES utilisateur(id) ON DELETE CASCADE,
    date_seance        DATE       NOT NULL DEFAULT CURRENT_DATE,
    heure_debut        TIME,
    duree_min          SMALLINT   CHECK (duree_min > 0),
    calories_depensees NUMERIC(7,2),
    notes              TEXT,
    created_at         TIMESTAMP  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_seance_user_date ON seance(utilisateur_id, date_seance);

-- -------------------------------------------------------------
-- 12. Table SEANCE_EXERCICE  (détail d'une séance)
-- -------------------------------------------------------------
CREATE TABLE seance_exercice (
    id          SERIAL        PRIMARY KEY,
    seance_id   INT           NOT NULL REFERENCES seance(id) ON DELETE CASCADE,
    exercice_id INT           NOT NULL REFERENCES exercice(id),
    ordre       SMALLINT      NOT NULL DEFAULT 1,
    series      SMALLINT      CHECK (series > 0),
    repetitions SMALLINT      CHECK (repetitions > 0),
    poids_kg    NUMERIC(5,2)  CHECK (poids_kg >= 0),
    duree_sec   INT           CHECK (duree_sec > 0),
    repos_sec   SMALLINT,
    notes       TEXT
);

CREATE INDEX idx_seance_exercice_seance ON seance_exercice(seance_id);

-- -------------------------------------------------------------
-- 13. Table METRIQUE_QUOTIDIENNE
-- -------------------------------------------------------------
CREATE TABLE metrique_quotidienne (
    id              SERIAL        PRIMARY KEY,
    utilisateur_id  INT           NOT NULL REFERENCES utilisateur(id) ON DELETE CASCADE,
    date_mesure     DATE          NOT NULL DEFAULT CURRENT_DATE,
    poids_kg        NUMERIC(5,2)  CHECK (poids_kg > 0),
    bpm_repos       SMALLINT      CHECK (bpm_repos BETWEEN 30 AND 250),
    bpm_max         SMALLINT      CHECK (bpm_max BETWEEN 50 AND 300),
    heures_sommeil  NUMERIC(4,2)  CHECK (heures_sommeil BETWEEN 0 AND 24),
    steps           INT           CHECK (steps >= 0),
    calories_brulees NUMERIC(7,2) CHECK (calories_brulees >= 0),
    body_fat_pct    NUMERIC(4,2)  CHECK (body_fat_pct BETWEEN 0 AND 100),
    imc_calcule     NUMERIC(5,2),
    source          VARCHAR(100)  DEFAULT 'manuel',
    created_at      TIMESTAMP     NOT NULL DEFAULT NOW(),
    -- Contrainte d'unicité : une seule mesure par user par jour
    UNIQUE (utilisateur_id, date_mesure)
);

CREATE INDEX idx_metrique_user_date ON metrique_quotidienne(utilisateur_id, date_mesure);

-- -------------------------------------------------------------
-- 14. VUES ANALYTIQUES
-- -------------------------------------------------------------

-- Vue : apports nutritionnels journaliers par utilisateur
CREATE OR REPLACE VIEW vue_apports_journaliers AS
SELECT
    u.id                                    AS utilisateur_id,
    u.nom || ' ' || u.prenom               AS nom_complet,
    jr.date_repas,
    jr.type_repas,
    SUM(lr.quantite_g / 100.0 * a.calories_100g)  AS total_calories,
    SUM(lr.quantite_g / 100.0 * a.proteines_g)    AS total_proteines_g,
    SUM(lr.quantite_g / 100.0 * a.glucides_g)     AS total_glucides_g,
    SUM(lr.quantite_g / 100.0 * a.lipides_g)      AS total_lipides_g
FROM utilisateur u
JOIN journal_repas jr ON jr.utilisateur_id = u.id
JOIN ligne_repas lr   ON lr.journal_id = jr.id
JOIN aliment a        ON a.id = lr.aliment_id
GROUP BY u.id, u.nom, u.prenom, jr.date_repas, jr.type_repas;

-- Vue : progression poids par utilisateur
CREATE OR REPLACE VIEW vue_progression_poids AS
SELECT
    u.id                     AS utilisateur_id,
    u.nom || ' ' || u.prenom AS nom_complet,
    u.poids_initial_kg,
    mq.date_mesure,
    mq.poids_kg,
    mq.poids_kg - u.poids_initial_kg AS variation_kg
FROM utilisateur u
JOIN metrique_quotidienne mq ON mq.utilisateur_id = u.id
ORDER BY u.id, mq.date_mesure;

-- Vue : statistiques activité par utilisateur
CREATE OR REPLACE VIEW vue_stats_activite AS
SELECT
    u.id                          AS utilisateur_id,
    u.nom || ' ' || u.prenom      AS nom_complet,
    COUNT(DISTINCT s.id)          AS nb_seances,
    SUM(s.duree_min)              AS total_minutes,
    AVG(s.duree_min)              AS duree_moy_min,
    SUM(s.calories_depensees)     AS total_calories_depensees,
    COUNT(DISTINCT se.exercice_id) AS nb_exercices_differents
FROM utilisateur u
LEFT JOIN seance s           ON s.utilisateur_id = u.id
LEFT JOIN seance_exercice se ON se.seance_id = s.id
GROUP BY u.id, u.nom, u.prenom;

-- Vue : KPIs business (dashboard direction)
CREATE OR REPLACE VIEW vue_kpis_business AS
SELECT
    COUNT(*)                                                   AS total_utilisateurs,
    COUNT(*) FILTER (WHERE abonnement = 'freemium')            AS nb_freemium,
    COUNT(*) FILTER (WHERE abonnement = 'premium')             AS nb_premium,
    COUNT(*) FILTER (WHERE abonnement = 'premium_plus')        AS nb_premium_plus,
    ROUND(
        COUNT(*) FILTER (WHERE abonnement IN ('premium','premium_plus'))::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                          AS taux_conversion_pct,
    AVG(EXTRACT(YEAR FROM AGE(date_naissance)))                AS age_moyen,
    COUNT(*) FILTER (WHERE actif = TRUE)                       AS utilisateurs_actifs
FROM utilisateur;

-- -------------------------------------------------------------
-- 15. Commentaires de documentation
-- -------------------------------------------------------------
COMMENT ON TABLE utilisateur           IS 'Profils des utilisateurs de la plateforme';
COMMENT ON TABLE objectif              IS 'Référentiel des objectifs santé disponibles';
COMMENT ON TABLE aliment               IS 'Catalogue nutritionnel (source : Kaggle Daily Food & Nutrition)';
COMMENT ON TABLE journal_repas         IS 'Journal alimentaire quotidien par utilisateur';
COMMENT ON TABLE ligne_repas           IS 'Détail des aliments consommés par repas';
COMMENT ON TABLE exercice              IS 'Catalogue des exercices sportifs (source : ExerciseDB API)';
COMMENT ON TABLE groupe_musculaire     IS 'Référentiel des groupes musculaires';
COMMENT ON TABLE seance                IS 'Séances d''entraînement réalisées';
COMMENT ON TABLE seance_exercice       IS 'Exercices effectués lors d''une séance';
COMMENT ON TABLE metrique_quotidienne  IS 'Métriques biométriques quotidiennes (source : Kaggle Gym Members & Fitness Tracker)';

-- -------------------------------------------------------------
-- 16. Table ETL_RUN_LOG  (historique des exécutions ETL)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_run_log (
    id              SERIAL      PRIMARY KEY,
    run_id          UUID        NOT NULL DEFAULT gen_random_uuid(),
    started_at      TIMESTAMP   NOT NULL,
    finished_at     TIMESTAMP,
    statut          VARCHAR(20) NOT NULL DEFAULT 'en_cours',  -- en_cours | succes | erreur
    nb_etl_total    SMALLINT,
    nb_etl_succes   SMALLINT,
    nb_etl_erreur   SMALLINT,
    duree_secondes  NUMERIC(8,2),
    rapport_json    JSONB,
    declencheur     VARCHAR(50) NOT NULL DEFAULT 'manuel'     -- manuel | scheduler
);

CREATE INDEX IF NOT EXISTS idx_etl_run_started ON etl_run_log(started_at DESC);

-- =============================================================
-- FIN DU SCRIPT
-- Déploiement : psql -U postgres -d healthai -f healthai_schema.sql
-- =============================================================
