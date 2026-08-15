-- =====================================================
-- Initialisation de la base de donnees recruitment
-- Cree automatiquement au demarrage de PostgreSQL
-- =====================================================

-- 1) Offres brutes recues depuis Kafka (avant nettoyage)
CREATE TABLE IF NOT EXISTS jobs_raw (
    job_id        BIGINT,
    title         TEXT,
    company_name  TEXT,
    location      TEXT,
    description   TEXT,
    med_salary    DOUBLE PRECISION,
    work_type     TEXT,
    experience    TEXT,
    received_at   TIMESTAMP DEFAULT NOW()
);

-- 2) Offres nettoyees + skills extraits (sortie du consumer Spark)
CREATE TABLE IF NOT EXISTS jobs_clean (
    job_id        BIGINT,
    title         TEXT,
    company_name  TEXT,
    location      TEXT,
    description   TEXT,
    med_salary    DOUBLE PRECISION,
    work_type     TEXT,
    experience    TEXT,
    skills        TEXT,          -- ex: "python,docker,aws"
    skill_count   INT
);

-- 3) Frequence des competences (pour Power BI)
CREATE TABLE IF NOT EXISTS skills_count (
    skill   TEXT,
    nb      INT
);

-- 4) Module A : classification metier (Random Forest)
CREATE TABLE IF NOT EXISTS job_predictions (
    job_id        BIGINT,
    title         TEXT,
    predicted_job TEXT,          -- Data Engineer / DevOps / Backend / Data Scientist / ML Engineer
    med_salary    DOUBLE PRECISION
);

-- 5) Module B : segmentation (K-Means)
CREATE TABLE IF NOT EXISTS clusters (
    job_id      BIGINT,
    title       TEXT,
    skill_count INT,
    med_salary  DOUBLE PRECISION,
    cluster_id  INT,
    cluster_label TEXT           -- Junior generaliste / Senior specialise / Cloud-DevOps / Data
);
