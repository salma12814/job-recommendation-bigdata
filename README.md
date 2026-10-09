# Analyse des offres d'emploi IT et recommandation de candidats

(sans Ray, Databricks, Snowflake, Delta Lake, Pinecone, FAISS, Redis, NiFi).

## Problematique
Peut-on recommander automatiquement les meilleures offres a un candidat a partir
de son CV et des competences demandees sur le marche IT ?

---

## Architecture

```
postings.csv (LinkedIn Kaggle)
        |
     Kafka            <- Data Ingestion
        |
  Spark Streaming     <- Data Preprocessing (nettoyage + extraction skills)
        |
   PostgreSQL         <- Data Storage
        |
   Spark MLlib        <- Data Analysis + Machine Learning
   |  Module A : Random Forest  -> classification metier
   |  Module B : K-Means        -> segmentation
   |  Module C : TF-IDF + Cosine-> recommandation
        |
     Flask            <- Interface (upload CV -> top 5 offres)
        |
   Power BI           <- Data Visualization
```

## Correspondance avec les 6 etapes demandees

| Etape | Fichier(s) |
|-------|-----------|
| 1. Data Ingestion | `producers/producer_jobs.py` (Kafka) |
| 2. Data Preprocessing | `spark/consumer_jobs.py` |
| 3. Data Storage | `sql/init.sql` + PostgreSQL |
| 4. Data Processing | `spark/consumer_jobs.py` + modules ML |
| 5. Data Analysis + ML | `spark/ml_module_a.py`, `ml_module_b.py`, `ml_module_c.py` |
| 6. Data Visualization | `flask_app/` + Power BI |

---

## PRE-REQUIS (Windows)

1. **Docker Desktop** installe et lance
2. **Python 3.10+** installe
3. Ton dataset : `C:\Users\Sabba\Downloads\jobs\postings.csv`

---

## ETAPE 1 — Lancer l'infrastructure (Kafka + Spark + PostgreSQL)

Ouvre un terminal (PowerShell) dans le dossier du projet :

```powershell
cd job-recommendation-bigdata
docker-compose up -d
```

Verifie que tout tourne :
```powershell
docker ps
```
Tu dois voir : `kafka`, `zookeeper`, `spark-master`, `spark-worker`, `postgres`, `pgadmin`.

Interfaces web disponibles :
- Spark UI    : http://localhost:8080
- pgAdmin     : http://localhost:5050  (login: admin@admin.com / admin)

---

## ETAPE 2 — Envoyer les offres dans Kafka (Data Ingestion)

Dans un **premier** terminal :

```powershell
cd producers
pip install -r requirements.txt
python producer_jobs.py
```

Le script lit `postings.csv` et envoie les offres dans le topic Kafka `jobs`.
(Par defaut 5000 offres ; modifie `MAX_MESSAGES` dans le script pour tout envoyer.)

---

## ETAPE 3 — Lancer le consumer Spark (Preprocessing + Storage)

Dans un **deuxieme** terminal, lance le streaming Spark qui nettoie
les donnees et les ecrit dans PostgreSQL :

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3 ^
  /app/consumer_jobs.py
```

> Astuce : laisse ce terminal ouvert, il traite les messages en continu.
> Quand le producer a fini, fais Ctrl+C pour arreter le consumer.

Verifie dans pgAdmin que la table `jobs_clean` se remplit.

---

## ETAPE 4 — Machine Learning

### Module A — Classification des metiers (Random Forest)
```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit ^
  --packages org.postgresql:postgresql:42.7.3 ^
  /app/ml_module_a.py
```
Resultat : table `job_predictions` (metier predit par offre) + accuracy affichee.

### Module B — Segmentation (K-Means)
```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit ^
  --packages org.postgresql:postgresql:42.7.3 ^
  /app/ml_module_b.py
```
Resultat : table `clusters` (groupe de chaque offre).

### Module C — Recommandation
Le Module C est utilise directement par l'app Flask (etape 5).
Tu peux le tester seul :
```powershell
cd flask_app
pip install -r requirements.txt
python ..\spark\ml_module_c.py
```

---

## ETAPE 5 — Interface Flask (Upload CV)

```powershell
cd flask_app
pip install -r requirements.txt
python app.py
```
Ouvre http://localhost:5000

- **Page "Upload CV"** : depose un CV PDF -> top 5 offres recommandees
- **Page "Recherche par offre"** : colle une offre -> top 5 offres proches

---

## ETAPE 6 — Power BI (Visualization)

1. Ouvre Power BI Desktop
2. Obtenir les donnees -> Base de donnees PostgreSQL
3. Serveur : `localhost` | Base : `recruitment`
4. User : `admin` | Password : `admin`
5. Importe les tables : `skills_count`, `job_predictions`, `clusters`, `jobs_clean`

Dashboards suggeres :
- Top 20 competences les plus demandees (`skills_count`)
- Nombre d'offres par metier predit (`job_predictions`)
- Repartition des clusters (`clusters`)
- Salaire median par metier

---

## Arreter le projet
```powershell
docker-compose down
```
(Ajoute `-v` pour supprimer aussi les donnees PostgreSQL.)

---

## Structure du projet
```
job-recommendation-bigdata/
├── docker-compose.yml
├── producers/
│   ├── producer_jobs.py
│   └── requirements.txt
├── spark/
│   ├── consumer_jobs.py      # Preprocessing + storage
│   ├── ml_module_a.py        # Classification (Random Forest)
│   ├── ml_module_b.py        # Segmentation (K-Means)
│   ├── ml_module_c.py        # Recommandation (TF-IDF + Cosine)
│   └── requirements.txt
├── flask_app/
│   ├── app.py
│   ├── templates/
│   │   ├── index.html
│   │   └── offre.html
│   └── requirements.txt
├── sql/
│   └── init.sql
└── README.md
```

## Notes
- Le dataset n'a pas de CVs separes : le Module C compare le texte du CV uploade
  avec les descriptions d'offres (suffisant pour un projet etudiant).
- Le label metier (Module A) est genere automatiquement depuis le titre des offres
  (faible supervision) — approche realiste pour l'entrainement.
