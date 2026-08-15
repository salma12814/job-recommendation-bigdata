"""
=====================================================
 DATA INGESTION  -  producer_jobs.py
=====================================================
Lit le fichier postings.csv (dataset LinkedIn Kaggle)
et envoie chaque offre ligne par ligne dans le topic
Kafka "jobs" -> simule l'arrivee continue (streaming).

Lancer (depuis Windows, hors Docker) :
    python producer_jobs.py
=====================================================
"""

import csv
import json
import time
import sys

from kafka import KafkaProducer

# ---- CONFIG ------------------------------------------------
CSV_PATH = r"C:\Users\pc\Desktop\jobs\postings.csv"   # <-- ton fichier
KAFKA_SERVER = "localhost:9092"
TOPIC = "jobs"
DELAY = 0.3          # secondes entre 2 messages (effet streaming)
MAX_MESSAGES = 5000  # limite pour les tests ; mets None pour tout envoyer
# ------------------------------------------------------------

# Colonnes utiles du dataset arshkon/linkedin-job-postings
# (le CSV contient bien plus de colonnes, on garde l'essentiel)
def build_message(row):
    return {
        "job_id":       row.get("job_id"),
        "title":        row.get("title"),
        "company_name": row.get("company_name"),
        "location":     row.get("location"),
        "description":  row.get("description"),
        "med_salary":   row.get("med_salary"),
        "work_type":    row.get("formatted_work_type"),
        "experience":   row.get("formatted_experience_level"),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"[producer_jobs] Lecture de {CSV_PATH}")
    print(f"[producer_jobs] Envoi vers topic '{TOPIC}' ...")

    sent = 0
    # encoding utf-8 + gestion des grosses descriptions
    csv.field_size_limit(10_000_000)
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            msg = build_message(row)
            # on ignore les lignes sans titre ou sans description
            if not msg["title"] or not msg["description"]:
                continue
            producer.send(TOPIC, msg)
            sent += 1
            if sent % 100 == 0:
                print(f"  -> {sent} offres envoyees")
            time.sleep(DELAY)
            if MAX_MESSAGES and sent >= MAX_MESSAGES:
                break

    producer.flush()
    print(f"[producer_jobs] TERMINE. {sent} offres envoyees au topic '{TOPIC}'.")


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError:
        print("ERREUR : fichier introuvable. Verifie CSV_PATH dans le script.")
        sys.exit(1)
