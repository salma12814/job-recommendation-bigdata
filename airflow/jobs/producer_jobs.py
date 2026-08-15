"""
PRODUCER (version Airflow) - tourne DANS le conteneur airflow.
Lit /data/postings.csv (dataset monte) et envoie dans Kafka.
"""
import csv
import json
import time

from kafka import KafkaProducer

CSV_PATH = "/data/postings.csv"      # dataset monte dans le conteneur airflow
KAFKA_SERVER = "kafka:29092"         # nom interne du conteneur Kafka
TOPIC = "jobs"
DELAY = 0.0                          # pas de delai : on veut que ca finisse vite
MAX_MESSAGES = 5000


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
    print(f"[producer] Lecture {CSV_PATH} -> topic '{TOPIC}'")
    sent = 0
    csv.field_size_limit(10_000_000)
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            msg = build_message(row)
            if not msg["title"] or not msg["description"]:
                continue
            producer.send(TOPIC, msg)
            sent += 1
            if sent % 500 == 0:
                print(f"  -> {sent} offres envoyees")
            if DELAY:
                time.sleep(DELAY)
            if MAX_MESSAGES and sent >= MAX_MESSAGES:
                break
    producer.flush()
    print(f"[producer] TERMINE. {sent} offres envoyees.")


if __name__ == "__main__":
    main()
