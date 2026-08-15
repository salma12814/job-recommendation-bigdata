"""
=====================================================
 DAG Airflow - Pipeline complet de recrutement Big Data
=====================================================
Orchestre automatiquement, dans l'ordre :
   produce_jobs -> process_jobs -> ml_module_a -> ml_module_b

Chaque tache tourne DANS le conteneur airflow (PySpark local + kafka-python).
=====================================================
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "souka",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="recrutement_pipeline",
    description="Ingestion Kafka -> Spark -> PostgreSQL -> ML (RandomForest + KMeans)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,        # lancement manuel (ou "@daily" pour automatique)
    catchup=False,
    tags=["bigdata", "recrutement", "ml"],
) as dag:

    # 1) DATA INGESTION : envoyer les offres dans Kafka
    produce_jobs = BashOperator(
        task_id="produce_jobs",
        bash_command="python /opt/airflow/jobs/producer_jobs.py",
    )

    # 2) PREPROCESSING + STORAGE : Spark lit Kafka, nettoie, ecrit PostgreSQL
    process_jobs = BashOperator(
        task_id="process_jobs",
        bash_command="python /opt/airflow/jobs/consumer_batch.py",
    )

    # 3) ML MODULE A : classification metier (Random Forest)
    ml_module_a = BashOperator(
        task_id="ml_module_a",
        bash_command="python /opt/airflow/jobs/ml_module_a.py",
    )

    # 4) ML MODULE B : segmentation (K-Means)
    ml_module_b = BashOperator(
        task_id="ml_module_b",
        bash_command="python /opt/airflow/jobs/ml_module_b.py",
    )

    # Ordre d'execution
    produce_jobs >> process_jobs >> ml_module_a >> ml_module_b
