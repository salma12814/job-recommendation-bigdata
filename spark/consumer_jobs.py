"""
=====================================================
 DATA PREPROCESSING  -  consumer_jobs.py
=====================================================
Spark Structured Streaming :
  1. Lit le topic Kafka "jobs"
  2. Nettoie les donnees (nulls, normalisation)
  3. Extrait les competences IT (regex sur mots-cles)
  4. Ecrit dans PostgreSQL : jobs_clean + skills_count

Lancer DANS le conteneur spark-master :
  docker exec -it spark-master spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3 \
    /app/consumer_jobs.py
=====================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lower, regexp_replace, split, size,
    array_join, explode, count
)
from pyspark.sql.types import StructType, StructField, StringType

# ---- CONFIG ------------------------------------------------
KAFKA_SERVER = "kafka:29092"        # nom interne du conteneur Kafka
TOPIC = "jobs"
PG_URL = "jdbc:postgresql://postgres:5432/recruitment"
PG_PROPS = {"user": "admin", "password": "admin", "driver": "org.postgresql.Driver"}

# Liste des competences IT a detecter dans les descriptions
SKILLS = [
    "python", "java", "javascript", "scala", "c++", "c#", "go", "rust",
    "sql", "nosql", "mongodb", "postgresql", "mysql", "redis",
    "spark", "hadoop", "kafka", "airflow", "hive",
    "docker", "kubernetes", "jenkins", "terraform", "ansible",
    "aws", "azure", "gcp", "cloud",
    "tensorflow", "pytorch", "scikit-learn", "pandas", "numpy",
    "react", "angular", "vue", "node", "django", "flask", "spring",
    "git", "linux", "ci/cd", "machine learning", "deep learning",
    "power bi", "tableau", "excel",
]
# ------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ConsumerJobs")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Schema du JSON envoye par le producer
schema = StructType([
    StructField("job_id", StringType()),
    StructField("title", StringType()),
    StructField("company_name", StringType()),
    StructField("location", StringType()),
    StructField("description", StringType()),
    StructField("med_salary", StringType()),
    StructField("work_type", StringType()),
    StructField("experience", StringType()),
])

# 1) Lecture du flux Kafka
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# 2) Parsing JSON
parsed = (
    raw.selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema).alias("d"))
    .select("d.*")
)

# 3) Nettoyage : supprimer nulls, normaliser texte
clean = (
    parsed
    .filter(col("title").isNotNull() & col("description").isNotNull())
    .withColumn("desc_low", lower(col("description")))
    .withColumn("desc_low", regexp_replace(col("desc_low"), r"[^a-z0-9+#/ ]", " "))
)


# 4) Extraction des skills (on construit une regex contenant tous les skills)
def add_skills(df):
    # pour chaque skill on verifie sa presence -> on concatene ceux trouves
    from pyspark.sql.functions import expr
    skill_exprs = []
    for s in SKILLS:
        safe = s.replace("+", r"\\+").replace("#", r"\\#")
        skill_exprs.append(
            f"CASE WHEN desc_low RLIKE '(^| ){safe}( |$)' THEN '{s}' ELSE '' END"
        )
    concat_expr = "concat_ws(',', " + ", ".join(skill_exprs) + ")"
    df = df.withColumn("skills_raw", expr(concat_expr))
    # nettoyer les virgules vides
    df = df.withColumn("skills", regexp_replace(col("skills_raw"), r",+", ","))
    df = df.withColumn("skills", regexp_replace(col("skills"), r"^,|,$", ""))
    df = df.withColumn("skill_count",
                       size(split(col("skills"), ",")))
    return df


# 5) Ecriture dans PostgreSQL (batch par batch)
def write_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    batch_df = add_skills(batch_df)

    out = batch_df.select(
        col("job_id").cast("long"),
        "title", "company_name", "location", "description",
        col("med_salary").cast("double"),
        "work_type", "experience",
        "skills", "skill_count",
    )

    # ecrire les offres nettoyees
    (out.write
        .mode("append")
        .jdbc(PG_URL, "jobs_clean", properties=PG_PROPS))

    # mettre a jour le comptage des skills
    skills_df = (
        out.filter(col("skills") != "")
        .withColumn("skill", explode(split(col("skills"), ",")))
        .groupBy("skill").agg(count("*").alias("nb"))
    )
    (skills_df.write
        .mode("append")
        .jdbc(PG_URL, "skills_count", properties=PG_PROPS))

    print(f"[consumer] batch {batch_id} ecrit : {out.count()} offres")


query = (
    clean.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/chk_jobs")
    .start()
)

print("[consumer_jobs] Streaming demarre. En attente des messages Kafka ...")
query.awaitTermination()
