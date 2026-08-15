"""
CONSUMER BATCH (version Airflow) - PySpark mode LOCAL.
Lit TOUT ce qui est dans Kafka (trigger availableNow), nettoie,
extrait les skills, ecrit dans PostgreSQL, puis S'ARRETE.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lower, regexp_replace, split, size, explode, count, expr
)
from pyspark.sql.types import StructType, StructField, StringType

KAFKA_SERVER = "kafka:29092"
TOPIC = "jobs"
PG_URL = "jdbc:postgresql://postgres:5432/recruitment"
PG_PROPS = {"user": "admin", "password": "admin", "driver": "org.postgresql.Driver"}

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

spark = (
    SparkSession.builder
    .appName("ConsumerBatch")
    .master("local[*]")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3")
    .config("spark.jars.ivy", "/tmp/.ivy2")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

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

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")     # on relit tout depuis le debut
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema).alias("d"))
    .select("d.*")
)

clean = (
    parsed
    .filter(col("title").isNotNull() & col("description").isNotNull())
    .withColumn("desc_low", lower(col("description")))
    .withColumn("desc_low", regexp_replace(col("desc_low"), r"[^a-z0-9+#/ ]", " "))
)


def add_skills(df):
    exprs = []
    for s in SKILLS:
        safe = s.replace("+", r"\\+").replace("#", r"\\#")
        exprs.append(f"CASE WHEN desc_low RLIKE '(^| ){safe}( |$)' THEN '{s}' ELSE '' END")
    df = df.withColumn("skills_raw", expr("concat_ws(',', " + ", ".join(exprs) + ")"))
    df = df.withColumn("skills", regexp_replace(col("skills_raw"), r",+", ","))
    df = df.withColumn("skills", regexp_replace(col("skills"), r"^,|,$", ""))
    df = df.withColumn("skill_count", size(split(col("skills"), ",")))
    return df


def write_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    batch_df = add_skills(batch_df)
    out = batch_df.select(
        col("job_id").cast("long"),
        "title", "company_name", "location", "description",
        col("med_salary").cast("double"),
        "work_type", "experience", "skills", "skill_count",
    )
    out.write.mode("append").jdbc(PG_URL, "jobs_clean", properties=PG_PROPS)

    skills_df = (
        out.filter(col("skills") != "")
        .withColumn("skill", explode(split(col("skills"), ",")))
        .groupBy("skill").agg(count("*").alias("nb"))
    )
    skills_df.write.mode("append").jdbc(PG_URL, "skills_count", properties=PG_PROPS)
    print(f"[consumer-batch] batch {batch_id} : {out.count()} offres ecrites")


query = (
    clean.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .trigger(availableNow=True)              # <-- lit tout puis s'arrete
    .option("checkpointLocation", "/tmp/chk_jobs_batch")
    .start()
)

query.awaitTermination()
print("[consumer-batch] TERMINE.")
spark.stop()
