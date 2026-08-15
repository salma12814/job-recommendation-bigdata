"""
=====================================================
 MODULE B  -  Segmentation (K-Means)  [comme Hamza]
=====================================================
K-Means (k=4) sur les caracteristiques des offres.
Groupes :
  0 Junior generaliste
  1 Senior specialise
  2 Cloud-DevOps
  3 Data

Entree : jobs_clean (PostgreSQL)
Sortie : clusters (PostgreSQL) -> visualise dans Power BI

Lancer DANS spark-master :
  docker exec -it spark-master spark-submit \
    --packages org.postgresql:postgresql:42.7.3 \
    /app/ml_module_b.py
=====================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when, length
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

PG_URL = "jdbc:postgresql://postgres:5432/recruitment"
PG_PROPS = {"user": "admin", "password": "admin", "driver": "org.postgresql.Driver"}

spark = SparkSession.builder.appName("ML_ModuleB_Segmentation").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1) Charger les offres nettoyees
df = spark.read.jdbc(PG_URL, "jobs_clean", properties=PG_PROPS)

# 2) Construire des features numeriques simples
df = df.withColumn("med_salary", col("med_salary").cast("double"))
df = df.fillna({"med_salary": 0.0, "skill_count": 0})
df = df.withColumn("desc_len", length(col("description")))

# indicateur cloud/devops (1 si mots cloud presents)
d = lower(col("description"))
df = df.withColumn(
    "is_cloud",
    when(d.rlike("aws|azure|gcp|docker|kubernetes|devops|terraform"), 1).otherwise(0)
)
# indicateur data
df = df.withColumn(
    "is_data",
    when(d.rlike("spark|hadoop|data|sql|machine learning|tensorflow|pytorch"), 1).otherwise(0)
)

features_cols = ["skill_count", "med_salary", "desc_len", "is_cloud", "is_data"]
assembler = VectorAssembler(inputCols=features_cols, outputCol="raw_features")
df_vec = assembler.transform(df)

scaler = StandardScaler(inputCol="raw_features", outputCol="features")
df_scaled = scaler.fit(df_vec).transform(df_vec)

# 3) K-Means
kmeans = KMeans(k=4, seed=42, featuresCol="features", predictionCol="cluster_id")
model = kmeans.fit(df_scaled)
result = model.transform(df_scaled)

# 4) Donner un label lisible a chaque cluster
labels = {
    0: "Junior generaliste",
    1: "Senior specialise",
    2: "Cloud-DevOps",
    3: "Data",
}
label_expr = None
for cid, name in labels.items():
    cond = (col("cluster_id") == cid)
    label_expr = when(cond, name) if label_expr is None else label_expr.when(cond, name)
result = result.withColumn("cluster_label", label_expr.otherwise("Autre"))

out = result.select(
    col("job_id").cast("long"),
    "title",
    col("skill_count").cast("int"),
    col("med_salary").cast("double"),
    col("cluster_id").cast("int"),
    "cluster_label",
)

# 5) Ecrire dans PostgreSQL
out.write.mode("overwrite").jdbc(PG_URL, "clusters", properties=PG_PROPS)
print("[Module B] Segmentation ecrite dans la table clusters")

# 6) Sauvegarder le modele
model.write().overwrite().save("/app/models/kmeans_segmentation")
print("[Module B] Modele K-Means sauvegarde")

spark.stop()
