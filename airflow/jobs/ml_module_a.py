"""
=====================================================
 MODULE A  -  Classification des metiers (Spark MLlib)
=====================================================
Random Forest sur les descriptions d'offres.
Predit la categorie metier :
  Data Engineer / DevOps / Backend / Data Scientist / ML Engineer

Entree  : table jobs_clean (PostgreSQL)
Sortie  : table job_predictions (PostgreSQL) + modele sauvegarde

Lancer DANS spark-master :
  docker exec -it spark-master spark-submit \
    --packages org.postgresql:postgresql:42.7.3 \
    /app/ml_module_a.py
=====================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, StringIndexer, IndexToString
from pyspark.ml.classification import RandomForestClassifier

PG_URL = "jdbc:postgresql://postgres:5432/recruitment"
PG_PROPS = {"user": "admin", "password": "admin", "driver": "org.postgresql.Driver"}

spark = (
    SparkSession.builder
    .appName("ML_ModuleA_Classification")
    .master("local[*]")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .config("spark.jars.ivy", "/tmp/.ivy2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1) Charger les offres nettoyees
df = spark.read.jdbc(PG_URL, "jobs_clean", properties=PG_PROPS)
df = df.filter(col("description").isNotNull())

# 2) Creer le LABEL automatiquement a partir du titre (faible supervision)
#    -> sert d'etiquette d'entrainement realiste pour un projet etudiant
t = lower(col("title"))
df = df.withColumn(
    "label_text",
    when(t.rlike("data engineer|etl|pipeline|big data"), "Data Engineer")
    .when(t.rlike("devops|sre|infrastructure|platform|cloud engineer"), "DevOps")
    .when(t.rlike("data scientist|analytics|statistic"), "Data Scientist")
    .when(t.rlike("machine learning|ml engineer|ai engineer|deep learning"), "ML Engineer")
    .when(t.rlike("backend|back-end|software engineer|developer|full stack|java|python developer"), "Backend")
    .otherwise(None)
)
df = df.filter(col("label_text").isNotNull())

print(f"[Module A] {df.count()} offres etiquetees pour l'entrainement")

# 3) Pipeline ML : texte -> TF-IDF -> Random Forest
tokenizer = Tokenizer(inputCol="description", outputCol="words")
tf = HashingTF(inputCol="words", outputCol="tf", numFeatures=5000)
idf = IDF(inputCol="tf", outputCol="features")
indexer = StringIndexer(inputCol="label_text", outputCol="label")
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=50)

pipeline = Pipeline(stages=[tokenizer, tf, idf, indexer, rf])

# 4) Split train/test
train, test = df.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train)

# 5) Evaluation simple
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
pred = model.transform(test)
acc = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy"
).evaluate(pred)
print(f"[Module A] Accuracy = {acc:.3f}")

# 6) Predire sur TOUTES les offres et reconvertir l'index en texte
labels = model.stages[3].labels   # StringIndexer labels
all_pred = model.transform(df)
converter = IndexToString(inputCol="prediction", outputCol="predicted_job", labels=labels)
all_pred = converter.transform(all_pred)

out = all_pred.select(
    col("job_id").cast("long"),
    "title",
    "predicted_job",
    col("med_salary").cast("double"),
)

# 7) Ecrire dans PostgreSQL
out.write.mode("overwrite").jdbc(PG_URL, "job_predictions", properties=PG_PROPS)
print("[Module A] Predictions ecrites dans job_predictions")

# 8) Sauvegarder le modele
model.write().overwrite().save("/tmp/models/rf_job_classifier")
print("[Module A] Modele sauvegarde dans /tmp/models/rf_job_classifier")

spark.stop()
