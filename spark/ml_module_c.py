"""
=====================================================
 MODULE C  -  Recommandation CV <-> Offres
=====================================================
TF-IDF + Cosine Similarity (scikit-learn).
Utilise directement par l'app Flask.

Deux fonctions :
  recommend_jobs_for_cv(cv_text)      -> top 5 offres pour un CV
  recommend_candidates_for_offer(txt) -> top 5 offres proches d'une offre
                                         (comme Hamza : offre -> profils proches)

Les offres sont lues depuis PostgreSQL (table jobs_clean).
=====================================================
"""

import psycopg2
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# ---- CONFIG (depuis la machine Windows -> localhost) -------
PG_CONN = dict(
    host="localhost",
    port=5432,
    dbname="recruitment",
    user="admin",
    password="admin",
)
# ------------------------------------------------------------


def _load_jobs():
    """Charge les offres depuis PostgreSQL dans un DataFrame pandas."""
    conn = psycopg2.connect(**PG_CONN)
    df = pd.read_sql(
        "SELECT job_id, title, company_name, location, description, "
        "med_salary, skills FROM jobs_clean WHERE description IS NOT NULL",
        conn,
    )
    conn.close()
    return df


class Recommender:
    """Vectorise toutes les offres une seule fois, puis compare."""

    def __init__(self):
        self.df = _load_jobs()
        # texte combine = titre + description + skills
        self.df["corpus"] = (
            self.df["title"].fillna("") + " "
            + self.df["description"].fillna("") + " "
            + self.df["skills"].fillna("")
        )
        self.vectorizer = TfidfVectorizer(
            stop_words="english", max_features=5000, ngram_range=(1, 2)
        )
        self.matrix = self.vectorizer.fit_transform(self.df["corpus"])

    def _top(self, query_text, k=5):
        q_vec = self.vectorizer.transform([query_text])
        sims = cosine_similarity(q_vec, self.matrix).flatten()
        idx = sims.argsort()[::-1][:k]
        results = self.df.iloc[idx].copy()
        results["score"] = sims[idx].round(3)
        return results[
            ["title", "company_name", "location", "med_salary", "skills", "score"]
        ].to_dict(orient="records")

    def recommend_jobs_for_cv(self, cv_text, k=5):
        """Top k offres recommandees pour le texte d'un CV."""
        return self._top(cv_text, k)

    def recommend_candidates_for_offer(self, offer_text, k=5):
        """
        Top k offres similaires a une offre donnee.
        (Faute de base de CV separee, on retourne les offres proches :
         meme logique de similarite que Hamza pour offre -> profils.)
        """
        return self._top(offer_text, k)


if __name__ == "__main__":
    # petit test en ligne de commande
    r = Recommender()
    print("Test recommandation pour un CV 'python spark aws docker' :")
    for job in r.recommend_jobs_for_cv("python spark aws docker data engineer"):
        print(" -", job["title"], "| score", job["score"])
