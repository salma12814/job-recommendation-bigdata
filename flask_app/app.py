"""
=====================================================
 FLASK APP  -  app.py
=====================================================
Interface web :
  - Page 1 : Upload CV PDF -> top 5 offres recommandees
  - Page 2 : Coller une offre -> top 5 offres/profils proches (comme Hamza)

Lancer (depuis Windows) :
    cd flask_app
    pip install -r requirements.txt
    python app.py
Puis ouvrir : http://localhost:5000
=====================================================
"""

import os
import sys

from flask import Flask, render_template, request
import PyPDF2

# importer le moteur de reco (Module C) depuis le dossier spark
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "spark"))
from ml_module_c import Recommender  # noqa: E402

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "uploads"
os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

# On charge le recommender UNE fois au demarrage (vectorisation des offres)
print("[flask] Chargement du moteur de recommandation ...")
RECO = Recommender()
print("[flask] Pret.")


def extract_pdf_text(path):
    """Extrait le texte d'un fichier PDF."""
    text = ""
    with open(path, "rb") as f:
        reader = PyPDF2.PdfReader(f)
        for page in reader.pages:
            text += page.extract_text() or ""
    return text


@app.route("/", methods=["GET", "POST"])
def index():
    jobs = None
    error = None
    if request.method == "POST":
        file = request.files.get("cv")
        if not file or file.filename == "":
            error = "Merci de choisir un fichier PDF."
        else:
            path = os.path.join(app.config["UPLOAD_FOLDER"], file.filename)
            file.save(path)
            cv_text = extract_pdf_text(path)
            if not cv_text.strip():
                error = "Impossible de lire le texte du PDF."
            else:
                jobs = RECO.recommend_jobs_for_cv(cv_text, k=5)
    return render_template("index.html", jobs=jobs, error=error)


@app.route("/offre", methods=["GET", "POST"])
def offre():
    results = None
    offer_text = ""
    if request.method == "POST":
        offer_text = request.form.get("offer_text", "")
        if offer_text.strip():
            results = RECO.recommend_candidates_for_offer(offer_text, k=5)
    return render_template("offre.html", results=results, offer_text=offer_text)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
