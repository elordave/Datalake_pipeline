from flask import Flask
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
load_dotenv()

app = Flask(__name__)

# Importer les blueprints depuis les endpoints
from endpoints.raw import raw_bp
from endpoints.staging import staging_bp
from endpoints.curated import curated_bp
from endpoints.health import health_bp
from endpoints.stats import stats_bp

# Enregistrer les blueprints avec un préfixe d'URL
app.register_blueprint(raw_bp, url_prefix="/raw")
app.register_blueprint(staging_bp, url_prefix="/staging")
app.register_blueprint(curated_bp, url_prefix="/curated")
app.register_blueprint(health_bp, url_prefix="/health")
app.register_blueprint(stats_bp, url_prefix="/stats")

if __name__ == "__main__":
    port = int(os.getenv("API_PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port)
