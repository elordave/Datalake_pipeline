from flask import Blueprint, jsonify
import pymysql
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

stats_bp = Blueprint("stats_bp", __name__)

# MySQL variables
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB")

# MongoDB variables
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

@stats_bp.route("/", methods=["GET"])
def get_stats():
    # Récupérer le nombre d'enregistrements dans la couche Staging (MySQL)
    try:
        connection = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            charset='utf8mb4'
        )
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM imdb_reviews;")
            staging_count = cursor.fetchone()[0]
        connection.close()
    except Exception as e:
        staging_count = f"Error: {str(e)}"
    
    # Récupérer le nombre d'enregistrements dans la couche Curated (MongoDB)
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        curated_count = db["imdb_reviews"].count_documents({})
        client.close()
    except Exception as e:
        curated_count = f"Error: {str(e)}"
    
    return jsonify({
        "staging_records_count": staging_count,
        "curated_records_count": curated_count
    }), 200
