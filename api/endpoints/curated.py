from flask import Blueprint, jsonify
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

curated_bp = Blueprint("curated_bp", __name__)

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

@curated_bp.route("/", methods=["GET"])
def get_curated_data():
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db["imdb_reviews"]
        count = collection.count_documents({})
        client.close()
        return jsonify({"curated_records_count": count}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
