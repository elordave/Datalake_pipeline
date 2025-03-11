from flask import Blueprint, jsonify
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

staging_bp = Blueprint("staging_bp", __name__)

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB")

@staging_bp.route("/", methods=["GET"])
def get_staging_data():
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
            count = cursor.fetchone()[0]
        connection.close()
        return jsonify({"staging_records_count": count}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
