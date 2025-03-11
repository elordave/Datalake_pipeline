from flask import Blueprint, jsonify
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

raw_bp = Blueprint("raw_bp", __name__)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-3")
S3_BUCKET = os.getenv("S3_BUCKET")
RAW_KEY = "imdb_raw.json"

@raw_bp.route("/", methods=["GET"])
def get_raw_data():
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        response = s3.get_object(Bucket=S3_BUCKET, Key=RAW_KEY)
        content = response["Body"].read().decode("utf-8")
        # Renvoie un aperçu (les 1000 premiers caractères) pour éviter de surcharger la réponse
        return jsonify({"raw_data_preview": content[:1000]}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
