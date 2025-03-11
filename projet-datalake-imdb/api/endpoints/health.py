from flask import Blueprint, jsonify

health_bp = Blueprint("health_bp", __name__)

@health_bp.route("/", methods=["GET"])
def health_check():
    return jsonify({"status": "OK"}), 200
