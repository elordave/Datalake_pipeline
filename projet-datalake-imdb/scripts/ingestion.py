import os
import json
import boto3
from datasets import load_dataset
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-3")  
S3_BUCKET = os.getenv("S3_BUCKET")

def upload_to_s3(bucket, key, content):
    # Création d'un client S3 avec les clés depuis les variables d'environnement
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    # Envoi du contenu vers S3
    s3.put_object(Bucket=bucket, Key=key, Body=content)
    print(f"Fichier envoyé sur S3 dans le bucket '{bucket}' sous la clé '{key}'.")

if __name__ == "__main__":
    print("Téléchargement du dataset IMDB...")
    # Télécharger le dataset IMDB depuis Hugging Face
    dataset = load_dataset("imdb")
    # On récupère par exemple la partition 'train'
    data_train = dataset["train"][:]
    json_data = json.dumps(data_train, ensure_ascii=False, indent=2)
    
    key_name = "imdb_raw.json"
    upload_to_s3(S3_BUCKET, key_name, json_data)
    
    print("Le dataset IMDB a été ingéré et stocké dans la couche Raw (S3).")
