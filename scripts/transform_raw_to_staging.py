import os
import json
import re
import string
import boto3
import pymysql
from dotenv import load_dotenv
import nltk
from nltk.stem import WordNetLemmatizer

# Télécharger les ressources NLTK nécessaires
nltk.download('wordnet', quiet=True)
nltk.download('omw-1.4', quiet=True)

# Charger les variables d'environnement depuis .env
load_dotenv()

# Variables AWS et S3
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-3")
S3_BUCKET = os.getenv("S3_BUCKET")
RAW_KEY = "imdb_raw.json"  # Nom du fichier dans S3

# Variables MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB")

# Initialisation du lemmatizer
lemmatizer = WordNetLemmatizer()

def download_from_s3(bucket, key):
    """Télécharge le contenu d'un objet S3 et le retourne en tant que chaîne de caractères."""
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    print(f"Fichier '{key}' téléchargé depuis le bucket '{bucket}'.")
    return content

def advanced_clean_text(text):
    """
    Effectue un nettoyage avancé sur le texte :
    - Suppression des balises HTML
    - Conversion en minuscules
    - Suppression de la ponctuation
    - Réduction des espaces multiples
    - Tokenisation et lemmatisation
    Retourne le texte nettoyé, le nombre de mots et le nombre de caractères.
    """
    # Supprimer les balises HTML
    text = re.sub(r'<.*?>', '', text)
    # Conversion en minuscules
    text = text.lower()
    # Suppression de la ponctuation
    text = text.translate(str.maketrans('', '', string.punctuation))
    # Réduction des espaces multiples
    text = re.sub(r'\s+', ' ', text).strip()
    # Tokenisation
    tokens = text.split()
    # Lemmatisation
    lemmatized_tokens = [lemmatizer.lemmatize(token) for token in tokens]
    # Reconstitution du texte nettoyé
    cleaned_text = ' '.join(lemmatized_tokens)
    return cleaned_text, len(lemmatized_tokens), len(cleaned_text)

def advanced_clean_data(text, record_id):
    """
    Applique des transformations avancées sur une critique (chaîne de caractères).
    Retourne un tuple contenant :
    - id (généré ici par l'indice)
    - original_review (texte original)
    - cleaned_review (texte nettoyé et lemmatisé)
    - label (valeur par défaut -1 puisque non fourni)
    - word_count (nombre de mots)
    - char_count (nombre de caractères)
    """
    original_review = text.strip()
    cleaned_review, word_count, char_count = advanced_clean_text(original_review)
    # On attribue -1 comme label par défaut
    label = -1
    return (record_id, original_review, cleaned_review, label, word_count, char_count)

def create_table(cursor):
    """Crée la table imdb_reviews avec des colonnes pour stocker les transformations avancées."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS imdb_reviews (
        id INT PRIMARY KEY,
        original_review LONGTEXT,
        cleaned_review LONGTEXT,
        label INT,
        word_count INT,
        char_count INT
    );
    """
    cursor.execute(create_table_query)
    print("Table 'imdb_reviews' créée ou déjà existante.")

def insert_data(cursor, data):
    """Insère plusieurs enregistrements dans la table imdb_reviews."""
    insert_query = """
    INSERT INTO imdb_reviews (id, original_review, cleaned_review, label, word_count, char_count)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        original_review = VALUES(original_review),
        cleaned_review = VALUES(cleaned_review),
        label = VALUES(label),
        word_count = VALUES(word_count),
        char_count = VALUES(char_count);
    """
    cursor.executemany(insert_query, data)
    print(f"{cursor.rowcount} enregistrements insérés (ou mis à jour).")

def main():
    # Télécharger le fichier raw depuis S3
    raw_content = download_from_s3(S3_BUCKET, RAW_KEY)
    
    # Charger les données JSON
    try:
        data = json.loads(raw_content)
    except json.JSONDecodeError as e:
        print("Erreur lors du décodage JSON :", e)
        return

    # Si le JSON contient une clé "text", on utilise cette liste
    if isinstance(data, dict) and "text" in data:
        reviews = data["text"]
    else:
        reviews = data

    print("Nombre de critiques à traiter :", len(reviews))
    
    # Transformation avancée des données
    transformed_data = []
    for idx, review in enumerate(reviews):
        # Chaque review est une chaîne, donc on passe directement à la transformation
        transformed_data.append(advanced_clean_data(review, idx))
    
    print(f"Nombre d'enregistrements transformés prêts à être insérés : {len(transformed_data)}")
    
    # Connexion à MySQL
    try:
        connection = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            charset='utf8mb4'
        )
    except Exception as e:
        print("Erreur lors de la connexion à MySQL :", e)
        return
    
    try:
        with connection.cursor() as cursor:
            # Créer la table si nécessaire
            create_table(cursor)
            # Insérer les données transformées
            insert_data(cursor, transformed_data)
        connection.commit()
        print("Données insérées avec succès dans MySQL.")
    except Exception as e:
        print("Erreur lors de l'insertion des données :", e)
    finally:
        connection.close()

if __name__ == "__main__":
    main()
