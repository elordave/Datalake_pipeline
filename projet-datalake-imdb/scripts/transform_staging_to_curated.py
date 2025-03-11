import os
import pymysql
from pymongo import MongoClient
from dotenv import load_dotenv
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# Télécharger le lexique VADER (seulement la première fois)
nltk.download('vader_lexicon', quiet=True)

# Charger les variables d'environnement depuis .env
load_dotenv()

# Variables MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB")

# Variables MongoDB
MONGO_URI = os.getenv("MONGO_URI")  
MONGO_DB = os.getenv("MONGO_DB")    

# Initialisation de l'analyseur de sentiment VADER
sia = SentimentIntensityAnalyzer()

def fetch_data_from_mysql():
    """Récupère tous les enregistrements de la table imdb_reviews dans MySQL."""
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
        return []

    data = []
    try:
        with connection.cursor() as cursor:
            query = "SELECT id, original_review, cleaned_review, label, word_count, char_count FROM imdb_reviews"
            cursor.execute(query)
            data = cursor.fetchall()
            print(f"{len(data)} enregistrements récupérés depuis MySQL.")
    except Exception as e:
        print("Erreur lors de la récupération des données :", e)
    finally:
        connection.close()
    return data

def enrich_data(record):
    """
    Pour un enregistrement donné, calcule l'analyse de sentiment sur le texte nettoyé.
    Retourne un dictionnaire avec toutes les informations enrichies.
    """
    # record est une tuple : (id, original_review, cleaned_review, label, word_count, char_count)
    (record_id, original_review, cleaned_review, label, word_count, char_count) = record
    
    # Calculer le score de sentiment en utilisant le champ cleaned_review
    sentiment_scores = sia.polarity_scores(cleaned_review)
    compound_score = sentiment_scores.get('compound')
    
    # Catégoriser le sentiment 
    if compound_score >= 0.05:
        sentiment = "positive"
    elif compound_score <= -0.05:
        sentiment = "negative"
    else:
        sentiment = "neutral"
    
    enriched_record = {
        "id": record_id,
        "original_review": original_review,
        "cleaned_review": cleaned_review,
        "label": label,
        "word_count": word_count,
        "char_count": char_count,
        "sentiment_score": compound_score,
        "sentiment": sentiment
    }
    return enriched_record

def insert_into_mongodb(documents):
    """Insère une liste de documents dans la collection imdb_reviews de MongoDB."""
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db["imdb_reviews"]
        # Insertion en masse
        result = collection.insert_many(documents)
        print(f"{len(result.inserted_ids)} documents insérés dans MongoDB.")
    except Exception as e:
        print("Erreur lors de l'insertion dans MongoDB :", e)
    finally:
        client.close()

def main():
    # Étape 1 : Récupérer les données depuis MySQL
    mysql_data = fetch_data_from_mysql()
    if not mysql_data:
        print("Aucune donnée récupérée depuis MySQL. Arrêt du script.")
        return
    
    # Étape 2 : Enrichir les données avec l'analyse de sentiment
    enriched_documents = []
    for record in mysql_data:
        enriched_doc = enrich_data(record)
        enriched_documents.append(enriched_doc)
    
    print(f"Nombre d'enregistrements enrichis prêts à être insérés dans MongoDB : {len(enriched_documents)}")
    
    # Étape 3 : Insérer les documents enrichis dans MongoDB
    insert_into_mongodb(enriched_documents)

if __name__ == "__main__":
    main()
