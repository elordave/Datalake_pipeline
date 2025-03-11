## Documentation technique

### Architecture de la solution

La solution mise en œuvre suit une architecture à 3 couches :

- **Raw Layer** : Amazon S3 pour stocker les fichiers JSON bruts récupérés.
- **Staging Layer** : Stockage dans MySQL.
- **Curated Layer (Données enrichies)** : MongoDB.
- **Orchestration (Airflow)** : Apache Airflow orchestre automatiquement les flux d'ingestion et de transformation.
- **Visualisation** : Interface avec Streamlit, consommant les données via une API Flask.

## Choix techniques

- **Stockage intermédiaire :** MySQL pour la facilité des requêtes SQL.
- **Stockage enrichi :** MongoDB choisi pour sa flexibilité avec les données enrichies structurées en format document JSON, adapté à des analyses flexibles.
- **Orchestration :** Apache Airflow, solution éprouvée en industrie pour orchestrer efficacement les scripts Python dans un pipeline.
- **API :** Flask pour la simplicité de mise en place d'une API REST.
- **Visualisation :** Streamlit pour un déploiement rapide, interactif, et facile à maintenir.

## Pré-requis

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Python 3.11](https://www.python.org/downloads/)
- [MySQL Community Edition](https://dev.mysql.com/downloads/mysql/)
- [MongoDB Community Server](https://www.mongodb.com/try/download/community)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
- [Streamlit](https://streamlit.io/)

## Étapes d'installation détaillées

### 1. Récupération du projet
Clonez le dépôt GitHub et naviguez vers son dossier :

```bash
git clone <url-du-projet>
cd projet-datalake-imdb
```

### 2. Configuration des variables d'environnement
Créez un fichier `.env` à la racine du projet avec ces valeurs :
```env
MYSQL_HOST=localhost
MYSQL_USER=root
MYSQL_PASSWORD=votre_mdp
MYSQL_DATABASE=datalake_staging
MONGO_URI=mongodb://localhost:27017
S3_BUCKET=projet-datalake-imdb
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.
```

### 2. Mise en place de l'environnement virtuel Python
```bash
python -m venv venv
source venv/bin/activate # sur Linux/macOS
venv\Scripts\activate  # sur Windows

pip install -r requirements.txt
```

### 2. Initialisation du stockage Raw
Assurez-vous que vos fichiers bruts sont chargés dans votre bucket Amazon S3 (`projet-datalake-imdb`).

### 3. Installation et lancement des bases de données (MySQL & MongoDB)
Assurez-vous que MySQL et MongoDB tournent localement sur leurs ports par défaut :
- **MySQL** : [localhost:3306](http://localhost:3306)
- **MongoDB** :  [MongoDB Compass](http://localhost:27017)

### 4. Lancer Airflow avec Docker

Allez dans le répertoire Airflow du projet :
```bash
cd airflow-docker
```

Lancez le service Airflow :
```bash
docker-compose up -d
```

Initialisez la base de données Airflow :
```bash
docker-compose run airflow-init airflow db init
```

Accédez à l'interface web Airflow :
```url
http://localhost:8080
```
Identifiants par défaut :
- **Username :** airflow
- **Password :** airflow

Vérifiez que votre DAG (`datalake_dag.py`) est visible et exécutable depuis l'interface web Airflow.
Dans la liste des DAGs, rechercher `datalake_pipeline`.
### 4. Lancer l’API Flask
Ouvrez un nouveau terminal et exécutez :
```bash
cd api
python app.py
```
Votre API sera disponible sur :
- [Raw Data](http://localhost:5000/raw)
- [Staging Layer](http://localhost:5000/staging)
- [Curated Layer](http://localhost:5000/curated)

### 5. Lancer le Dashboard Streamlit
Ouvrez un nouveau terminal puis :
```bash
cd frontend
python -m streamlit run appStreamlit.py
```

Consultez votre application web à l'adresse suivante : [http://localhost:8501](http://localhost:8501)

Votre interface affiche :
- Aperçu des données brutes (Raw)
- Données intermédiaires (Staging)
- Données enrichies (Curated)

Chaque visualisation récupère les données via l’API Flask.

## Validation Technique

Pour vérifier l'intégrité technique de votre pipeline :

- Vérifiez dans Airflow que votre DAG (`datalake_dag.py`) s’exécute correctement : [http://localhost:8080](http://localhost:8080)
- Consultez les logs de chaque étape sur l’interface d’Airflow.
- Vérifiez l’existence des données intermédiaires dans MySQL avec :
```sql
SELECT COUNT(*) FROM imdb_reviews;
```
- Vérifiez les données Curated dans MongoDB à l'aide de MongoDB Compass :
  ```bash
  mongo
  use datalake_curated
  db.imdb_reviews.find().limit(5)
  ```

**Remarques :**  
Je suis joignable pour toutes questions relatives au projet : [ely.sene@efrei.net](mailto:ely.sene@efrei.net)