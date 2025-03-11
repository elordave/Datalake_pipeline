from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Paramètres par défaut pour le DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'datalake_pipeline',
    default_args=default_args,
    description='Pipeline d’intégration du Data Lake : ingestion, transformation raw->staging, transformation staging->curated',
    schedule_interval='@daily',  # Vous pouvez changer la fréquence ou mettre None pour une exécution manuelle
    catchup=False
)

# Chemin absolu vers le dossier de votre projet.
# Remplacez ce chemin par le chemin absolu sur votre machine.
project_path = "C:/Users/dinel/OneDrive/Bureau/DataLake_Project/projet-datalake-imdb"

# Tâche 1 : Ingestion des données (couche Raw)
ingest_task = BashOperator(
    task_id='ingest_data',
    bash_command=f'python "{project_path}/scripts/ingestion.py"',
    dag=dag
)

# Tâche 2 : Transformation Raw -> Staging
transform_raw_task = BashOperator(
    task_id='transform_raw_to_staging',
    bash_command=f'python "{project_path}/scripts/transform_raw_to_staging.py"',
    dag=dag
)

# Tâche 3 : Transformation Staging -> Curated
transform_staging_task = BashOperator(
    task_id='transform_staging_to_curated',
    bash_command=f'python "{project_path}/scripts/transform_staging_to_curated.py"',
    dag=dag
)

# Définir l'ordre d'exécution des tâches
ingest_task >> transform_raw_task >> transform_staging_task
