import streamlit as st
import requests
import pandas as pd
import altair as alt

st.set_page_config(page_title="Dashboard Data Lake & Airflow", layout="wide")

st.title("Dashboard Data Lake & Airflow")
st.markdown("Cette application vous permet de visualiser les résultats du Data Lake et du pipeline Airflow via l'API Gateway.")

page = st.sidebar.radio("Navigation", (
    "Accueil", 
    "Données Raw", 
    "Données Staging", 
    "Données Curated", 
    "Statistiques", 
    "Santé"
))

api_base = "http://localhost:5000"  # Assurez-vous que votre API Flask écoute sur ce port

@st.cache_data(show_spinner=False)
def fetch_api(endpoint):
    """Fonction pour interroger l'API et récupérer les données en JSON."""
    try:
        response = requests.get(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Erreur {response.status_code} sur {endpoint}")
    except Exception as e:
        st.error(f"Erreur de connexion à {endpoint} : {e}")
    return None

if page == "Accueil":
    st.header("Vue d'ensemble")
    stats = fetch_api(f"{api_base}/stats")
    if stats:
        df_stats = pd.DataFrame(list(stats.items()), columns=["Métrique", "Valeur"])
        st.table(df_stats)
        chart = alt.Chart(df_stats).mark_bar().encode(
            x=alt.X("Métrique:N"),
            y=alt.Y("Valeur:Q")
        )
        st.altair_chart(chart, use_container_width=True)

elif page == "Données Raw":
    st.header("Données Raw")
    raw_data = fetch_api(f"{api_base}/raw")
    if raw_data:
        preview = raw_data.get("raw_data_preview", "Aucun aperçu disponible.")
        st.text_area("Aperçu des données Raw", preview, height=300)

elif page == "Données Staging":
    st.header("Données Staging")
    staging_data = fetch_api(f"{api_base}/staging")
    if staging_data:
        st.write(staging_data)

elif page == "Données Curated":
    st.header("Données Curated")
    curated_data = fetch_api(f"{api_base}/curated")
    if curated_data:
        st.write(curated_data)

elif page == "Statistiques":
    st.header("Statistiques")
    stats = fetch_api(f"{api_base}/stats")
    if stats:
        df_stats = pd.DataFrame(list(stats.items()), columns=["Métrique", "Valeur"])
        st.table(df_stats)
        st.bar_chart(df_stats.set_index("Métrique"))

elif page == "Santé":
    st.header("État de Santé de l'API")
    health = fetch_api(f"{api_base}/health")
    if health:
        st.json(health)
