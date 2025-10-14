import streamlit as st
import pandas as pd
import requests
import datetime
import time
from urllib.parse import urlencode
from io import StringIO
from pydref import Pydref

# =========================================================
# CONFIGURATION
# =========================================================
st.set_page_config(
    page_title="Alignement IdRef ↔ HAL",
    layout="wide"
)

HAL_SEARCH_API = "https://api.archives-ouvertes.fr/search/"
HAL_AUTHOR_API = "https://api.archives-ouvertes.fr/ref/author/"
FIELDS_LIST = "docid,fullName_s,valid_s,halId_s,orcidId_s,firstName_s,lastName_s"
REQUEST_DELAY = 0.3

# =========================================================
# INITIALISATION PYDREF
# =========================================================
@st.cache_resource
def get_pydref_instance():
    return Pydref()

try:
    pydref_api = get_pydref_instance()
except Exception as e:
    st.error(f"Erreur lors de l'initialisation de Pydref : {e}")
    st.stop()

# =========================================================
# FONCTIONS DE RECHERCHE HAL
# =========================================================
def fetch_publications_for_collection(collection_code):
    """Récupère toutes les publications de la collection HAL donnée."""
    all_docs = []
    rows = 10000
    start = 0
    query_params = {"q": "*:*", "wt": "json", "fl": "structHasAuthId_fs", "rows": rows}

    while True:
        query_params["start"] = start
        url = f"{HAL_SEARCH_API}{collection_code}/?{urlencode(query_params)}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        docs = data.get("response", {}).get("docs", [])
        all_docs.extend(docs)

        if len(docs) < rows:
            break
        start += rows
        time.sleep(REQUEST_DELAY)

    return all_docs


def extract_author_ids(publications):
    """Extrait les IDs auteurs depuis les publications HAL."""
    author_ids = set()
    for doc in publications:
        authors = doc.get("structHasAuthId_fs", [])
        for a in authors:
            parts = a.split("_JoinSep_")
            if len(parts) > 1:
                full_id = parts[1].split("_FacetSep")[0]
                docid = full_id.split("-")[-1].strip()
                if docid.isdigit() and docid != "0":
                    author_ids.add(docid)
    return list(author_ids)


def fetch_author_details_batch(author_ids, fields, batch_size=20):
    """Récupère les formes-auteurs HAL en requêtes par lot."""
    authors_details = []
    clean_ids = [i.strip() for i in author_ids if i.strip()]
    total = len(clean_ids)

    progress = st.progress(0, text="Chargement des formes-auteurs HAL...")

    for start in range(0, total, batch_size):
        batch = clean_ids[start:start + batch_size]
        or_query = " OR ".join([f'person_i:"{i}"' for i in batch])
        params = {"q": or_query, "wt": "json", "fl": fields, "rows": batch_size}
        url = f"{HAL_AUTHOR_API}?{urlencode(params)}"

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            docs = data.get("response", {}).get("docs", [])
            authors_details.extend(docs)
        except requests.exceptions.RequestException as e:
            st.warning(f"⚠️ Erreur sur le lot {batch}: {e}")
            continue

        progress.progress(min((start + batch_size) / total, 1.0))
        time.sleep(REQUEST_DELAY)

    progress.empty()
    return authors_details

# =========================================================
# RECHERCHE IDREF
# =========================================================
def search_idref_for_person(full_name, min_birth_year, min_death_year):
    try:
        return pydref_api.get_idref(
            query=full_name,
            min_birth_year=min_birth_year,
            min_death_year=min_death_year,
            is_scientific=True,
            exact_fullname=True
        )
    except Exception as e:
        st.warning(f"Erreur IdRef pour '{full_name}': {e}")
        return []

# =========================================================
# INTERFACE UTILISATEUR
# =========================================================
st.title("🔗 Alignez une liste de chercheurs avec IdRef et HAL")
st.markdown(
    "Téléversez un fichier CSV ou Excel avec les colonnes **Nom** et **Prénom**, "
    "et saisissez une **collection HAL**. L’application recherche les correspondances "
    "dans **IdRef** et dans **HAL**, fusionne les données, et génère un CSV enrichi."
)

uploaded_file = st.file_uploader("📁 Téléverser un fichier (.csv, .xlsx)", type=["csv", "xlsx"])
collection_code = st.text_input("🏛️ Code de la collection HAL (ex: CDMO)", "")

col1, col2 = st.columns(2)
current_year = datetime.datetime.now().year
min_birth_year = col1.number_input("Année de naissance min. (YYYY)", value=1920, min_value=1000, max_value=current_year, step=1)
min_death_year = col2.number_input("Année de décès min. (YYYY)", value=2005, min_value=1000, max_value=current_year + 5, step=1)

if uploaded_file and collection_code:
    try:
        if uploaded_file.name.endswith(".csv"):
            data = pd.read_csv(uploaded_file)
        else:
            data = pd.read_excel(uploaded_file)

        st.success(f"✅ {len(data)} lignes chargées depuis {uploaded_file.name}.")
        st.dataframe(data.head())

        cols = data.columns.tolist()
        name_col = st.selectbox("Colonne Nom", options=cols, index=0)
        firstname_col = st.selectbox("Colonne Prénom", options=cols, index=1 if len(cols) > 1 else 0)

        if st.button("🚀 Lancer la recherche combinée IdRef + HAL", type="primary"):
            # --------------------
            # Étape 1 : IdRef
            # --------------------
            idref_results = []
            st.info("🔍 Recherche IdRef en cours...")
            progress_idref = st.progress(0)

            for idx, row in data.iterrows():
                full_name = f"{row[firstname_col]} {row[name_col]}".strip()
                matches = search_idref_for_person(full_name, min_birth_year, min_death_year)
                ppn = matches[0].get("idref") if matches else None

                idref_results.append({
                    "Nom": row[name_col],
                    "Prénom": row[firstname_col],
                    "idref_ppn": ppn,
                    "source": "Fichier"
                })

                progress_idref.progress((idx + 1) / len(data))

            idref_df = pd.DataFrame(idref_results)

            # --------------------
            # Étape 2 : HAL
            # --------------------
            st.info(f"📡 Recherche des formes-auteurs HAL pour la collection {collection_code}...")
            pubs = fetch_publications_for_collection(collection_code)
            author_ids = extract_author_ids(pubs)
            hal_authors = fetch_author_details_batch(author_ids, FIELDS_LIST, batch_size=20)

            hal_df = pd.DataFrame(hal_authors)
            hal_df.rename(columns={
                "fullName_s": "Nom_Complet",
                "firstName_s": "Prénom",
                "lastName_s": "Nom"
            }, inplace=True)
            hal_df["source"] = "HAL"

            # --------------------
            # Étape 3 : Fusion IdRef ↔ HAL
            # --------------------
            merged_df = pd.merge(
                idref_df,
                hal_df,
                on=["Nom", "Prénom"],
                how="outer",
                indicator=True
            )

            merged_df["source"] = merged_df["_merge"].map({
                "both": "Fichier + HAL",
                "left_only": "Fichier",
                "right_only": "HAL"
            })
            merged_df.drop(columns=["_merge"], inplace=True)

            st.success(f"Fusion terminée : {len(merged_df)} lignes finales.")
            st.dataframe(merged_df.head())

            # --------------------
            # Étape 4 : Export CSV
            # --------------------
            csv_output = merged_df.to_csv(index=False, sep=";", encoding="utf-8")
            st.download_button(
                "💾 Télécharger le fichier fusionné (CSV)",
                csv_output,
                file_name=f"fusion_idref_hal_{collection_code}_{datetime.datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

    except Exception as e:
        st.error(f"Erreur lors du traitement : {e}")
