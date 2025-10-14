# hal_authors_app.py
# ------------------------------------------------------------
# Application Streamlit pour extraire les formes-auteurs HAL
# par collection et période, avec export CSV automatique.
# ------------------------------------------------------------

import streamlit as st
import requests
import pandas as pd
import time
from urllib.parse import urlencode

# ------------------------------------------------------------
# Constantes
# ------------------------------------------------------------
HAL_SEARCH_API = "https://api.archives-ouvertes.fr/search/"
HAL_AUTHOR_API = "https://api.archives-ouvertes.fr/ref/author/"
FIELDS_LIST = "docid,fullName_s,firstName_s,lastName_s,valid_s,idHal_s,halId_s,idrefId_s,orcidId_s,emailDomain_s "
REQUEST_DELAY = 0.5  # délai recommandé entre requêtes

# ------------------------------------------------------------
# Fonctions utilitaires
# ------------------------------------------------------------
def fetch_publications_for_collection(collection_code, years):
    """ Récupère toutes les publications d'une collection HAL. """
    all_docs = []
    rows = 10000
    start = 0

    query_params = {
        "q": "*:*",
        "wt": "json",
        "fl": "structHasAuthId_fs",
        "rows": rows,
    }

    if years:
        query_params["fq"] = f"producedDateY_i:{years}"

    while True:
        query_params["start"] = start
        url = f"{HAL_SEARCH_API}{collection_code}/?{urlencode(query_params)}"

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        docs = data.get("response", {}).get("docs", [])
        num_found = data.get("response", {}).get("numFound", 0)
        all_docs.extend(docs)

        if len(all_docs) >= num_found or not docs:
            break

        start += rows
        time.sleep(REQUEST_DELAY)

    return all_docs


def extract_author_ids(publications):
    """ Extrait tous les docid auteurs uniques d'une liste de publications. """
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
    """ Récupère les formes-auteurs par lots pour accélérer les requêtes. """
    authors_details = []
    clean_ids = [i.strip() for i in author_ids if i.strip()]
    total = len(clean_ids)

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
            for doc in docs:
                if doc.get("valid_s") == "VALID":
                    doc["valid_s"] = "forme auteur principale d'un IdHAL"
                elif doc.get("valid_s") == "OLD":
                    doc["valid_s"] = "forme auteur alternative d'un IdHAL"
                elif doc.get("valid_s") == "INCOMING":
                    doc["valid_s"] = "forme auteur sans IdHAL associé"
                authors_details.append(doc)
        except requests.exceptions.RequestException as e:
            st.warning(f"⚠️ Erreur sur le lot {batch}: {e}")
            continue

        progress = min(start + batch_size, total)
        st.progress(progress / total)
        time.sleep(REQUEST_DELAY)

    return authors_details


# ------------------------------------------------------------
# Interface Streamlit
# ------------------------------------------------------------
st.set_page_config(page_title="Extraction HAL - Auteurs", page_icon="📚", layout="centered")

st.title("📚 Extraction des formes-auteurs HAL")
st.markdown(
    """
    Cette application extrait les **formes-auteurs** (IdHAL, ORCID, etc.)
    à partir d’une **collection HAL** et d’une **période donnée**.
    """
)

# Entrées utilisateur
col1, col2 = st.columns(2)
with col1:
    collection_code = st.text_input("Code de la collection HAL (ex : CDMO)", "")
with col2:
    years = st.text_input("Année ou intervalle (ex : 2024 ou [2020 TO 2024])", "")

batch_size = st.slider("Taille des lots (requêtes groupées)", 10, 50, 20, step=5)
delay = st.slider("Délai entre requêtes (secondes)", 0.1, 1.0, 0.5, 0.1)

# Lancement
if st.button("🚀 Lancer l'extraction") and collection_code:
    REQUEST_DELAY = delay
    st.info(f"Extraction en cours pour **{collection_code}**, période **{years or 'toutes'}**...")

    try:
        # Étape 1
        pubs = fetch_publications_for_collection(collection_code, years)
        if not pubs:
            st.error("Aucune publication trouvée pour cette collection.")
        else:
            # Étape 2
            author_ids = extract_author_ids(pubs)
            st.success(f"{len(author_ids)} formes-auteurs détectées.")

            # Étape 3
            details = fetch_author_details_batch(author_ids, FIELDS_LIST, batch_size=batch_size)

            if not details:
                st.error("Aucune forme-auteur récupérée.")
            else:
                df = pd.DataFrame(details)
                requested_fields = [f.strip() for f in FIELDS_LIST.split(",")]
                df = df[[f for f in requested_fields if f in df.columns]]

                filename = f"formes_auteurs_{collection_code}_{years or 'all'}.csv"
                csv = df.to_csv(index=False, sep=";", encoding="utf-8")
                st.download_button("📥 Télécharger le CSV", csv, file_name=filename, mime="text/csv")

                st.dataframe(df.head())
                st.success(f"Extraction terminée ({len(df)} lignes).")

    except Exception as e:
        st.error(f"Erreur pendant l'extraction : {e}")
