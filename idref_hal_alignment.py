import streamlit as st
import pandas as pd
import requests
import datetime
import time
from urllib.parse import urlencode
from io import StringIO
from pydref import Pydref
import unicodedata
from difflib import SequenceMatcher

# Try to import rapidfuzz for better fuzzy matching; fall back to difflib if unavailable
try:
    from rapidfuzz import fuzz
    from rapidfuzz import process as rf_process
    USE_RAPIDFUZZ = True
except Exception:
    USE_RAPIDFUZZ = False

# =========================================================
# CONFIGURATION
# =========================================================
st.set_page_config(
    page_title="Alignement IdRef ↔ HAL (avec matching flou)",
    layout="wide"
)

HAL_SEARCH_API = "https://api.archives-ouvertes.fr/search/"
HAL_AUTHOR_API = "https://api.archives-ouvertes.fr/ref/author/"
FIELDS_LIST = "docid,fullName_s,valid_s,halId_s,orcidId_s,firstName_s,lastName_s"
REQUEST_DELAY = 0.3

# =========================================================
# UTIL: normalisation des chaînes (supprime accents, ponctuation, lowercase)
# =========================================================
def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    # Unicode normalize and strip diacritics
    s = unicodedata.normalize('NFD', s)
    s = ''.join(ch for ch in s if unicodedata.category(ch) != 'Mn')
    # Lowercase and remove extra spaces/punctuation except hyphen and space
    s = s.lower()
    # Replace multiple spaces with single
    s = " ".join(s.split())
    return s

def similarity_score(a: str, b: str) -> float:
    """
    Retourne un score [0..100] de similarité entre deux chaînes.
    Utilise rapidfuzz si disponible, sinon difflib.SequenceMatcher.
    """
    if not a and not b:
        return 100.0
    if USE_RAPIDFUZZ:
        return fuzz.QRatio(a, b)  # QRatio est rapide et accent-insensitive si on normalise en amont
    else:
        # SequenceMatcher donne 0..1 ratio
        return SequenceMatcher(None, a, b).ratio() * 100.0

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

    if total == 0:
        return []

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
# FONCTION DE FUSION AVEC MATCHING FLOU
# =========================================================
def fuzzy_merge_file_hal(df_file, df_hal, threshold=85):
    """
    df_file: DataFrame with at least columns 'Nom' and 'Prénom' (source = 'Fichier')
    df_hal: DataFrame from HAL with columns 'Nom', 'Prénom', plus HAL fields (source = 'HAL')
    threshold: minimum similarity (0..100) to consider a match on normalized full name
    Returns merged dataframe with source column set to 'Fichier', 'HAL' or 'Fichier + HAL'
    """
    # Prepare copies
    df_file = df_file.copy()
    df_hal = df_hal.copy()

    # Normalize name columns
    df_file['norm_first'] = df_file['Prénom'].fillna('').apply(normalize_text)
    df_file['norm_last']  = df_file['Nom'].fillna('').apply(normalize_text)
    df_file['norm_full']  = (df_file['norm_first'] + ' ' + df_file['norm_last']).str.strip()

    df_hal['norm_first'] = df_hal['Prénom'].fillna('').apply(normalize_text)
    df_hal['norm_last']  = df_hal['Nom'].fillna('').apply(normalize_text)
    # HAL may have fullName_s too
    if 'Nom_Complet' in df_hal.columns:
        df_hal['norm_full'] = df_hal['Nom_Complet'].fillna('').apply(normalize_text)
    else:
        df_hal['norm_full'] = (df_hal['norm_first'] + ' ' + df_hal['norm_last']).str.strip()

    # Keep track of which HAL rows are already matched
    hal_available = df_hal.copy()
    hal_available['__matched'] = False

    merged_rows = []
    used_hal_indices = set()

    # For each file row, find best HAL match among available ones
    for f_idx, f_row in df_file.iterrows():
        f_name = f_row['norm_full']
        best_score = -1
        best_h_idx = None

        # If empty name, skip matching
        if not f_name:
            merged_rows.append({
                **f_row.to_dict(),
                **{c: None for c in df_hal.columns if c not in ['Nom','Prénom','norm_first','norm_last','norm_full','__matched']},
                'source': 'Fichier'
            })
            continue

        # iterate over available hal rows (not yet matched)
        for h_idx, h_row in hal_available[hal_available['__matched'] == False].iterrows():
            h_name = h_row['norm_full']
            score = similarity_score(f_name, h_name)
            if score > best_score:
                best_score = score
                best_h_idx = h_idx

            # quick exact after normalization to favor exact equality
            if f_name == h_name:
                best_score = 100.0
                best_h_idx = h_idx
                break

        if best_score >= threshold and best_h_idx is not None:
            # merge file row and hal row
            h_row = hal_available.loc[best_h_idx]
            merged = {}

            # Start with file columns (keep orig file columns as-is)
            for col in df_file.columns:
                merged[col] = f_row.get(col)

            # Add HAL columns (only those not already present or add with prefix)
            for col in df_hal.columns:
                if col in ['Nom','Prénom','norm_first','norm_last','norm_full','__matched']:
                    continue
                # If column collides with file, prefix with HAL_
                if col in merged:
                    merged[f"HAL_{col}"] = h_row.get(col)
                else:
                    merged[col] = h_row.get(col)

            merged['source'] = 'Fichier + HAL'
            merged['match_score'] = best_score

            merged_rows.append(merged)

            # mark HAL row as matched
            hal_available.at[best_h_idx, '__matched'] = True
            used_hal_indices.add(best_h_idx)
        else:
            # No good match found: keep as file-only
            merged = {}
            for col in df_file.columns:
                merged[col] = f_row.get(col)
            # Add empty HAL columns placeholders
            for col in df_hal.columns:
                if col in ['Nom','Prénom','norm_first','norm_last','norm_full','__matched']:
                    continue
                # keep column names consistent as HAL_{col}
                merged[f"HAL_{col}"] = None
            merged['source'] = 'Fichier'
            merged['match_score'] = best_score if best_score >= 0 else None
            merged_rows.append(merged)

    # Add remaining HAL-only rows
    remaining_hal = hal_available[hal_available['__matched'] == False]
    for h_idx, h_row in remaining_hal.iterrows():
        row = {}
        # Fill file columns as None
        for col in df_file.columns:
            row[col] = None
        # Copy HAL columns
        for col in df_hal.columns:
            if col in ['Nom','Prénom','norm_first','norm_last','norm_full','__matched']:
                continue
            row[f"HAL_{col}"] = h_row.get(col)
        # Also include HAL's author name in the file name fields for clarity
        row['Nom'] = h_row.get('Nom')
        row['Prénom'] = h_row.get('Prénom')
        row['source'] = 'HAL'
        row['match_score'] = None
        merged_rows.append(row)

    # Build final DataFrame
    final_df = pd.DataFrame(merged_rows)

    # Reorder columns: original file cols, then HAL_*, then source, match_score
    file_cols = list(df_file.columns)
    # remove norm_* from output
    file_cols = [c for c in file_cols if not c.startswith('norm_')]

    hal_cols = [c for c in final_df.columns if c.startswith('HAL_')]
    other_cols = [c for c in final_df.columns if c not in file_cols + hal_cols + ['source','match_score']]

    ordered_cols = file_cols + hal_cols + other_cols + ['source','match_score']
    # Keep only existing
    ordered_cols = [c for c in ordered_cols if c in final_df.columns]

    final_df = final_df[ordered_cols]

    return final_df

# =========================================================
# INTERFACE UTILISATEUR
# =========================================================
st.title("🔗 Alignez une liste de chercheurs avec IdRef et HAL (matching flou)")
st.markdown(
    "Téléversez un fichier CSV ou Excel avec les colonnes **Nom** et **Prénom**, "
    "et saisissez une **collection HAL**. L’application recherche les correspondances "
    "dans **IdRef** et dans **HAL**, effectue un matching flou et génère un CSV enrichi."
)

uploaded_file = st.file_uploader("📁 Téléverser un fichier (.csv, .xlsx)", type=["csv", "xlsx"])
collection_code = st.text_input("🏛️ Code de la collection HAL (ex: CDMO)", "")

col1, col2 = st.columns(2)
current_year = datetime.datetime.now().year
min_birth_year = col1.number_input("Année de naissance min. (YYYY)", value=1920, min_value=1000, max_value=current_year, step=1)
min_death_year = col2.number_input("Année de décès min. (YYYY)", value=2005, min_value=1000, max_value=current_year + 5, step=1)

st.markdown("---")
st.markdown("**Paramètres de matching flou**")
col3, col4 = st.columns([2,1])
similarity_threshold = col3.slider("Seuil de similarité (%) pour considérer une correspondance", 60, 100, 85, step=1)
batch_size = col4.slider("Taille des lots HAL", 10, 50, 20)

st.markdown("---")
if USE_RAPIDFUZZ:
    st.info("fast fuzzy matching: using rapidfuzz (si installé).")
else:
    st.info("fallback fuzzy matching: using difflib (SequenceMatcher). Install 'rapidfuzz' for better results.")

if uploaded_file and collection_code:
    try:
        if uploaded_file.name.endswith(".csv"):
            data = pd.read_csv(uploaded_file)
        else:
            data = pd.read_excel(uploaded_file)

        st.success(f"✅ {len(data)} lignes chargées depuis {uploaded_file.name}.")
        st.dataframe(data.head())

        cols = data.columns.tolist()

        # heuristique pour sélectionner par défaut
        def find_default_index(candidates, cols):
            for i, col in enumerate(cols):
                if col.lower() in candidates:
                    return i
            return 0 if cols else None

        name_col_idx = find_default_index(['nom','last_name','surname','familyname'], cols)
        firstname_col_idx = find_default_index(['prénom','prenom','first_name','givenname'], cols)

        name_col = st.selectbox("Colonne Nom", options=cols, index=name_col_idx)
        firstname_col = st.selectbox("Colonne Prénom", options=cols, index=firstname_col_idx)

        if st.button("🚀 Lancer la recherche combinée IdRef + HAL", type="primary"):
            # --------------------
            # Étape 1 : IdRef
            # --------------------
            idref_results = []
            st.info("🔍 Recherche IdRef en cours...")
            progress_idref = st.progress(0)

            for idx, row in data.iterrows():
                first = row[firstname_col] if pd.notna(row[firstname_col]) else ""
                last = row[name_col] if pd.notna(row[name_col]) else ""
                full_name = f"{first} {last}".strip()
                matches = []
                if full_name:
                    matches = search_idref_for_person(full_name, min_birth_year, min_death_year)
                ppn = matches[0].get("idref") if matches else None

                idref_results.append({
                    "Nom": last,
                    "Prénom": first,
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
            hal_authors = fetch_author_details_batch(author_ids, FIELDS_LIST, batch_size=batch_size)

            # Build hal dataframe with normalized name columns
            hal_df = pd.DataFrame(hal_authors)
            # Ensure first/last name columns exist
            if 'firstName_s' not in hal_df.columns:
                hal_df['firstName_s'] = None
            if 'lastName_s' not in hal_df.columns:
                hal_df['lastName_s'] = None
            hal_df.rename(columns={
                "fullName_s": "Nom_Complet",
                "firstName_s": "Prénom",
                "lastName_s": "Nom"
            }, inplace=True)
            # Add HAL origin marker
            # Also keep orcidId_s, halId_s, valid_s if present (they will be included as HAL_... in merged)
            hal_df["source"] = "HAL"

            # --------------------
            # Étape 3 : Fusion floue
            # --------------------
            st.info("🔗 Fusion floue IdRef ↔ HAL en cours...")
            merged_df = fuzzy_merge_file_hal(idref_df[['Nom','Prénom','idref_ppn','source']], hal_df, threshold=similarity_threshold)

            st.success(f"Fusion terminée : {len(merged_df)} lignes finales.")
            st.dataframe(merged_df.head(50))

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
