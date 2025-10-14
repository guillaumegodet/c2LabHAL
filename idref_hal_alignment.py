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
    Fusion floue entre fichier (IdRef) et HAL avec colonne finale fixe pour éviter doublons.
    - Renomme une éventuelle colonne 'source' dans le fichier en 'FILE_source' pour éviter conflit.
    - Préfixe toutes les colonnes HAL par 'HAL_'.
    - Retourne un DataFrame sans colonnes dupliquées.
    """
    df_file = df_file.copy()
    df_hal = df_hal.copy()

    # ----- Renommer column 'source' du fichier si elle existe (évite conflit)
    if 'source' in df_file.columns:
        df_file = df_file.rename(columns={'source': 'FILE_source'})

    # ----- Normalisation des noms
    df_file['norm_first'] = df_file.get('Prénom', '').fillna('').apply(normalize_text)
    df_file['norm_last']  = df_file.get('Nom', '').fillna('').apply(normalize_text)
    df_file['norm_full']  = (df_file['norm_first'] + ' ' + df_file['norm_last']).str.strip()

    df_hal['norm_first'] = df_hal.get('Prénom', '').fillna('').apply(normalize_text)
    df_hal['norm_last']  = df_hal.get('Nom', '').fillna('').apply(normalize_text)
    if 'Nom_Complet' in df_hal.columns:
        df_hal['norm_full'] = df_hal['Nom_Complet'].fillna('').apply(normalize_text)
    else:
        df_hal['norm_full'] = (df_hal['norm_first'] + ' ' + df_hal['norm_last']).str.strip()

    # ----- Colonnes de base (fichier) : conserver Nom/Prénom et toute colonne utile sauf norm_*
    file_cols = [c for c in df_file.columns if not c.startswith('norm_')]
    # Assurer que Nom et Prénom existent dans file_cols (même si absent dans origine)
    if 'Nom' not in file_cols:
        file_cols.insert(0, 'Nom')
    if 'Prénom' not in file_cols:
        file_cols.insert(1 if file_cols[0]=='Nom' else 0, 'Prénom')

    # ----- Colonnes HAL à ajouter (brutes), on les préfixera HAL_
    hal_exclude = {'Nom', 'Prénom', 'norm_first', 'norm_last', 'norm_full', '__matched'}
    hal_raw_cols = [c for c in df_hal.columns if c not in hal_exclude]

    hal_prefixed_cols = [f"HAL_{c}" for c in hal_raw_cols]

    # ----- Template colonne finale fixe
    final_cols = list(dict.fromkeys(file_cols + hal_prefixed_cols + ['source', 'match_score']))

    # ----- Marquer matched
    df_hal['__matched'] = False

    merged_rows = []

    # ----- Préparer un template dict rempli de None pour homogénéité
    template = {col: None for col in final_cols}

    # ----- Pour chaque ligne du fichier, chercher le meilleur match HAL
    for f_idx, f_row in df_file.iterrows():
        row_dict = template.copy()  # démarre vide, toutes clés présentes
        # Remplir colonnes fichier
        for col in file_cols:
            row_dict[col] = f_row.get(col) if col in f_row.index else None

        f_name = str(f_row.get('norm_full', '')).strip()
        best_score = -1
        best_h_idx = None

        if not f_name:
            row_dict['source'] = 'Fichier'
            row_dict['match_score'] = None
            merged_rows.append(row_dict)
            continue

        # Parcourir HAL non appariés
        for h_idx, h_row in df_hal[df_hal['__matched'] == False].iterrows():
            h_name = str(h_row.get('norm_full', '')).strip()
            score = similarity_score(f_name, h_name)
            if score > best_score:
                best_score = score
                best_h_idx = h_idx
            if f_name == h_name:  # égalité normalisée -> stop
                best_score = 100.0
                best_h_idx = h_idx
                break

        if best_score >= threshold and best_h_idx is not None:
            # Fusionner : remplir HAL_... et meta
            h_row = df_hal.loc[best_h_idx]
            for i, col in enumerate(hal_raw_cols):
                pref = f"HAL_{col}"
                row_dict[pref] = h_row.get(col)
            row_dict['source'] = 'Fichier + HAL'
            row_dict['match_score'] = best_score
            # marquer comme apparié
            df_hal.at[best_h_idx, '__matched'] = True
        else:
            # Pas de match : source Fichier, HAL_... restent None
            row_dict['source'] = 'Fichier'
            row_dict['match_score'] = best_score if best_score >= 0 else None

        merged_rows.append(row_dict)

    # ----- Ajouter HAL-only restants
    remaining = df_hal[df_hal['__matched'] == False]
    for h_idx, h_row in remaining.iterrows():
        row_dict = template.copy()
        # on met Nom/Prénom à partir de HAL (pour visibilité)
        row_dict['Nom'] = h_row.get('Nom')
        row_dict['Prénom'] = h_row.get('Prénom')
        for i, col in enumerate(hal_raw_cols):
            pref = f"HAL_{col}"
            row_dict[pref] = h_row.get(col)
        row_dict['source'] = 'HAL'
        row_dict['match_score'] = None
        merged_rows.append(row_dict)

    # ----- Création DataFrame final sans duplication possible
    final_df = pd.DataFrame(merged_rows, columns=final_cols)

    # Si on avait renommé FILE_source précédemment, on peut renommer proprement dans la sortie
    if 'FILE_source' in final_df.columns:
        # Si l'utilisateur veut conserver FILE_source, on laisse ; sinon tu peux fusionner/décider
        pass

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
