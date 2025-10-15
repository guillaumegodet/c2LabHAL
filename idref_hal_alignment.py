# streamlit_app_idref_hal_fusion_advanced.py

import streamlit as st
import pandas as pd
import requests
import datetime
import time
from urllib.parse import urlencode
from io import BytesIO
import unicodedata
from difflib import SequenceMatcher
from pydref import Pydref

try:
    from rapidfuzz import fuzz
    USE_RAPIDFUZZ = True
except ImportError:
    USE_RAPIDFUZZ = False

try:
    import xlsxwriter  # noqa
    EXCEL_ENGINE = "xlsxwriter"
except ImportError:
    try:
        import openpyxl  # noqa
        EXCEL_ENGINE = "openpyxl"
    except ImportError:
        EXCEL_ENGINE = None

# =========================================================
# CONFIGURATION
# =========================================================
st.set_page_config(page_title="Alignement IdRef ↔ HAL (Avancé)", layout="wide")

HAL_SEARCH_API = "https://api.archives-ouvertes.fr/search/"
HAL_AUTHOR_API = "https://api.archives-ouvertes.fr/ref/author/"
FIELDS_LIST = "docid,form_i,person_i,lastName_s,firstName_s,valid_s,idHal_s,halId_s,idrefId_s,orcidId_s,emailDomain_s"
REQUEST_DELAY = 0.3

# =========================================================
# UTILITAIRES
# =========================================================
def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return " ".join(s.lower().split())

def similarity_score(a, b):
    if not a and not b:
        return 100.0
    if USE_RAPIDFUZZ:
        return fuzz.QRatio(a, b)
    return SequenceMatcher(None, a, b).ratio() * 100

# =========================================================
# INITIALISATION PYDREF
# =========================================================
@st.cache_resource
def get_pydref_instance():
    return Pydref()

pydref_api = get_pydref_instance()

def search_idref_for_person(full_name, min_birth_year, min_death_year):
    try:
        return pydref_api.get_idref(
            query=full_name,
            min_birth_year=min_birth_year,
            min_death_year=min_death_year,
            is_scientific=True,
            exact_fullname=True,
        )
    except Exception as e:
        st.warning(f"Erreur IdRef pour '{full_name}': {e}")
        return []

# =========================================================
# HAL — récupération filtrée
# =========================================================
def fetch_publications_for_collection(collection_code, year_min=None, year_max=None):
    """Récupère les publications HAL d'une collection, avec filtre sur les années."""
    all_docs, rows, start = [], 10000, 0
    base_query = "*:*"
    if year_min or year_max:
        year_min = year_min or 1900
        year_max = year_max or datetime.datetime.now().year
        base_query = f"producedDateY_i:[{year_min} TO {year_max}]"

    query_params = {"q": base_query, "wt": "json", "fl": "structHasAuthId_fs", "rows": rows}
    while True:
        query_params["start"] = start
        url = f"{HAL_SEARCH_API}{collection_code}/?{urlencode(query_params)}"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()
        docs = data.get("response", {}).get("docs", [])
        all_docs.extend(docs)
        if len(docs) < rows:
            break
        start += rows
        time.sleep(REQUEST_DELAY)
    return all_docs

def extract_author_ids(publications):
    ids = set()
    for doc in publications:
        for a in doc.get("structHasAuthId_fs", []):
            parts = a.split("_JoinSep_")
            if len(parts) > 1:
                full_id = parts[1].split("_FacetSep")[0]
                docid = full_id.split("-")[-1].strip()
                if docid.isdigit() and docid != "0":
                    ids.add(docid)
    return list(ids)

def fetch_author_details_batch(author_ids, fields, batch_size=20):
    authors = []
    ids = [i.strip() for i in author_ids if i and str(i).strip()]
    total = len(ids)
    if total == 0:
        return []
    progress = st.progress(0, text="Chargement des formes-auteurs HAL...")
    for start in range(0, total, batch_size):
        batch = ids[start:start + batch_size]
        or_query = " OR ".join([f'person_i:\"{i}\"' for i in batch])
        params = {"q": or_query, "wt": "json", "fl": fields, "rows": batch_size}
        url = f"{HAL_AUTHOR_API}?{urlencode(params)}"
        try:
            r = requests.get(url)
            r.raise_for_status()
            docs = r.json().get("response", {}).get("docs", [])
            authors.extend(docs)
        except Exception as e:
            st.warning(f"⚠️ Erreur sur le lot {batch}: {e}")
        progress.progress(min((start + batch_size) / total, 1.0))
        time.sleep(REQUEST_DELAY)
    progress.empty()
    return authors

# =========================================================
# FUSION FLOUE (inchangée)
# =========================================================
def fuzzy_merge_file_hal(df_file, df_hal, threshold=85):
    hal_keep_cols = [
        "form_i", "person_i", "lastName_s", "firstName_s", "valid_s",
        "idHal_s", "halId_s", "idrefId_s", "orcidId_s", "emailDomain_s"
    ]
    hal_keep_cols = [c for c in hal_keep_cols if c in df_hal.columns]
    df_file["norm_full"] = (df_file["Prénom"].fillna("").apply(normalize_text) + " " +
                            df_file["Nom"].fillna("").apply(normalize_text)).str.strip()
    df_hal["norm_full"] = (df_hal["firstName_s"].fillna("").apply(normalize_text) + " " +
                           df_hal["lastName_s"].fillna("").apply(normalize_text)).str.strip()
    df_hal["__matched"] = False
    idref_cols = [
        "Nom", "Prénom", "idref_ppn", "idref_ppn_list", "idref_status", "nb_match",
        "match_info", "alt_names", "idref_orcid", "description_match"
    ]
    idref_cols = [c for c in idref_cols if c in df_file.columns or c in ["Nom", "Prénom"]]
    hal_prefixed_cols = [f"HAL_{c}" for c in hal_keep_cols]
    final_cols = list(dict.fromkeys(idref_cols + hal_prefixed_cols + ["source", "match_score"]))
    template = {c: None for c in final_cols}
    merged_rows = []

    for _, f_row in df_file.iterrows():
        r = template.copy()
        for c in idref_cols:
            r[c] = f_row[c] if c in f_row.index else None
        f_name = f_row.get("norm_full", "")
        best_score, best_idx = -1, None
        if f_name:
            for h_idx, h_row in df_hal[df_hal["__matched"] == False].iterrows():
                s = similarity_score(f_name, h_row["norm_full"])
                if s > best_score:
                    best_score, best_idx = s, h_idx
                if f_name == h_row["norm_full"]:
                    best_score, best_idx = 100.0, h_idx
                    break
        if best_idx is not None and best_score >= threshold:
            h_row = df_hal.loc[best_idx]
            for c in hal_keep_cols:
                r[f"HAL_{c}"] = h_row.get(c)
            r["source"], r["match_score"] = "Fichier + HAL", best_score
            df_hal.at[best_idx, "__matched"] = True
        else:
            r["source"], r["match_score"] = "Fichier", best_score if best_score >= 0 else None
        merged_rows.append(r)

    for _, h_row in df_hal[df_hal["__matched"] == False].iterrows():
        r = template.copy()
        r["Nom"], r["Prénom"] = h_row.get("lastName_s"), h_row.get("firstName_s")
        for c in hal_keep_cols:
            r[f"HAL_{c}"] = h_row.get(c)
        r["source"], r["match_score"] = "HAL", None
        merged_rows.append(r)
    return pd.DataFrame(merged_rows, columns=final_cols)

# =========================================================
# EXPORT XLSX (inchangé sauf fallback)
# =========================================================
def export_to_xlsx(fusion_df, idref_df, hal_df):
    if EXCEL_ENGINE is None:
        raise RuntimeError("Aucun moteur Excel disponible. Installez 'xlsxwriter' ou 'openpyxl'.")

    output = BytesIO()
    with pd.ExcelWriter(output, engine=EXCEL_ENGINE) as writer:
        fusion_df.to_excel(writer, sheet_name="Fusion", index=False)
        idref_df.to_excel(writer, sheet_name="IdRef", index=False)
        hal_df.to_excel(writer, sheet_name="HAL", index=False)
    output.seek(0)
    return output

# =========================================================
# INTERFACE
# =========================================================
st.title("🔗 Alignement IdRef ↔ HAL (avancé)")

uploaded_file = st.file_uploader("📁 Téléverser un fichier (.csv, .xlsx)", type=["csv", "xlsx"])
collection_code = st.text_input("🏛️ Code de la collection HAL", "")

col1, col2 = st.columns(2)
current_year = datetime.datetime.now().year
min_birth_year = col1.number_input("Année de naissance min.", 1920, current_year, 1920)
min_death_year = col2.number_input("Année de décès min.", 2005, current_year + 5, 2005)

col3, col4 = st.columns(2)
year_min = col3.number_input("Année min des publications HAL", 1900, current_year, 2015)
year_max = col4.number_input("Année max des publications HAL", 1900, current_year + 5, current_year)

similarity_threshold = st.slider("Seuil de similarité (%)", 60, 100, 85)
batch_size = st.slider("Taille des lots HAL", 10, 50, 20)

if uploaded_file and collection_code:
    data = pd.read_csv(uploaded_file) if uploaded_file.name.endswith(".csv") else pd.read_excel(uploaded_file)
    cols = data.columns.tolist()
    name_col = st.selectbox("Colonne Nom", options=cols)
    firstname_col = st.selectbox("Colonne Prénom", options=cols)
    if st.button("🚀 Lancer la recherche combinée IdRef + HAL"):
        idref_rows = []
        progress = st.progress(0, text="Recherche IdRef en cours...")
        
        for idx, row in data.iterrows():
            first, last = str(row[firstname_col]).strip(), str(row[name_col]).strip()
            full = f"{first} {last}".strip()
            matches = search_idref_for_person(full, min_birth_year, min_death_year)
            nb_match = len(matches)
            
            # Initialisation
            idref_row = {
                "Nom": last,
                "Prénom": first,
                "idref_ppn": None,
                "idref_ppn_list": None,
                "idref_status": "not_found",
                "nb_match": nb_match,
                "match_info": None,
                "alt_names": None,
                "idref_orcid": None,
                "idref_description": None,
            }
        
            if nb_match > 0:
                # --- Extraire toutes les infos des correspondances ---
                ppn_list = [m.get("idref") for m in matches if m.get("idref")]
                idref_row["idref_ppn"] = ppn_list[0] if ppn_list else None
                idref_row["idref_ppn_list"] = "|".join(ppn_list)
                idref_row["idref_status"] = "found" if nb_match == 1 else "ambiguous"
        
                # --- Infos textuelles ---
                names = [f"{m.get('first_name','')} {m.get('last_name','')}".strip() for m in matches]
                idref_row["match_info"] = "; ".join(names)
        
                # --- Descriptions issues de pydref.get_description_from_idref_notice() ---
                descs = []
                for m in matches:
                    d = m.get("description", [])
                    if isinstance(d, list):
                        descs.extend(d)
                idref_row["idref_description"] = "; ".join(descs) if descs else None
        
                # --- Noms alternatifs ---
                alts = []
                for m in matches:
                    a = m.get("alt_names", [])
                    if isinstance(a, list):
                        alts.extend(a)
                idref_row["alt_names"] = "; ".join(sorted(set(alts))) if alts else None
        
                # --- ORCID ---
                for m in matches:
                    for ident in m.get("identifiers", []):
                        if "orcid" in ident:
                            idref_row["idref_orcid"] = ident["orcid"]
        
            idref_rows.append(idref_row)
            progress.progress((idx + 1) / len(data))
        
        idref_df = pd.DataFrame(idref_rows)
        progress.empty()



        st.info(f"📡 Récupération HAL ({year_min}–{year_max}) pour {collection_code}...")
        pubs = fetch_publications_for_collection(collection_code, year_min, year_max)
        author_ids = extract_author_ids(pubs)
        hal_authors = fetch_author_details_batch(author_ids, FIELDS_LIST, batch_size)
        hal_df = pd.DataFrame(hal_authors)
        if "lastName_s" not in hal_df.columns:
            hal_df["lastName_s"] = None
        if "firstName_s" not in hal_df.columns:
            hal_df["firstName_s"] = None

        st.info("🔗 Fusion floue en cours...")
        merged_df = fuzzy_merge_file_hal(idref_df, hal_df, threshold=similarity_threshold)
        st.success(f"Fusion terminée : {len(merged_df)} lignes.")

        st.dataframe(merged_df.head(50))
        csv_output = merged_df.to_csv(index=False, sep=";", encoding="utf-8")
        st.download_button("💾 Télécharger le CSV", csv_output,
                           file_name=f"fusion_idref_hal_{collection_code}_{datetime.datetime.now():%Y%m%d}.csv",
                           mime="text/csv")

        try:
            xlsx_output = export_to_xlsx(merged_df, idref_df, hal_df)
            st.download_button("📘 Télécharger le fichier Excel (XLSX)", xlsx_output,
                               file_name=f"fusion_idref_hal_{collection_code}_{datetime.datetime.now():%Y%m%d}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except RuntimeError as re:
            st.warning(str(re))
