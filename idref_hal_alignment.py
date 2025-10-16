# streamlit_app_idref_hal_multimode_fixed.py
import streamlit as st
import pandas as pd
import requests
import datetime
import time
import concurrent.futures
import unicodedata
import re
from io import BytesIO
from urllib.parse import urlencode
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
from pydref import Pydref

# Optional: rapidfuzz
try:
    from rapidfuzz import fuzz
    USE_RAPIDFUZZ = True
except ImportError:
    USE_RAPIDFUZZ = False

# Excel engines
try:
    import xlsxwriter
    EXCEL_ENGINE = "xlsxwriter"
except ImportError:
    try:
        import openpyxl
        EXCEL_ENGINE = "openpyxl"
    except ImportError:
        EXCEL_ENGINE = None

# =========================
# CONFIG
# =========================
st.set_page_config(page_title="Alignement Annuaire de chercheurs ↔ IdRef ↔ Collection HAL", layout="wide")
HAL_SEARCH_API = "https://api.archives-ouvertes.fr/search/"
HAL_AUTHOR_API = "https://api.archives-ouvertes.fr/ref/author/"
FIELDS_LIST = "docid,form_i,person_i,lastName_s,firstName_s,valid_s,idHal_s,halId_s,idrefId_s,orcidId_s,emailDomain_s"

# =========================
# UTILS
# =========================
def normalize_text(s):
    if s is None:
        return ""
    s = unicodedata.normalize("NFD", str(s))
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn").lower().strip()

def similarity_score(a, b):
    if USE_RAPIDFUZZ:
        return fuzz.QRatio(a or "", b or "")
    return SequenceMatcher(None, a or "", b or "").ratio() * 100

@st.cache_resource
def get_pydref():
    return Pydref()

pydref_api = get_pydref()

def search_idref_for_person(full_name, min_birth, min_death):
    try:
        return pydref_api.get_idref(
            query=full_name,
            min_birth_year=min_birth,
            min_death_year=min_death,
            is_scientific=True,
            exact_fullname=True,
        )
    except Exception:
        return []

# =========================
# HAL HELPERS
# =========================
def fetch_publications_for_collection(coll, year_min=None, year_max=None):
    docs = []
    rows = 10000
    start = 0
    base_q = "*:*"
    if year_min or year_max:
        year_min = year_min or 1900
        year_max = year_max or datetime.datetime.now().year
        base_q = f"producedDateY_i:[{year_min} TO {year_max}]"
    params = {"q": base_q, "wt": "json", "fl": "structHasAuthId_fs", "rows": rows}
    while True:
        params["start"] = start
        r = requests.get(f"{HAL_SEARCH_API}{coll}/", params=params)
        r.raise_for_status()
        chunk = r.json().get("response", {}).get("docs", [])
        docs.extend(chunk)
        if len(chunk) < rows:
            break
        start += rows
        time.sleep(0.25)
    return docs

def extract_author_ids(docs):
    ids = set()
    for d in docs:
        for a in d.get("structHasAuthId_fs", []):
            if "_JoinSep_" in a:
                i = a.split("_JoinSep_")[1].split("_FacetSep")[0]
                num = i.split("-")[-1]
                if num.isdigit():
                    ids.add(num)
    return list(ids)

def fetch_author_details_batch(ids, fields, batch_size=20):
    authors = []
    total = len(ids)
    prog = st.progress(0, text="📦 Téléchargement des formes-auteurs HAL...")
    for i in range(0, total, batch_size):
        batch = ids[i:i+batch_size]
        q = " OR ".join([f'person_i:\"{x}\"' for x in batch])
        params = {"q": q, "wt": "json", "fl": fields, "rows": batch_size}
        try:
            r = requests.get(HAL_AUTHOR_API, params=params)
            r.raise_for_status()
            authors += r.json().get("response", {}).get("docs", [])
        except Exception as e:
            st.warning(f"⚠️ Erreur HAL pour lot {batch}: {e}")
        prog.progress(min((i+batch_size)/total, 1.0))
        time.sleep(0.2)
    prog.empty()
    return authors

# =========================
# IdRef enrichment (parallélisé pour HAL)
# =========================
def process_hal_row(row, min_birth, min_death):
    hal_first = row.get("firstName_s") or ""
    hal_last = row.get("lastName_s") or ""
    hal_full = f"{hal_first} {hal_last}".strip()
    hal_idrefs = row.get("idrefId_s")

    result = {"idref_ppn_list": None, "idref_status": "not_found", "nb_match": 0,
              "match_info": None, "alt_names": None, "idref_orcid": None,
              "idref_description": None, "idref_idhal": None}

    # Try HAL-provided idrefId_s first
    if pd.notna(hal_idrefs) and str(hal_idrefs).strip():
        ppns = re.findall(r"([0-9]{6,}[A-ZX]?)", str(hal_idrefs))
        descs, alts = [], []
        orcid, idhal, match_info = None, None, None
        parsed_any = False
        for ppn in ppns:
            try:
                xml = pydref_api.get_idref_notice(ppn)
                if not xml:
                    continue
                parsed_any = True
                soup = BeautifulSoup(xml, "lxml")
                desc = pydref_api.get_description_from_idref_notice(soup)
                alt = pydref_api.get_alternative_names_from_idref_notice(soup)
                ids = pydref_api.get_identifiers_from_idref_notice(soup)
                for ident in ids:
                    if "orcid" in ident:
                        orcid = ident["orcid"]
                    if "idhal" in ident:
                        idhal = ident["idhal"]
                nameinfo = pydref_api.get_name_from_idref_notice(soup)
                match_info = f"{nameinfo.get('first_name','')} {nameinfo.get('last_name','')}".strip()
                if isinstance(desc, list):
                    descs += desc
                if isinstance(alt, list):
                    alts += alt
            except Exception:
                continue
        if parsed_any:
            result.update({
                "idref_ppn_list": "|".join(ppns) if ppns else None,
                "idref_status": "found",
                "nb_match": len(ppns) if ppns else 1,
                "match_info": match_info,
                "alt_names": "; ".join(sorted(set(alts))) if alts else None,
                "idref_orcid": orcid,
                "idref_description": "; ".join(descs) if descs else None,
                "idref_idhal": idhal,
            })
            return result

    # Fallback: search by name in IdRef
    if hal_full:
        matches = search_idref_for_person(hal_full, min_birth, min_death)
        nb = len(matches)
        if nb > 0:
            ppns = [m.get("idref","").replace("idref","") for m in matches if m.get("idref")]
            descs, alts = [], []
            orcid, idhal, match_info = None, None, None
            for m in matches:
                if isinstance(m.get("description"), list):
                    descs += m["description"]
                if isinstance(m.get("alt_names"), list):
                    alts += m["alt_names"]
                for ident in m.get("identifiers", []):
                    if "orcid" in ident:
                        orcid = ident["orcid"]
                if not match_info:
                    match_info = f"{m.get('first_name','')} {m.get('last_name','')}".strip()
                if "idhal" in m:
                    idhal = m["idhal"]
            result.update({
                "idref_ppn_list": "|".join(ppns) if ppns else None,
                "idref_status": "found" if nb == 1 else "ambiguous",
                "nb_match": nb,
                "match_info": match_info,
                "alt_names": "; ".join(sorted(set(alts))) if alts else None,
                "idref_orcid": orcid,
                "idref_description": "; ".join(descs) if descs else None,
                "idref_idhal": idhal,
            })
    return result

def enrich_hal_rows_with_idref_parallel(hal_df, min_birth, min_death, max_workers=8):
    hal_df = hal_df.copy()
    st.info(f"🔄 Enrichissement IdRef parallèle ({len(hal_df)} auteurs HAL)...")
    total = len(hal_df)
    results = []
    prog = st.progress(0)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(process_hal_row, row, min_birth, min_death): idx for idx, row in hal_df.iterrows()}
        done = 0
        for fut in concurrent.futures.as_completed(futures):
            idx = futures[fut]
            try:
                results.append((idx, fut.result()))
            except Exception:
                results.append((idx, {}))
            done += 1
            if done % 5 == 0 or done == total:
                prog.progress(done / total)
    prog.empty()
    for idx, res in results:
        for k, v in res.items():
            hal_df.at[idx, k] = v
    return hal_df

# =========================
# Fuzzy merge
# =========================
def fuzzy_merge_file_hal(df_file, df_hal, threshold=85):
    hal_keep = ["form_i","person_i","lastName_s","firstName_s","valid_s","idHal_s","halId_s","idrefId_s","orcidId_s","emailDomain_s"]
    hal_keep = [c for c in hal_keep if c in df_hal.columns]
    df_file = df_file.copy()
    df_hal = df_hal.copy()
    df_file["norm_full"] = (df_file["Prénom"].fillna("").apply(normalize_text)+" "+df_file["Nom"].fillna("").apply(normalize_text)).str.strip()
    df_hal["norm_full"] = (df_hal["firstName_s"].fillna("").apply(normalize_text)+" "+df_hal["lastName_s"].fillna("").apply(normalize_text)).str.strip()
    df_hal["__matched"] = False
    cols_out = ["Nom","Prénom","idref_ppn_list","idref_status","nb_match","match_info","alt_names","idref_orcid","idref_description","idref_idhal"]
    hal_pref = [f"HAL_{c}" for c in hal_keep]
    merged = []

    # merge file -> hal
    for _, fr in df_file.iterrows():
        row = {**{c: fr.get(c) for c in cols_out}, **{c: None for c in hal_pref}, "source": "Fichier", "match_score": None}
        best_score, best_idx = -1, None
        f_norm = fr.get("norm_full","")
        if f_norm:
            for i, hr in df_hal[df_hal["__matched"] == False].iterrows():
                score = similarity_score(f_norm, hr.get("norm_full",""))
                if score > best_score:
                    best_score, best_idx = score, i
        if best_idx is not None and best_score >= threshold:
            h = df_hal.loc[best_idx]
            for c in hal_keep:
                row[f"HAL_{c}"] = h.get(c)
            row["source"] = "Fichier + HAL"
            row["match_score"] = best_score
            df_hal.at[best_idx, "__matched"] = True
        merged.append(row)

    # add remaining HAL-only rows, ensure Nom/Prénom filled
    for _, h in df_hal[df_hal["__matched"] == False].iterrows():
        row = {c: None for c in cols_out + hal_pref + ["source","match_score"]}
        row["Nom"] = h.get("lastName_s") or h.get("Nom") or ""
        row["Prénom"] = h.get("firstName_s") or h.get("Prénom") or ""
        for c in cols_out:
            row[c] = h.get(c)
        for c in hal_keep:
            row[f"HAL_{c}"] = h.get(c)
        row["source"] = "HAL"
        row["match_score"] = None
        merged.append(row)

    df_final = pd.DataFrame(merged)
    df_final = df_final.loc[:, ~df_final.columns.duplicated()]
    return df_final

# =========================
# EXPORT XLSX
# =========================
def export_xlsx(fusion, idref_df=None, hal_df=None, params=None):
    out = BytesIO()
    with pd.ExcelWriter(out, engine=EXCEL_ENGINE or "xlsxwriter") as w:
        fusion.to_excel(w, sheet_name="Résultats", index=False)
        if idref_df is not None:
            idref_df.to_excel(w, sheet_name="extraction IdRef", index=False)
        if hal_df is not None:
            hal_df.to_excel(w, sheet_name="extraction HAL", index=False)
        if params is not None:
            pd.DataFrame([params]).to_excel(w, sheet_name="Paramètres", index=False)
    out.seek(0)
    return out

# =========================
# INTERFACE
# =========================
def add_sidebar_menu():
    st.sidebar.header("À Propos")
    st.sidebar.info(
        """
        **c2LabHAL - Version CSV** :
        Cet outil permet de comparer une liste de publications (fournie via un fichier CSV contenant au minimum les colonnes 'doi' et 'Title')
        avec une collection HAL spécifique. Il enrichit également les données avec Unpaywall et les permissions de dépôt.
        """
    )
    st.sidebar.markdown("---")
   
    st.sidebar.header("Autres applications c2LabHAL")
    st.sidebar.markdown("📖 [c2LabHAL - Application Principale](https://c2labhal.streamlit.app/)")
    st.sidebar.markdown("🏛️ [c2LabHAL version Nantes Université](https://c2labhal-nantes.streamlit.app/)")
    st.sidebar.markdown("🔗 [Alignez une liste de chercheurs avec IdRef et HAL](https://c2labhal-idref-hal-alignment.streamlit.app/)")


    st.sidebar.markdown("---")
   
    st.sidebar.markdown("Présentation du projet :")
    st.sidebar.markdown("[📊 Voir les diapositives](https://slides.com/guillaumegodet/deck-d5bc03#/2)")
    st.sidebar.markdown("Code source :")
    st.sidebar.markdown("[🐙 Voir sur GitHub](https://github.com/GuillaumeGodet/c2labhal)")
    
    st.title("🔗 Alignement Annuaire de chercheurs ↔ IdRef ↔ Collection HAL")

uploaded_file = st.file_uploader("📄 Fichier auteurs (facultatif)", type=["csv","xlsx"])
col1, col2 = st.columns(2)
current_year = datetime.datetime.now().year
min_birth = col1.number_input("Année naissance min. (IdRef)", 1900, current_year, 1920)
min_death = col2.number_input("Année décès min. (IdRef)", 1900, current_year + 5, 2005)



# default params
col_nom_choice = None
col_pre_choice = None

# If a file was uploaded, read it immediately and show column selectors
if uploaded_file is not None:
    try:
        df_preview = pd.read_csv(uploaded_file) if uploaded_file.name.endswith(".csv") else pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"Erreur lors de la lecture du fichier : {e}")
        st.stop()
    st.write("Aperçu du fichier téléversé :")
    st.dataframe(df_preview.head(5))
    cols = df_preview.columns.tolist()
    col_nom_choice = st.selectbox("Sélectionner la colonne contenant le NOM", options=cols)
    col_pre_choice = st.selectbox("Sélectionner la colonne contenant le PRÉNOM", options=cols)

collection_code = st.text_input("🏛️ Code collection HAL (facultatif)")

col3, col4 = st.columns(2)
year_min = col3.number_input("Année min des publications HAL", 1900, current_year, 2015)
year_max = col4.number_input("Année max des publications HAL", 1900, current_year + 5, current_year)

threads = st.slider("Threads IdRef HAL (pour enrichissement)", 2, 16, 8)
similarity_threshold = st.slider("Seuil similarité (%) pour fusion", 60, 100, 85)

if st.button("🚀 Lancer l’analyse"):
    file_provided = uploaded_file is not None
    hal_provided = bool(collection_code.strip())

    if not file_provided and not hal_provided:
        st.warning("Veuillez fournir un fichier ou un code de collection HAL.")
        st.stop()

    # --- Mode 1: FICHIER SEUL ---
    if file_provided and not hal_provided:
        st.header("🧾 Mode 1 : Fichier seul (recherche IdRef)")
        if col_nom_choice is None or col_pre_choice is None:
            st.error("Sélectionnez d'abord les colonnes Nom et Prénom (après upload).")
            st.stop()
        df_in = df_preview  # already read above
        res = []
        prog = st.progress(0, text="Recherche IdRef pour le fichier...")
        for i, r in df_in.iterrows():
            first = str(r[col_pre_choice]).strip()
            last = str(r[col_nom_choice]).strip()
            full = f"{first} {last}".strip()
            matches = search_idref_for_person(full, min_birth, min_death)
            nb = len(matches)
            info = {"Nom": last, "Prénom": first, "idref_ppn_list": None, "idref_status": "not_found",
                    "nb_match": nb, "match_info": None, "alt_names": None, "idref_orcid": None,
                    "idref_description": None, "idref_idhal": None}
            if nb:
                ppns = [m.get("idref","").replace("idref","") for m in matches if m.get("idref")]
                info["idref_ppn_list"] = "|".join(ppns)
                info["idref_status"] = "found" if nb == 1 else "ambiguous"
                info["match_info"] = "; ".join([f"{m.get('first_name','')} {m.get('last_name','')}" for m in matches])
                desc, alt = [], []
                for m in matches:
                    if isinstance(m.get("description"), list):
                        desc += m["description"]
                    if isinstance(m.get("alt_names"), list):
                        alt += m["alt_names"]
                    for ident in m.get("identifiers", []):
                        if "orcid" in ident:
                            info["idref_orcid"] = ident["orcid"]
                    if "idhal" in m:
                        info["idref_idhal"] = m.get("idhal")
                info["idref_description"] = "; ".join(desc) if desc else None
                info["alt_names"] = "; ".join(sorted(set(alt))) if alt else None
            res.append(info)
            prog.progress((i+1)/len(df_in))
        prog.empty()
        idref_df = pd.DataFrame(res)
        st.dataframe(idref_df.head(50))
        csv = idref_df.to_csv(index=False, sep=";", encoding="utf-8")
        st.download_button("⬇️ Télécharger CSV IdRef", csv, file_name="idref_result.csv", mime="text/csv")

    # --- Mode 2: HAL SEUL ---
    elif hal_provided and not file_provided:
        st.header("🏛️ Mode 2 : Collection HAL seule")
        st.info("Récupération des auteurs HAL et enrichissement IdRef...")
        pubs = fetch_publications_for_collection(collection_code, year_min, year_max)
        ids = extract_author_ids(pubs)
        hal_auths = fetch_author_details_batch(ids, FIELDS_LIST)
        hal_df = pd.DataFrame(hal_auths)

        # Nettoyage idrefId_s -> extraire PPN(s)
        if "idrefId_s" in hal_df.columns:
            def extract_idref_ppn(val):
                if pd.isna(val):
                    return None
                s = str(val)
                ids_found = re.findall(r"([0-9]{6,}[A-ZX]?)", s)
                return "|".join(sorted(set(ids_found))) if ids_found else None
            hal_df["idrefId_s"] = hal_df["idrefId_s"].apply(extract_idref_ppn)

        # Nettoyage orcid
        if "orcidId_s" in hal_df.columns:
            hal_df["orcidId_s"] = hal_df["orcidId_s"].astype(str).str.extract(r"(\d{4}-\d{4}-\d{4}-\d{4})")[0]

        # Ensure name columns exist
        if "lastName_s" not in hal_df.columns:
            hal_df["lastName_s"] = None
        if "firstName_s" not in hal_df.columns:
            hal_df["firstName_s"] = None

        # Enrich HAL rows with IdRef info in parallel
        hal_df = enrich_hal_rows_with_idref_parallel(hal_df, min_birth, min_death, max_workers=threads)
        st.success("Extraction HAL et enrichissement IdRef terminés ✅")
        st.dataframe(hal_df.head(50))

        # Export HAL enriched
        xlsx = export_xlsx(hal_df, idref_df=None, hal_df=hal_df, params={"collection": collection_code, "year_min": year_min, "year_max": year_max})
        st.download_button("⬇️ Télécharger XLSX HAL", xlsx, file_name="hal_idref.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # --- Mode 3: FICHIER + HAL (fusion) ---
    elif file_provided and hal_provided:
        st.header("🧩 Mode 3 : Fichier + HAL (fusion complète)")
        if col_nom_choice is None or col_pre_choice is None:
            st.error("Sélectionnez d'abord les colonnes Nom et Prénom (après upload).")
            st.stop()
        df_in = df_preview

        st.info("📥 Extraction HAL + enrichissement IdRef...")
        pubs = fetch_publications_for_collection(collection_code, year_min, year_max)
        ids = extract_author_ids(pubs)
        hal_auths = fetch_author_details_batch(ids, FIELDS_LIST)
        hal_df = pd.DataFrame(hal_auths)

        # Nettoyage idrefId_s et orcidId_s
        if "idrefId_s" in hal_df.columns:
            hal_df["idrefId_s"] = hal_df["idrefId_s"].astype(str).apply(lambda v: ("|".join(re.findall(r'([0-9]{6,}[A-ZX]?)', v)) if pd.notna(v) else None))
        if "orcidId_s" in hal_df.columns:
            hal_df["orcidId_s"] = hal_df["orcidId_s"].astype(str).str.extract(r"(\d{4}-\d{4}-\d{4}-\d{4})")[0]

        if "lastName_s" not in hal_df.columns:
            hal_df["lastName_s"] = None
        if "firstName_s" not in hal_df.columns:
            hal_df["firstName_s"] = None

        # Enrich HAL with IdRef (parallel)
        hal_df = enrich_hal_rows_with_idref_parallel(hal_df, min_birth, min_death, max_workers=threads)

        # IdRef enrichment for file
        st.info("🔍 Recherche IdRef sur fichier...")
        res = []
        prog = st.progress(0, text="Recherche IdRef pour le fichier...")
        for i, r in df_in.iterrows():
            first = str(r[col_pre_choice]).strip()
            last = str(r[col_nom_choice]).strip()
            full = f"{first} {last}".strip()
            matches = search_idref_for_person(full, min_birth, min_death)
            nb = len(matches)
            info = {"Nom": last, "Prénom": first, "idref_ppn_list": None, "idref_status": "not_found",
                    "nb_match": nb, "match_info": None, "alt_names": None, "idref_orcid": None,
                    "idref_description": None, "idref_idhal": None}
            if nb:
                ppns = [m.get("idref","").replace("idref","") for m in matches if m.get("idref")]
                info["idref_ppn_list"] = "|".join(ppns)
                info["idref_status"] = "found" if nb == 1 else "ambiguous"
                info["match_info"] = "; ".join([f"{m.get('first_name','')} {m.get('last_name','')}" for m in matches])
                desc, alt = [], []
                for m in matches:
                    if isinstance(m.get("description"), list):
                        desc += m["description"]
                    if isinstance(m.get("alt_names"), list):
                        alt += m["alt_names"]
                    for ident in m.get("identifiers", []):
                        if "orcid" in ident:
                            info["idref_orcid"] = ident["orcid"]
                    if "idhal" in m:
                        info["idref_idhal"] = m.get("idhal")
                info["idref_description"] = "; ".join(desc) if desc else None
                info["alt_names"] = "; ".join(sorted(set(alt))) if alt else None
            res.append(info)
            prog.progress((i+1)/len(df_in))
        prog.empty()
        idref_df = pd.DataFrame(res)

        # Fusion floue
        st.info("⚙️ Fusion...")
        fusion = fuzzy_merge_file_hal(idref_df, hal_df, threshold=similarity_threshold)
        st.dataframe(fusion.head(50))
        st.success("✅ Fusion terminée")

        # Export
        params = {"collection": collection_code, "year_min": year_min, "year_max": year_max,
                  "similarity_threshold": similarity_threshold, "threads": threads,
                  "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        xlsx = export_xlsx(fusion, idref_df=idref_df, hal_df=hal_df, params=params)
        st.download_button("⬇️ Télécharger XLSX (fusion)", xlsx, file_name="fusion_idref_hal.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
