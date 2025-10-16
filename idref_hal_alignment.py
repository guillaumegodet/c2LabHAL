# streamlit_app_idref_hal_multimode.py
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
st.set_page_config(page_title="Alignement IdRef ↔ HAL (multi-mode)", layout="wide")
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

    if pd.notna(hal_idrefs) and str(hal_idrefs).strip():
        ppns = re.findall(r"([0-9]{6,}[A-ZX]?)", str(hal_idrefs))
        for ppn in ppns:
            try:
                xml = pydref_api.get_idref_notice(ppn)
                if not xml:
                    continue
                soup = BeautifulSoup(xml, "lxml")
                desc = pydref_api.get_description_from_idref_notice(soup)
                alt = pydref_api.get_alternative_names_from_idref_notice(soup)
                ids = pydref_api.get_identifiers_from_idref_notice(soup)
                orcid, idhal = None, None
                for ident in ids:
                    if "orcid" in ident:
                        orcid = ident["orcid"]
                    if "idhal" in ident:
                        idhal = ident["idhal"]
                nameinfo = pydref_api.get_name_from_idref_notice(soup)
                match_info = f"{nameinfo.get('first_name','')} {nameinfo.get('last_name','')}".strip()
                result.update({
                    "idref_ppn_list": "|".join(ppns),
                    "idref_status": "found",
                    "nb_match": len(ppns),
                    "match_info": match_info,
                    "alt_names": "; ".join(sorted(set(alt))) if alt else None,
                    "idref_orcid": orcid,
                    "idref_description": "; ".join(desc) if desc else None,
                    "idref_idhal": idhal,
                })
                return result
            except Exception:
                continue

    if hal_full:
        matches = search_idref_for_person(hal_full, min_birth, min_death)
        nb = len(matches)
        if nb > 0:
            ppns = [m.get("idref","").replace("idref","") for m in matches if m.get("idref")]
            descs, alts = [], []
            orcid, idhal, match_info = None, None, None
            for m in matches:
                if isinstance(m.get("description"), list): descs += m["description"]
                if isinstance(m.get("alt_names"), list): alts += m["alt_names"]
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
    df_file["norm_full"] = (df_file["Prénom"].fillna("").apply(normalize_text)+" "+df_file["Nom"].fillna("").apply(normalize_text)).str.strip()
    df_hal["norm_full"] = (df_hal["firstName_s"].fillna("").apply(normalize_text)+" "+df_hal["lastName_s"].fillna("").apply(normalize_text)).str.strip()
    df_hal["__matched"] = False
    cols_out = ["Nom","Prénom","idref_ppn_list","idref_status","nb_match","match_info","alt_names","idref_orcid","idref_description","idref_idhal"]
    hal_pref = [f"HAL_{c}" for c in hal_keep]
    merged = []
    for _, fr in df_file.iterrows():
        row = {**{c: fr.get(c) for c in cols_out}, **{c: None for c in hal_pref}, "source": "Fichier", "match_score": None}
        best_score, best_idx = -1, None
        f_norm = fr["norm_full"]
        for i, hr in df_hal[df_hal["__matched"] == False].iterrows():
            score = similarity_score(f_norm, hr["norm_full"])
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
    # HAL-only
    for _, h in df_hal[df_hal["__matched"] == False].iterrows():
        row = {"Nom": h.get("lastName_s"), "Prénom": h.get("firstName_s")}
        for c in cols_out:
            row[c] = h.get(c)
        for c in hal_keep:
            row[f"HAL_{c}"] = h.get(c)
        row["source"] = "HAL"
        row["match_score"] = None
        merged.append(row)
    return pd.DataFrame(merged)

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
st.title("🔗 Alignement IdRef ↔ HAL — Multi-mode")

uploaded = st.file_uploader("📄 Fichier auteurs (facultatif)", type=["csv","xlsx"])
collection = st.text_input("🏛️ Code collection HAL (facultatif)")

col1,col2 = st.columns(2)
cur = datetime.datetime.now().year
minb = col1.number_input("Année naissance min.",1900,cur,1920)
mind = col2.number_input("Année décès min.",1900,cur+5,2005)
col3,col4 = st.columns(2)
ymin = col3.number_input("Année min HAL",1900,cur,2015)
ymax = col4.number_input("Année max HAL",1900,cur+5,cur)
threads = st.slider("Threads IdRef HAL",2,16,8)

# Détermination du scénario
if st.button("🚀 Lancer l’analyse"):
    file_provided = uploaded is not None
    hal_provided = bool(collection.strip())

    if not file_provided and not hal_provided:
        st.warning("Veuillez fournir un fichier ou un code de collection HAL.")
        st.stop()

    # === CAS 1 : FICHIER SEUL ===
    if file_provided and not hal_provided:
        st.header("🧾 Mode 1 : Fichier seul (recherche IdRef)")
        df = pd.read_csv(uploaded) if uploaded.name.endswith(".csv") else pd.read_excel(uploaded)
        cols = df.columns.tolist()
        col_nom = st.selectbox("Colonne Nom", cols)
        col_pre = st.selectbox("Colonne Prénom", cols)
        if st.button("🔍 Lancer la recherche IdRef"):
            res = []
            prog = st.progress(0)
            for i, r in df.iterrows():
                first, last = str(r[col_pre]).strip(), str(r[col_nom]).strip()
                full = f"{first} {last}".strip()
                matches = search_idref_for_person(full, minb, mind)
                nb = len(matches)
                info = {"Nom": last,"Prénom": first,"idref_ppn_list": None,"idref_status": "not_found","nb_match": nb,
                        "match_info": None,"alt_names": None,"idref_orcid": None,"idref_description": None,"idref_idhal": None}
                if nb:
                    ppns = [m.get("idref","").replace("idref","") for m in matches if m.get("idref")]
                    info["idref_ppn_list"]="|".join(ppns)
                    info["idref_status"]="found" if nb==1 else "ambiguous"
                res.append(info)
                prog.progress((i+1)/len(df))
            prog.empty()
            idref_df = pd.DataFrame(res)
            st.dataframe(idref_df.head(50))
            csv = idref_df.to_csv(index=False,sep=";",encoding="utf-8")
            st.download_button("⬇️ Télécharger CSV", csv, file_name="idref_result.csv", mime="text/csv")

    # === CAS 2 : HAL SEUL ===
    elif hal_provided and not file_provided:
        st.header("🏛️ Mode 2 : Collection HAL seule")
        st.info("Récupération des auteurs HAL et enrichissement IdRef...")
        pubs = fetch_publications_for_collection(collection, ymin, ymax)
        ids = extract_author_ids(pubs)
        hal_auths = fetch_author_details_batch(ids, FIELDS_LIST)
        hal_df = pd.DataFrame(hal_auths)

        # Nettoyage idrefId_s
        if "idrefId_s" in hal_df:
            def extract_idref_ppn(val):
                if pd.isna(val): return None
                s = str(val)
                ids = re.findall(r"([0-9]{6,}[A-ZX]?)", s)
                return "|".join(sorted(set(ids))) if ids else None
            hal_df["idrefId_s"] = hal_df["idrefId_s"].apply(extract_idref_ppn)

        if "orcidId_s" in hal_df:
            hal_df["orcidId_s"] = hal_df["orcidId_s"].astype(str).str.extract(r"(\d{4}-\d{4}-\d{4}-\d{4})")[0]

        hal_df = enrich_hal_rows_with_idref_parallel(hal_df, minb, mind, max_workers=threads)
        st.success("Extraction HAL et enrichissement IdRef terminés ✅")
        st.dataframe(hal_df.head(50))
        xlsx = export_xlsx(hal_df, hal_df=hal_df)
        st.download_button("⬇️ Télécharger XLSX", xlsx, file_name="hal_idref.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # === CAS 3 : FICHIER + HAL ===
    elif file_provided and hal_provided:
        st.header("🧩 Mode 3 : Fichier + HAL (fusion complète)")
        df = pd.read_csv(uploaded) if uploaded.name.endswith(".csv") else pd.read_excel(uploaded)
        cols = df.columns.tolist()
        col_nom = st.selectbox("Colonne Nom", cols)
        col_pre = st.selectbox("Colonne Prénom", cols)

        st.info("📥 Extraction HAL + enrichissement IdRef...")
        pubs = fetch_publications_for_collection(collection, ymin, ymax)
        ids = extract_author_ids(pubs)
        hal_auths = fetch_author_details_batch(ids, FIELDS_LIST)
        hal_df = pd.DataFrame(hal_auths)
        # Nettoyage
        if "idrefId_s" in hal_df:
            hal_df["idrefId_s"] = hal_df["idrefId_s"].astype(str).str.extract(r"([0-9]{6,}[A-ZX]?)")[0]
        if "orcidId_s" in hal_df:
            hal_df["orcidId_s"] = hal_df["orcidId_s"].astype(str).str.extract(r"(\d{4}-\d{4}-\d{4}-\d{4})")[0]
        hal_df = enrich_hal_rows_with_idref_parallel(hal_df, minb, mind, max_workers=threads)

        # IdRef pour fichier
        st.info("🔍 Recherche IdRef sur fichier...")
        res = []
        for _, r in df.iterrows():
            first, last = str(r[col_pre]).strip(), str(r[col_nom]).strip()
            full = f"{first} {last}".strip()
            matches = search_idref_for_person(full, minb, mind)
            nb = len(matches)
            info = {"Nom": last,"Prénom": first,"idref_ppn_list": None,"idref_status": "not_found","nb_match": nb,
                    "match_info": None,"alt_names": None,"idref_orcid": None,"idref_description": None,"idref_idhal": None}
            if nb:
                ppns = [m.get("idref","").replace("idref","") for m in matches if m.get("idref")]
                info["idref_ppn_list"]="|".join(ppns)
                info["idref_status"]="found" if nb==1 else "ambiguous"
            res.append(info)
        idref_df = pd.DataFrame(res)

        st.info("⚙️ Fusion floue...")
        fusion = fuzzy_merge_file_hal(idref_df, hal_df, 85)
        st.dataframe(fusion.head(50))
        st.success("✅ Fusion terminée")

        xlsx = export_xlsx(fusion, idref_df=idref_df, hal_df=hal_df)
        st.download_button("⬇️ Télécharger XLSX", xlsx,
                           file_name="fusion_idref_hal.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
