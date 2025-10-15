# streamlit_app_idref_hal_parallel.py
import streamlit as st
import pandas as pd
import requests
import datetime
import time
import concurrent.futures
from urllib.parse import urlencode
from io import BytesIO
import unicodedata
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
from pydref import Pydref

# Optional rapidfuzz
try:
    from rapidfuzz import fuzz
    USE_RAPIDFUZZ = True
except ImportError:
    USE_RAPIDFUZZ = False

# Excel engine
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
st.set_page_config(page_title="Alignement IdRef ↔ HAL (Parallélisé)", layout="wide")
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

# =========================
# Pydref instance
# =========================
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
# HAL helpers
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
        q = " OR ".join([f'person_i:"{x}"' for x in batch])
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
# Parallel IdRef enrichment
# =========================
def process_hal_row(row, min_birth, min_death):
    import re
    hal_first = row.get("firstName_s") or ""
    hal_last = row.get("lastName_s") or ""
    hal_full = f"{hal_first} {hal_last}".strip()
    hal_idrefs = row.get("idrefId_s")

    result = {
        "idref_ppn_list": None,
        "idref_status": "not_found",
        "nb_match": 0,
        "match_info": None,
        "alt_names": None,
        "idref_orcid": None,
        "idref_description": None,
        "idref_idhal": None,
    }

    # 1️⃣ Si HAL contient idrefId_s
    if pd.notna(hal_idrefs) and str(hal_idrefs).strip():
        ppns = re.findall(r"(\d{6,})", str(hal_idrefs))
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

    # 2️⃣ Sinon : recherche IdRef par nom
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
# Fuzzy merge (identique)
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
def export_xlsx(fusion, idref_df, hal_df, params):
    out = BytesIO()
    with pd.ExcelWriter(out, engine=EXCEL_ENGINE or "xlsxwriter") as w:
        fusion.to_excel(w, sheet_name="Fusion", index=False)
        idref_df.to_excel(w, sheet_name="extraction IdRef", index=False)
        hal_df.to_excel(w, sheet_name="extraction HAL", index=False)
        pd.DataFrame([params]).to_excel(w, sheet_name="Paramètres", index=False)
    out.seek(0)
    return out

# =========================
# INTERFACE
# =========================
st.title("🔗 Alignement IdRef ↔ HAL (Parallélisé)")
f = st.file_uploader("📁 Fichier (.csv ou .xlsx)", type=["csv","xlsx"])
coll = st.text_input("🏛️ Code collection HAL","")
c1,c2=st.columns(2)
cur=datetime.datetime.now().year
minb=c1.number_input("Année naissance min.",1900,cur,1920)
mind=c2.number_input("Année décès min.",1900,cur+5,2005)
c3,c4=st.columns(2)
ymin=c3.number_input("Année min HAL",1900,cur,2015)
ymax=c4.number_input("Année max HAL",1900,cur+5,cur)
threads=st.slider("Threads IdRef HAL",2,16,8)
if f and coll:
    df = pd.read_csv(f) if f.name.endswith(".csv") else pd.read_excel(f)
    cols = df.columns.tolist()
    col_nom = st.selectbox("Colonne Nom", cols)
    col_pre = st.selectbox("Colonne Prénom", cols)
    if st.button("🚀 Lancer l'extraction complète"):
        # Étape 1 : IdRef fichier
        rows = []
        prog = st.progress(0, text="Recherche IdRef pour fichier...")
        for i, r in df.iterrows():
            first, last = str(r[col_pre]).strip(), str(r[col_nom]).strip()
            full = f"{first} {last}".strip()
            matches = search_idref_for_person(full, minb, mind)
            nb = len(matches)
            res = {"Nom": last, "Prénom": first, "idref_ppn_list": None, "idref_status": "not_found", "nb_match": nb,
                   "match_info": None, "alt_names": None, "idref_orcid": None, "idref_description": None, "idref_idhal": None}
            if nb:
                ppns = [m.get("idref","").replace("idref","") for m in matches if m.get("idref")]
                res["idref_ppn_list"]="|".join(ppns)
                res["idref_status"]="found" if nb==1 else "ambiguous"
                res["match_info"]="; ".join([f"{m.get('first_name','')} {m.get('last_name','')}" for m in matches])
                desc,alt=[],[]
                for m in matches:
                    if isinstance(m.get("description"),list): desc+=m["description"]
                    if isinstance(m.get("alt_names"),list): alt+=m["alt_names"]
                    for ident in m.get("identifiers",[]):
                        if "orcid" in ident: res["idref_orcid"]=ident["orcid"]
                    if "idhal" in m: res["idref_idhal"]=m["idhal"]
                res["idref_description"]="; ".join(desc)
                res["alt_names"]="; ".join(sorted(set(alt)))
            rows.append(res)
            prog.progress((i+1)/len(df))
        prog.empty()
        idref_df=pd.DataFrame(rows)

        # Étape 2 : HAL
        st.info("📥 Extraction HAL...")
        pubs=fetch_publications_for_collection(coll,ymin,ymax)
        ids=extract_author_ids(pubs)
        hal_auths=fetch_author_details_batch(ids,FIELDS_LIST)
        hal_df=pd.DataFrame(hal_auths)
        if "orcidId_s" in hal_df:
            hal_df["orcidId_s"]=hal_df["orcidId_s"].astype(str).str.extract(r"(\d{4}-\d{4}-\d{4}-\d{4})")[0]
        hal_df=enrich_hal_rows_with_idref_parallel(hal_df,minb,mind,max_workers=threads)

        # Étape 3 : fusion floue
        st.info("⚙️ Fusion floue...")
        fusion=fuzzy_merge_file_hal(idref_df,hal_df,85)
        st.dataframe(fusion.head(50))
        st.success("✅ Fusion terminée")

        # Export
        csv=fusion.to_csv(index=False,sep=";",encoding="utf-8")
        st.download_button("⬇️ Télécharger CSV",csv,file_name="fusion_idref_hal.csv",mime="text/csv")
        params={"Collection":coll,"Année min HAL":ymin,"Année max HAL":ymax,"Threads":threads,"Date":datetime.datetime.now().strftime("%Y-%m-%d %H:%M")}
        try:
            xlsx=export_xlsx(fusion,idref_df,hal_df,params)
            st.download_button("⬇️ Télécharger XLSX",xlsx,file_name="fusion_idref_hal.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as e:
            st.error(str(e))
