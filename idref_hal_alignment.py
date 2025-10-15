# streamlit_app_idref_hal_with_hal_idref_parallel.py
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

# Optional: rapidfuzz for faster fuzzy matching
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

# =============================
# CONFIG
# =============================
st.set_page_config(page_title="Alignement IdRef ↔ HAL (Parallélisé)", layout="wide")
HAL_SEARCH_API = "https://api.archives-ouvertes.fr/search/"
HAL_AUTHOR_API = "https://api.archives-ouvertes.fr/ref/author/"
FIELDS_LIST = "docid,form_i,person_i,lastName_s,firstName_s,valid_s,idHal_s,halId_s,idrefId_s,orcidId_s,emailDomain_s"

# =============================
# UTIL
# =============================
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

# =============================
# PYDREF INSTANCE
# =============================
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
    except Exception:
        return []

# =============================
# HAL HELPERS
# =============================
def fetch_publications_for_collection(collection_code, year_min=None, year_max=None):
    """Récupère les publications HAL d'une collection, avec filtre sur les années."""
    all_docs, rows, start = [], 10000, 0
    base_query = "*:*"
    if year_min or year_max:
        year_min = year_min or 1900
        year_max = year_max or datetime.datetime.now().year
        base_query = f"producedDateY_i:[{year_min} TO {year_max}]"

    params = {"q": base_query, "wt": "json", "fl": "structHasAuthId_fs", "rows": rows}
    while True:
        params["start"] = start
        url = f"{HAL_SEARCH_API}{collection_code}/?{urlencode(params)}"
        r = requests.get(url)
        r.raise_for_status()
        docs = r.json().get("response", {}).get("docs", [])
        all_docs.extend(docs)
        if len(docs) < rows:
            break
        start += rows
        time.sleep(0.2)
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
    ids = [i.strip() for i in author_ids if i]
    total = len(ids)
    if total == 0:
        return []
    prog = st.progress(0, text="Téléchargement des formes-auteurs HAL...")
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
            st.warning(f"⚠️ Erreur lot {batch}: {e}")
        prog.progress(min((start + batch_size) / total, 1.0))
        time.sleep(0.2)
    prog.empty()
    return authors

# =============================
# PARALLEL ENRICHMENT FUNCTION
# =============================
def process_hal_row(row, min_birth_year, min_death_year):
    """Fonction exécutée en parallèle pour enrichir une ligne HAL avec IdRef"""
    import re
    from bs4 import BeautifulSoup

    hal_first = row.get("firstName_s") or row.get("Prénom") or ""
    hal_last = row.get("lastName_s") or row.get("Nom") or ""
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

    found_ppns, descs, alts = [], [], []
    idref_orcid, idref_idhal, match_info = None, None, None

    # 1️⃣ Si idrefId_s présent dans HAL
    if pd.notna(hal_idrefs) and str(hal_idrefs).strip() not in ["None", "nan", "[]", ""]:
        s = str(hal_idrefs)
        ppns = re.findall(r"(\d{6,})", s)
        for ppn in ppns:
            try:
                xml = pydref_api.get_idref_notice(ppn)
                if not xml:
                    continue
                soup = BeautifulSoup(xml, "lxml")
                desc = pydref_api.get_description_from_idref_notice(soup)
                alt = pydref_api.get_alternative_names_from_idref_notice(soup)
                ids = pydref_api.get_identifiers_from_idref_notice(soup)
                for ident in ids:
                    if "orcid" in ident:
                        idref_orcid = ident["orcid"]
                    if "idhal" in ident:
                        idref_idhal = ident["idhal"]
                nameinfo = pydref_api.get_name_from_idref_notice(soup)
                match_info = f"{nameinfo.get('first_name','')} {nameinfo.get('last_name','')}".strip()
                descs.extend(desc)
                alts.extend(alt)
                found_ppns.append(ppn)
            except Exception:
                continue
        if found_ppns:
            result.update({
                "idref_ppn_list": "|".join(found_ppns),
                "idref_status": "found",
                "nb_match": len(found_ppns),
                "match_info": match_info,
                "alt_names": "; ".join(sorted(set(alts))) if alts else None,
                "idref_orcid": idref_orcid,
                "idref_description": "; ".join(descs) if descs else None,
                "idref_idhal": idref_idhal,
            })
            return result

    # 2️⃣ Sinon : recherche par nom/prénom
    if hal_full:
        matches = search_idref_for_person(hal_full, min_birth_year, min_death_year)
        nb = len(matches)
        if
