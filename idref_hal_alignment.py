# streamlit_app_idref_hal_with_hal_idref.py
import streamlit as st
import pandas as pd
import requests
import datetime
import time
from urllib.parse import urlencode
from io import BytesIO
import unicodedata
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
from pydref import Pydref

# Optional: rapidfuzz for faster/more robust fuzzy matching
try:
    from rapidfuzz import fuzz
    USE_RAPIDFUZZ = True
except ImportError:
    USE_RAPIDFUZZ = False

# Excel engine detection
try:
    import xlsxwriter  # noqa F401
    EXCEL_ENGINE = "xlsxwriter"
except Exception:
    try:
        import openpyxl  # noqa F401
        EXCEL_ENGINE = "openpyxl"
    except Exception:
        EXCEL_ENGINE = None

# ---------------------
# Configuration
# ---------------------
st.set_page_config(page_title="IdRef ↔ HAL (HAL-provided IdRef enrichment)", layout="wide")
HAL_SEARCH_API = "https://api.archives-ouvertes.fr/search/"
HAL_AUTHOR_API = "https://api.archives-ouvertes.fr/ref/author/"
# fields to request from HAL author ref API
FIELDS_LIST = "docid,form_i,person_i,lastName_s,firstName_s,valid_s,idHal_s,halId_s,idrefId_s,orcidId_s,emailDomain_s"
REQUEST_DELAY = 0.25  # seconds between calls (tweak as needed)

# ---------------------
# Utilities
# ---------------------
def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return " ".join(s.lower().split())

def similarity_score(a, b):
    if (not a) and (not b):
        return 100.0
    if USE_RAPIDFUZZ:
        return fuzz.QRatio(a, b)
    return SequenceMatcher(None, a, b).ratio() * 100

# ---------------------
# Pydref instance
# ---------------------
@st.cache_resource
def get_pydref_instance():
    return Pydref()

pydref_api = get_pydref_instance()

def search_idref_for_person(full_name, min_birth_year, min_death_year):
    """Wrapper to call pydref.get_idref with given constraints."""
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

# ---------------------
# HAL helpers
# ---------------------
def fetch_publications_for_collection(collection_code, year_min=None, year_max=None):
    """Fetch publications for collection, optionally filtered by producedDateY_i range."""
    all_docs = []
    rows = 10000
    start = 0
    base_q = "*:*"
    if year_min is not None or year_max is not None:
        year_min = year_min or 1900
        year_max = year_max or datetime.datetime.now().year
        base_q = f"producedDateY_i:[{year_min} TO {year_max}]"

    query_params = {"q": base_q, "wt": "json", "fl": "structHasAuthId_fs", "rows": rows}
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
                full = parts[1].split("_FacetSep")[0]
                docid = full.split("-")[-1].strip()
                if docid.isdigit() and docid != "0":
                    ids.add(docid)
    return list(ids)

def fetch_author_details_batch(author_ids, fields, batch_size=20):
    authors = []
    clean_ids = [i.strip() for i in author_ids if i and str(i).strip()]
    total = len(clean_ids)
    if total == 0:
        return []

    progress = st.progress(0, text="Récupération des formes-auteurs HAL (par lots)...")
    for start in range(0, total, batch_size):
        batch = clean_ids[start:start + batch_size]
        or_query = " OR ".join([f'person_i:"{i}"' for i in batch])
        params = {"q": or_query, "wt": "json", "fl": fields, "rows": batch_size}
        url = f"{HAL_AUTHOR_API}?{urlencode(params)}"
        try:
            r = requests.get(url)
            r.raise_for_status()
            docs = r.json().get("response", {}).get("docs", [])
            authors.extend(docs)
        except Exception as e:
            st.warning(f"Erreur pour lot {batch}: {e}")
        progress.progress(min((start + batch_size) / total, 1.0))
        time.sleep(REQUEST_DELAY)
    progress.empty()
    return authors

# ---------------------
# IdRef enrichment for HAL authors
# ---------------------
def enrich_hal_rows_with_idref(hal_df, min_birth_year, min_death_year):
    """
    For each HAL author row:
     - if hal row has idrefId_s, use that ppn to fetch the IdRef notice and parse description/identifiers etc.
     - otherwise query IdRef by name (search_idref_for_person) and fill fields (same structure as idref_df).
    Returns hal_df with added columns:
      idref_ppn_list, idref_status, nb_match, match_info, alt_names, idref_orcid, idref_description, idref_idhal
    """
    # initialize columns
    hal_df = hal_df.copy()
    hal_df["idref_ppn_list"] = None
    hal_df["idref_status"] = None
    hal_df["nb_match"] = 0
    hal_df["match_info"] = None
    hal_df["alt_names"] = None
    hal_df["idref_orcid"] = None
    hal_df["idref_description"] = None
    hal_df["idref_idhal"] = None

    total = len(hal_df)
    prog = st.progress(0, text="Enrichissement IdRef des auteurs HAL...")
    for idx, row in hal_df.iterrows():
        # prefer HAL-provided idrefId_s
        found_ppns = []
        descriptions = []
        alt_names = []
        idref_orcid = None
        idref_idhal = None
        status = "not_found"
        nb = 0
        match_info = None

        # normalize name for possible search fallbacks
        hal_first = row.get("firstName_s") or row.get("Prénom") or ""
        hal_last = row.get("lastName_s") or row.get("Nom") or ""
        hal_full = f"{hal_first} {hal_last}".strip()

        # If HAL contains idrefId_s, prefer it
        hal_idrefs = row.get("idrefId_s")
        if pd.notna(hal_idrefs) and hal_idrefs not in [None, "None", "nan", "[]", ""]:
            # hal_idrefs may be a list-like string or a single string; try to extract ppn(s)
            if isinstance(hal_idrefs, list):
                ppns = []
                for x in hal_idrefs:
                    if isinstance(x, str):
                        ppns.append(x.replace("idref", "").strip())
            else:
                # cast to str and extract all sequences of digits after optional 'idref'
                s = str(hal_idrefs)
                # e.g. "['idref085354562']" or "idref085354562"
                import re
                ppns = re.findall(r"idref\s*:?\\?\'?\"?0*?(\d+)|(\d{6,})", s)
                # ppns is list of tuples from regex groups; flatten
                flat = []
                for t in ppns:
                    if isinstance(t, tuple):
                        flat.extend([g for g in t if g])
                    else:
                        flat.append(t)
                ppn_list_clean = [p for p in flat if p]
                if not ppn_list_clean:
                    # fallback: extract digits generally
                    ppn_list_clean = re.findall(r"(\d{6,})", s)
                ppns = ppn_list_clean
            # For each ppn, fetch the idref notice and parse
            parsed_any = False
            for ppn in ppns:
                try:
                    xml = pydref_api.get_idref_notice(ppn)
                    if not xml:
                        continue
                    soup = BeautifulSoup(xml, "lxml")
                    parsed_any = True
                    # description
                    descs = pydref_api.get_description_from_idref_notice(soup)
                    if descs:
                        descriptions.extend(descs)
                    # alt names
                    alt = pydref_api.get_alternative_names_from_idref_notice(soup)
                    if alt:
                        alt_names.extend(alt)
                    # identifiers (to find orcid)
                    idents = pydref_api.get_identifiers_from_idref_notice(soup)
                    for ident in idents:
                        if "orcid" in ident:
                            idref_orcid = ident["orcid"]
                    # name
                    nameinfo = pydref_api.get_name_from_idref_notice(soup)
                    match_info = f"{nameinfo.get('first_name','')} {nameinfo.get('last_name','')}".strip()
                    idref_idhal = None
                    # the pydref parser doesn't expose an 'idhal' field by default; if present in identifiers, capture
                    for ident in idents:
                        if "idhal" in ident:
                            idref_idhal = ident.get("idhal")
                    found_ppns.append(str(ppn))
                except Exception as e:
                    # non-fatal
                    continue
            if parsed_any:
                status = "found"
                nb = len(found_ppns) if found_ppns else 1
        else:
            # no HAL-provided idref: query IdRef by name
            if hal_full:
                matches = search_idref_for_person(hal_full, min_birth_year, min_death_year)
                nb = len(matches)
                if nb > 0:
                    status = "found" if nb == 1 else "ambiguous"
                    # collect ppns (remove idref prefix)
                    for m in matches:
                        if m.get("idref"):
                            found_ppns.append(m.get("idref").replace("idref", ""))
                    # descriptions, alt_names, orcid
                    for m in matches:
                        d = m.get("description", [])
                        if isinstance(d, list):
                            descriptions.extend(d)
                        a = m.get("alt_names", [])
                        if isinstance(a, list):
                            alt_names.extend(a)
                        for ident in m.get("identifiers", []):
                            if "orcid" in ident:
                                idref_orcid = ident["orcid"]
                        # name info
                        if not match_info:
                            match_info = f"{m.get('first_name','')} {m.get('last_name','')}".strip()
        # assign back to hal_df
        hal_df.at[idx, "idref_ppn_list"] = "|".join(found_ppns) if found_ppns else None
        hal_df.at[idx, "idref_status"] = status
        hal_df.at[idx, "nb_match"] = nb
        hal_df.at[idx, "match_info"] = match_info
        hal_df.at[idx, "alt_names"] = "; ".join(sorted(set(alt_names))) if alt_names else None
        hal_df.at[idx, "idref_orcid"] = idref_orcid
        hal_df.at[idx, "idref_description"] = "; ".join(descriptions) if descriptions else None
        hal_df.at[idx, "idref_idhal"] = idref_idhal
        prog.progress((idx + 1) / total)
        # small delay to be polite
        time.sleep(REQUEST_DELAY / 2)
    prog.empty()
    return hal_df

# ---------------------
# Fuzzy merge (uses idref fields in both dataframes)
# ---------------------
def fuzzy_merge_file_hal(df_file, df_hal, threshold=85):
    # hal_keep cols (we'll prefix them)
    hal_keep_cols = [
        "form_i", "person_i", "lastName_s", "firstName_s", "valid_s",
        "idHal_s", "halId_s", "idrefId_s", "orcidId_s", "emailDomain_s"
    ]
    hal_keep_cols = [c for c in hal_keep_cols if c in df_hal.columns]

    # prepare normalized full names
    df_file = df_file.copy()
    df_hal = df_hal.copy()
    df_file["norm_full"] = (df_file.get("Prénom", "").fillna("").apply(normalize_text) + " " +
                            df_file.get("Nom", "").fillna("").apply(normalize_text)).str.strip()
    df_hal["norm_full"] = (df_hal.get("firstName_s", "").fillna("").apply(normalize_text) + " " +
                           df_hal.get("lastName_s", "").fillna("").apply(normalize_text)).str.strip()
    df_hal["__matched"] = df_hal.get("__matched", False)

    # idref columns to carry from file/hal (ensure presence)
    idref_cols = [
        "Nom", "Prénom", "idref_ppn_list", "idref_status", "nb_match",
        "match_info", "alt_names", "idref_orcid", "idref_description", "idref_idhal"
    ]
    idref_cols = [c for c in idref_cols if c in df_file.columns or c in ["Nom", "Prénom"]]

    hal_prefixed_cols = [f"HAL_{c}" for c in hal_keep_cols]
    final_cols = list(dict.fromkeys(idref_cols + hal_prefixed_cols + ["source", "match_score"]))

    template = {c: None for c in final_cols}
    merged_rows = []

    # iterate over file rows
    for _, f_row in df_file.iterrows():
        row = template.copy()
        for c in idref_cols:
            row[c] = f_row[c] if c in f_row.index else None
        f_name = f_row.get("norm_full", "")
        best_score, best_idx = -1, None
        if f_name:
            for h_idx, h_row in df_hal[df_hal["__matched"] == False].iterrows():
                s = similarity_score(f_name, h_row.get("norm_full", ""))
                if s > best_score:
                    best_score, best_idx = s, h_idx
                if f_name == h_row.get("norm_full", ""):
                    best_score, best_idx = 100.0, h_idx
                    break
        if best_idx is not None and best_score >= threshold:
            h_row = df_hal.loc[best_idx]
            for c in hal_keep_cols:
                row[f"HAL_{c}"] = h_row.get(c)
            # also copy hal idref fields into prefixed columns if present
            # (we intentionally leave idref_* top-level columns as coming from file or hal merged)
            row["source"] = "Fichier + HAL"
            row["match_score"] = best_score
            df_hal.at[best_idx, "__matched"] = True
        else:
            row["source"] = "Fichier"
            row["match_score"] = best_score if best_score >= 0 else None
        merged_rows.append(row)

    # add remaining HAL-only records
    for _, h_row in df_hal[df_hal.get("__matched", False) == False].iterrows():
        row = template.copy()
        # fill name
        row["Nom"] = h_row.get("lastName_s") or h_row.get("Nom")
        row["Prénom"] = h_row.get("firstName_s") or h_row.get("Prénom")
        # fill hal fields
        for c in hal_keep_cols:
            row[f"HAL_{c}"] = h_row.get(c)
        # fill idref fields from HAL enrichment if present
        for col in ["idref_ppn_list", "idref_status", "nb_match", "match_info", "alt_names", "idref_orcid", "idref_description", "idref_idhal"]:
            if col in h_row.index:
                row[col] = h_row.get(col)
        row["source"] = "HAL"
        row["match_score"] = None
        merged_rows.append(row)

    final_df = pd.DataFrame(merged_rows, columns=final_cols)
    final_df = final_df.loc[:, ~final_df.columns.duplicated()]
    return final_df

# ---------------------
# Excel export with renamed sheets and coloring
# ---------------------
def export_to_xlsx(fusion_df, idref_df, hal_df, params_info):
    if EXCEL_ENGINE is None:
        raise RuntimeError("Aucun moteur Excel disponible. Installez 'xlsxwriter' (recommandé) ou 'openpyxl'.")

    output = BytesIO()
    with pd.ExcelWriter(output, engine=EXCEL_ENGINE) as writer:
        fusion_df.to_excel(writer, sheet_name="Fusion", index=False)
        idref_df.to_excel(writer, sheet_name="extraction IdRef", index=False)
        hal_df.to_excel(writer, sheet_name="extraction HAL", index=False)
        pd.DataFrame([params_info]).to_excel(writer, sheet_name="Paramètres", index=False)

        if EXCEL_ENGINE == "xlsxwriter":
            wb = writer.book
            hdr = wb.add_format({"bold": True, "bg_color": "#D9E1F2"})
            fmt_file = wb.add_format({"bg_color": "#BDD7EE"})
            fmt_hal = wb.add_format({"bg_color": "#E2EFDA"})
            fmt_both = wb.add_format({"bg_color": "#FFF2CC"})
            ws = writer.sheets["Fusion"]
            for col_num, col in enumerate(fusion_df.columns):
                ws.write(0, col_num, col, hdr)
                maxlen = min(50, max(10, fusion_df[col].astype(str).map(len).max() if not fusion_df[col].empty else 10))
                ws.set_column(col_num, col_num, maxlen + 2)
            if "source" in fusion_df.columns:
                for row_idx, val in enumerate(fusion_df["source"], start=1):
                    fmt = fmt_file if val == "Fichier" else fmt_hal if val == "HAL" else fmt_both
                    ws.set_row(row_idx, None, fmt)
        else:
            # basic header formatting with openpyxl (best-effort)
            ws = writer.sheets["Fusion"]
            try:
                for col_idx, col in enumerate(fusion_df.columns, start=1):
                    cell = ws.cell(row=1, column=col_idx)
                    cell.font = cell.font.copy(bold=True)
                    maxlen = min(50, max(10, fusion_df[col].astype(str).map(len).max() if not fusion_df[col].empty else 10))
                    ws.column_dimensions[cell.column_letter].width = maxlen + 2
            except Exception:
                pass

    output.seek(0)
    return output

# ---------------------
# Streamlit UI
# ---------------------
st.title("🔗 Alignement IdRef ↔ HAL — enrichissement IdRef pour auteurs HAL")

uploaded_file = st.file_uploader("Téléverser un fichier (.csv, .xlsx)", type=["csv", "xlsx"])
collection_code = st.text_input("Code de la collection HAL (ex: CDMO)", "")

col1, col2 = st.columns(2)
current_year = datetime.datetime.now().year
min_birth_year = col1.number_input("Année naissance min. (IdRef)", 1900, current_year, 1920)
min_death_year = col2.number_input("Année décès min. (IdRef)", 1900, current_year + 5, 2005)

col3, col4 = st.columns(2)
year_min = col3.number_input("Année min des publications HAL", 1900, current_year, 2015)
year_max = col4.number_input("Année max des publications HAL", 1900, current_year + 5, current_year)

similarity_threshold = st.slider("Seuil de similarité (%)", 60, 100, 85)
batch_size = st.slider("Taille des lots HAL", 10, 50, 20)

if uploaded_file and collection_code:
    # load uploaded file
    data = pd.read_csv(uploaded_file) if uploaded_file.name.endswith(".csv") else pd.read_excel(uploaded_file)
    cols = data.columns.tolist()
    name_col = st.selectbox("Colonne Nom", options=cols)
    firstname_col = st.selectbox("Colonne Prénom", options=cols)

    if st.button("Lancer l'extraction et la fusion"):
        # Step 1: IdRef enrichment for uploaded file
        idref_rows = []
        prog = st.progress(0, text="Recherche IdRef pour le fichier...")
        for idx, row in data.iterrows():
            first = str(row[firstname_col]).strip() if pd.notna(row[firstname_col]) else ""
            last = str(row[name_col]).strip() if pd.notna(row[name_col]) else ""
            full = f"{first} {last}".strip()
            matches = []
            if full:
                matches = search_idref_for_person(full, min_birth_year, min_death_year)
            nb = len(matches)
            idref_row = {
                "Nom": last, "Prénom": first,
                "idref_ppn_list": None,
                "idref_status": "not_found",
                "nb_match": nb,
                "match_info": None,
                "alt_names": None,
                "idref_orcid": None,
                "idref_description": None,
                "idref_idhal": None,
            }
            if nb > 0:
                ppn_list = [m.get("idref", "").replace("idref", "") for m in matches if m.get("idref")]
                idref_row["idref_ppn_list"] = "|".join(ppn_list)
                idref_row["idref_status"] = "found" if nb == 1 else "ambiguous"
                names = [f"{m.get('first_name','')} {m.get('last_name','')}".strip() for m in matches]
                idref_row["match_info"] = "; ".join(names)
                descs = []
                alts = []
                for m in matches:
                    d = m.get("description", [])
                    if isinstance(d, list):
                        descs.extend(d)
                    a = m.get("alt_names", [])
                    if isinstance(a, list):
                        alts.extend(a)
                    for ident in m.get("identifiers", []):
                        if "orcid" in ident:
                            idref_row["idref_orcid"] = ident["orcid"]
                    if "idhal" in m:
                        idref_row["idref_idhal"] = m.get("idhal")
                idref_row["idref_description"] = "; ".join(descs) if descs else None
                idref_row["alt_names"] = "; ".join(sorted(set(alts))) if alts else None
            idref_rows.append(idref_row)
            prog.progress((idx + 1) / len(data))
        prog.empty()
        idref_df = pd.DataFrame(idref_rows)

        # Step 2: HAL extraction (filtered by years)
        st.info(f"Récupération des formes-auteurs HAL pour la collection {collection_code} ({year_min}–{year_max})")
        pubs = fetch_publications_for_collection(collection_code, year_min, year_max)
        author_ids = extract_author_ids(pubs)
        hal_authors = fetch_author_details_batch(author_ids, FIELDS_LIST, batch_size=batch_size)
        hal_df = pd.DataFrame(hal_authors)

        # normalize ORCID from HAL (extract pure code if present)
        if "orcidId_s" in hal_df.columns:
            hal_df["orcidId_s"] = hal_df["orcidId_s"].astype(str).str.extract(r"(\d{4}-\d{4}-\d{4}-\d{4})")[0]

        # ensure name fields exist
        if "lastName_s" not in hal_df.columns:
            hal_df["lastName_s"] = None
        if "firstName_s" not in hal_df.columns:
            hal_df["firstName_s"] = None

        # Step 2.5: Enrich HAL rows with IdRef info (either from HAL-provided idrefId_s or by querying IdRef by name)
        hal_df = enrich_hal_rows_with_idref(hal_df, min_birth_year, min_death_year)

        # Step 3: Fuzzy merge file idref_df with enriched hal_df
        st.info("Fusion floue des données...")
        merged_df = fuzzy_merge_file_hal(idref_df, hal_df, threshold=similarity_threshold)
        st.success(f"Fusion terminée — {len(merged_df)} lignes obtenues.")
        st.dataframe(merged_df.head(50))

        # Step 4: Export CSV + XLSX
        csv_output = merged_df.to_csv(index=False, sep=";", encoding="utf-8")
        st.download_button("Télécharger CSV", csv_output,
                           file_name=f"fusion_idref_hal_{collection_code}_{datetime.datetime.now():%Y%m%d}.csv",
                           mime="text/csv")

        params_info = {
            "Collection HAL": collection_code,
            "Year min (HAL)": year_min,
            "Year max (HAL)": year_max,
            "IdRef birth min": min_birth_year,
            "IdRef death min": min_death_year,
            "Similarity threshold": similarity_threshold,
            "Batch size HAL": batch_size,
            "Extraction date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Engine excel": EXCEL_ENGINE or "none"
        }

        try:
            xlsx_bytes = export_to_xlsx(merged_df, idref_df, hal_df, params_info)
            st.download_button("Télécharger XLSX (multi-feuilles)", xlsx_bytes,
                               file_name=f"fusion_idref_hal_{collection_code}_{datetime.datetime.now():%Y%m%d}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except RuntimeError as re:
            st.warning(str(re))
            st.info("Installez xlsxwriter: pip install xlsxwriter")
