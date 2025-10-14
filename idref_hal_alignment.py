# streamlit_app_idref_hal_fusion_fixed.py
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

# Try to import rapidfuzz for faster fuzzy matching (optional)
try:
    from rapidfuzz import fuzz
    USE_RAPIDFUZZ = True
except ImportError:
    USE_RAPIDFUZZ = False

# Try to detect available excel engine
EXCEL_ENGINE = None
try:
    import xlsxwriter  # noqa: F401
    EXCEL_ENGINE = "xlsxwriter"
except Exception:
    try:
        import openpyxl  # noqa: F401
        EXCEL_ENGINE = "openpyxl"
    except Exception:
        EXCEL_ENGINE = None

# =========================================================
# CONFIG
# =========================================================
st.set_page_config(page_title="Alignement IdRef ↔ HAL (corrigé)", layout="wide")

HAL_SEARCH_API = "https://api.archives-ouvertes.fr/search/"
HAL_AUTHOR_API = "https://api.archives-ouvertes.fr/ref/author/"
FIELDS_LIST = "docid,form_i,person_i,lastName_s,firstName_s,valid_s,idHal_s,halId_s,idrefId_s,orcidId_s,emailDomain_s"
REQUEST_DELAY = 0.3

# =========================================================
# UTIL
# =========================================================
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

# =========================================================
# Pydref
# =========================================================
@st.cache_resource
def get_pydref_instance():
    return Pydref()

try:
    pydref_api = get_pydref_instance()
except Exception as e:
    st.error(f"Erreur lors de l'initialisation de Pydref : {e}")
    st.stop()

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
# HAL helpers
# =========================================================
def fetch_publications_for_collection(collection_code):
    all_docs, rows, start = [], 10000, 0
    query_params = {"q": "*:*", "wt": "json", "fl": "structHasAuthId_fs", "rows": rows}
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
        or_query = " OR ".join([f'person_i:"{i}"' for i in batch])
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
# FUSION FLOUE (corrigée & stable)
# =========================================================
def fuzzy_merge_file_hal(df_file, df_hal, threshold=85):
    """
    df_file: DataFrame contenant au moins columns: Nom, Prénom, idref_*...
    df_hal: DataFrame tel que construit à partir de l'API HAL (avec lastName_s, firstName_s, etc.)
    threshold: similarité minimale (0-100)
    Retourne: DataFrame final avec colonnes idref... puis HAL_... , source, match_score
    """
    # defensive copy
    df_file = df_file.copy()
    df_hal = df_hal.copy()

    # colonnes HAL que l'on veut garder (si présentes)
    hal_keep_cols = [
        "form_i", "person_i", "lastName_s", "firstName_s", "valid_s",
        "idHal_s", "halId_s", "idrefId_s", "orcidId_s", "emailDomain_s"
    ]
    hal_keep_cols = [c for c in hal_keep_cols if c in df_hal.columns]

    # construire des colonnes Nom/Prénom sûres dans df_hal
    # priorité : lastName_s / firstName_s ; sinon colonnes 'Nom'/'Prénom' si présentes
    df_hal["HAL_lastName_source"] = df_hal.get("lastName_s", None)
    df_hal["HAL_firstName_source"] = df_hal.get("firstName_s", None)
    if "Nom" in df_hal.columns and "lastName_s" not in df_hal.columns:
        df_hal["HAL_lastName_source"] = df_hal["Nom"]
    if "Prénom" in df_hal.columns and "firstName_s" not in df_hal.columns:
        df_hal["HAL_firstName_source"] = df_hal["Prénom"]

    # normalisation pour matching
    df_file["norm_full"] = (df_file.get("Prénom", "").fillna("").apply(normalize_text) + " " +
                            df_file.get("Nom", "").fillna("").apply(normalize_text)).str.strip()

    df_hal["norm_full"] = (df_hal.get("HAL_firstName_source", "").fillna("").apply(normalize_text) + " " +
                           df_hal.get("HAL_lastName_source", "").fillna("").apply(normalize_text)).str.strip()

    df_hal["__matched"] = False

    # colonnes idref (conserver celles existantes + s'assurer Nom/Prénom présents)
    idref_cols = [
        "Nom", "Prénom", "idref_ppn", "idref_status", "nb_match",
        "match_info", "alt_names", "idref_orcid"
    ]
    # ensure Nom/Prénom kept
    idref_cols = [c for c in idref_cols if c in df_file.columns or c in ["Nom", "Prénom"]]

    # colonnes HAL préfixées
    hal_prefixed_cols = [f"HAL_{c}" for c in hal_keep_cols]
    final_cols = list(dict.fromkeys(idref_cols + hal_prefixed_cols + ["source", "match_score"]))

    # template ligne
    template = {c: None for c in final_cols}
    merged_rows = []

    # itérer sur le fichier (préférer: idref rows)
    for _, f_row in df_file.iterrows():
        r = template.copy()
        # copy idref/file columns
        for c in idref_cols:
            r[c] = f_row[c] if c in f_row.index else None

        f_name = str(f_row.get("norm_full", "")).strip()
        best_score, best_idx = -1, None

        if f_name:
            # chercher meilleur HAL non apparié
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
                r[f"HAL_{c}"] = h_row.get(c)
            r["source"] = "Fichier + HAL"
            r["match_score"] = best_score
            df_hal.at[best_idx, "__matched"] = True
        else:
            r["source"] = "Fichier"
            r["match_score"] = best_score if best_score >= 0 else None

        merged_rows.append(r)

    # maintenant ajouter HAL-only (non appariés)
    for _, h_row in df_hal[df_hal["__matched"] == False].iterrows():
        r = template.copy()
        # remplir Nom/Prénom à partir des champs HAL explicites (lastName_s / firstName_s)
        last = h_row.get("lastName_s") or h_row.get("HAL_lastName_source") or ""
        first = h_row.get("firstName_s") or h_row.get("HAL_firstName_source") or ""
        r["Nom"] = last if pd.notna(last) else (h_row.get("Nom") if "Nom" in h_row.index else None)
        r["Prénom"] = first if pd.notna(first) else (h_row.get("Prénom") if "Prénom" in h_row.index else None)
        for c in hal_keep_cols:
            r[f"HAL_{c}"] = h_row.get(c)
        r["source"] = "HAL"
        r["match_score"] = None
        merged_rows.append(r)

    final_df = pd.DataFrame(merged_rows, columns=final_cols)
    # sécurité : s'assurer aucune colonne dupliquée
    final_df = final_df.loc[:, ~final_df.columns.duplicated()]

    return final_df

# =========================================================
# EXPORT XLSX avec fallback et coloration
# =========================================================
def export_to_xlsx(fusion_df, idref_df, hal_df):
    if EXCEL_ENGINE is None:
        raise RuntimeError(
            "Aucun moteur Excel disponible. Installez 'xlsxwriter' (recommandé) ou 'openpyxl'.\n"
            "Commande: pip install xlsxwriter"
        )

    output = BytesIO()
    with pd.ExcelWriter(output, engine=EXCEL_ENGINE) as writer:
        fusion_df.to_excel(writer, sheet_name="Fusion", index=False)
        idref_df.to_excel(writer, sheet_name="IdRef", index=False)
        hal_df.to_excel(writer, sheet_name="HAL", index=False)

        # only xlsxwriter supports rich cell formats easily; openpyxl also allows styling but different API
        if EXCEL_ENGINE == "xlsxwriter":
            wb = writer.book
            header_fmt = wb.add_format({"bold": True, "bg_color": "#D9E1F2"})
            fmt_file = wb.add_format({"bg_color": "#BDD7EE"})   # bleu
            fmt_hal = wb.add_format({"bg_color": "#E2EFDA"})    # vert pâle
            fmt_both = wb.add_format({"bg_color": "#FFF2CC"})   # jaune

            ws = writer.sheets["Fusion"]
            # header + widths
            for col_num, col in enumerate(fusion_df.columns):
                ws.write(0, col_num, col, header_fmt)
                maxlen = min(50, max(10, fusion_df[col].astype(str).map(len).max() if not fusion_df[col].empty else 10))
                ws.set_column(col_num, col_num, maxlen + 2)

            # color rows by source
            if "source" in fusion_df.columns:
                for row_idx, val in enumerate(fusion_df["source"], start=1):
                    fmt = fmt_file if val == "Fichier" else fmt_hal if val == "HAL" else fmt_both
                    ws.set_row(row_idx, None, fmt)
        else:
            # minimal formatting with openpyxl: set header bold and column widths
            ws = writer.sheets["Fusion"]
            # writer.sheets are openpyxl worksheet objects in this case
            try:
                for col_idx, col in enumerate(fusion_df.columns, start=1):
                    cell = ws.cell(row=1, column=col_idx)
                    cell.font = cell.font.copy(bold=True)
                    # set column width approximate
                    maxlen = min(50, max(10, fusion_df[col].astype(str).map(len).max() if not fusion_df[col].empty else 10))
                    ws.column_dimensions[cell.column_letter].width = maxlen + 2
                # basic row coloring not applied for openpyxl here to keep code short
            except Exception:
                # if any styling error, ignore and return raw workbook
                pass

    output.seek(0)
    return output

# =========================================================
# STREAMLIT UI
# =========================================================
st.title("🔗 Alignement IdRef ↔ HAL (corrigé)")

uploaded_file = st.file_uploader("📁 Téléverser un fichier (.csv, .xlsx)", type=["csv", "xlsx"])
collection_code = st.text_input("🏛️ Code de la collection HAL (ex: CDMO)", "")

col1, col2 = st.columns(2)
current_year = datetime.datetime.now().year
min_birth_year = col1.number_input("Année de naissance min.", 1920, current_year, 1920)
min_death_year = col2.number_input("Année de décès min.", 2005, current_year + 5, 2005)

similarity_threshold = st.slider("Seuil de similarité (%)", 60, 100, 85)
batch_size = st.slider("Taille des lots HAL", 10, 50, 20)

if uploaded_file and collection_code:
    try:
        data = pd.read_csv(uploaded_file) if uploaded_file.name.endswith(".csv") else pd.read_excel(uploaded_file)
        cols = data.columns.tolist()
        name_col = st.selectbox("Colonne Nom", options=cols)
        firstname_col = st.selectbox("Colonne Prénom", options=cols)

        if st.button("🚀 Lancer la recherche combinée IdRef + HAL"):
            # IdRef phase
            idref_results = []
            progress = st.progress(0)
            for idx, row in data.iterrows():
                first = row[firstname_col] if pd.notna(row[firstname_col]) else ""
                last = row[name_col] if pd.notna(row[name_col]) else ""
                full = f"{first} {last}".strip()
                matches = []
                if full:
                    matches = search_idref_for_person(full, min_birth_year, min_death_year)
                nb = len(matches)
                idref_row = {
                    "Nom": last, "Prénom": first,
                    "idref_ppn": None, "idref_status": "not_found",
                    "nb_match": nb, "match_info": None, "alt_names": None, "idref_orcid": None
                }
                if nb > 0:
                    best = matches[0]
                    idref_row["idref_ppn"] = best.get("idref")
                    idref_row["idref_status"] = "found" if nb == 1 else "ambiguous"
                    idref_row["match_info"] = f"{best.get('last_name','')} {best.get('first_name','')}"
                    if "alt_names" in best:
                        idref_row["alt_names"] = " | ".join(best["alt_names"])
                    for ident in best.get("identifiers", []):
                        if "orcid" in ident:
                            idref_row["idref_orcid"] = ident["orcid"]
                idref_results.append(idref_row)
                progress.progress((idx + 1) / len(data))
            idref_df = pd.DataFrame(idref_results)

            # HAL phase
            st.info(f"📡 Récupération HAL pour la collection {collection_code}...")
            pubs = fetch_publications_for_collection(collection_code)
            author_ids = extract_author_ids(pubs)
            hal_authors = fetch_author_details_batch(author_ids, FIELDS_LIST, batch_size=batch_size)
            hal_df = pd.DataFrame(hal_authors)
            # Ensure these columns exist for downstream logic
            if "lastName_s" not in hal_df.columns:
                hal_df["lastName_s"] = None
            if "firstName_s" not in hal_df.columns:
                hal_df["firstName_s"] = None
            # keep original columns (they'll be used)
            # perform fuzzy merge
            merged_df = fuzzy_merge_file_hal(idref_df, hal_df, threshold=similarity_threshold)
            st.success(f"Fusion terminée : {len(merged_df)} lignes.")
            st.dataframe(merged_df.head(50))

            # Exports
            csv_output = merged_df.to_csv(index=False, sep=";", encoding="utf-8")
            st.download_button("💾 Télécharger le CSV",
                               csv_output,
                               file_name=f"fusion_idref_hal_{collection_code}_{datetime.datetime.now():%Y%m%d}.csv",
                               mime="text/csv")

            try:
                xlsx_output = export_to_xlsx(merged_df, idref_df, hal_df)
                st.download_button("📘 Télécharger le fichier Excel (XLSX)",
                                   xlsx_output,
                                   file_name=f"fusion_idref_hal_{collection_code}_{datetime.datetime.now():%Y%m%d}.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            except RuntimeError as re:
                st.warning(str(re))
                st.info("Pour activer l'export XLSX, installez 'xlsxwriter' (recommandé) ou 'openpyxl'.")
                st.code("pip install xlsxwriter")
    except Exception as e:
        st.error(f"Erreur : {e}")
