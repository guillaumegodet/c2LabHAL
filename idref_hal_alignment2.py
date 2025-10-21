import streamlit as st
import pandas as pd
import requests
import datetime
import time
import concurrent.futures
import unicodedata
import re
from io import BytesIO
from difflib import SequenceMatcher
from pydref import Pydref
from bs4 import BeautifulSoup 
from urllib.parse import urlencode 

try:
    from rapidfuzz import fuzz
    USE_RAPIDFUZZ = True
except ImportError:
    USE_RAPIDFUZZ = False

try:
    import xlsxwriter
    EXCEL_ENGINE = "xlsxwriter"
except ImportError:
    try:
        import openpyxl
        EXCEL_ENGINE = "openpyxl"
    except ImportError:
        EXCEL_ENGINE = None 

st.set_page_config(page_title="Alignement Annuaire de chercheurs ↔ IdRef ↔ Collection HAL", layout="wide")

def add_sidebar_menu():
    st.sidebar.header("À propos")
    st.sidebar.info(
        """
        **c2LabHAL - Alignement IdRef ↔ HAL**

        Cet outil permet :
        - de rechercher des correspondances entre des auteurs d’un fichier et IdRef,
        - d’extraire les formes-auteurs associées à des structures de recherche HAL,
        - et de fusionner les deux sources.
        """
    )
    st.sidebar.markdown("---")
    st.sidebar.header("Autres applications c2LabHAL")
    st.sidebar.markdown("📖 [c2LabHAL - Application Principale](https://c2labhal.streamlit.app/)")
    st.sidebar.markdown("🏛️ [Version Nantes Université](https://c2labhal-nantes.streamlit.app/)")
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Présentation du projet :")
    st.sidebar.markdown("[📊 Voir les diapositives](https://slides.com/guillaumegodet/deck-d5bc03#/2)")
    st.sidebar.markdown("### Code source :")
    st.sidebar.markdown("[🐙 GitHub](https://github.com/GuillaumeGodet/c2labhal)")

add_sidebar_menu()

def normalize_text(s):
    if s is None:
        return ""
    s = str(s).replace("’", "'")
    s = unicodedata.normalize("NFD", s)
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

def fetch_publications_for_structures(struct_ids, year_min=None, year_max=None):
    if isinstance(struct_ids, str):
        struct_ids = [s.strip() for s in struct_ids.split(",") if s.strip()]
    if not struct_ids:
        return []
    docs = []
    rows, start = 10000, 0
    year_min = year_min or 1900
    year_max = year_max or datetime.datetime.now().year
    struct_query = " OR ".join(struct_ids)
    base_q = f"structId_i:({struct_query}) AND producedDateY_i:[{year_min} TO {year_max}]"
    params = {"q": base_q, "wt": "json", "fl": "structHasAuthId_fs", "rows": rows}
    st.info(f"🔎 Extraction des publications pour {', '.join(struct_ids)} ({year_min}-{year_max})...")
    while True:
        params["start"] = start
        r = requests.get("https://api.archives-ouvertes.fr/search/", params=params)
        r.raise_for_status()
        chunk = r.json().get("response", {}).get("docs", [])
        docs.extend(chunk)
        if len(chunk) < rows:
            break
        start += rows
        time.sleep(0.2)
    st.success(f"✅ {len(docs)} publications trouvées.")
    return docs

def extract_author_ids(docs, struct_ids=None):
    ids = set()
    if isinstance(struct_ids, str):
        struct_ids = [s.strip() for s in struct_ids.split(",") if s.strip()]
    for d in docs:
        for entry in d.get("structHasAuthId_fs", []):
            try:
                struct_part = entry.split("_FacetSep_")[0]
                if struct_ids and struct_part not in struct_ids:
                    continue
                after_join = entry.split("_JoinSep_")[1]
                form_id = after_join.split("_FacetSep_")[0]
                if form_id:
                    ids.add(form_id)
            except Exception:
                continue
    return list(sorted(ids))

def fetch_author_details_batch(ids, fields, batch_size=20):
    HAL_AUTHOR_API = "https://api.archives-ouvertes.fr/ref/author/"
    authors = []
    total = len(ids)
    prog = st.progress(0, text="📦 Téléchargement des formes-auteurs HAL...")
    for i in range(0, total, batch_size):
        batch = ids[i:i+batch_size]
        q = " OR ".join([f'docid:\"{x}\"' for x in batch]) 
        params = {"q": q, "wt": "json", "fl": fields, "rows": batch_size}
        try:
            r = requests.get(HAL_AUTHOR_API, params=params)
            r.raise_for_status()
            authors += r.json().get("response", {}).get("docs", [])
        except Exception as e:
            st.warning(f"⚠️ Erreur HAL sur le lot {batch}: {e}")
        prog.progress(min((i+batch_size)/total, 1.0))
        time.sleep(0.25)
    prog.empty()
    return authors

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
        if ppns:
            descs, alts = [], []
            orcid, idhal, match_info = None, None, None
            parsed_any = False
            for ppn in ppns:
                try:
                    xml = pydref_api.get_idref_notice(ppn)
                    if not xml: continue
                    parsed_any = True
                    soup = BeautifulSoup(xml, "lxml") 
                    ids_details = pydref_api.get_identifiers_from_idref_notice(soup)
                    for ident in ids_details:
                        if "orcid" in ident and not orcid:
                            orcid = ident["orcid"]
                        if "idhal" in ident and not idhal:
                            idhal = ident["idhal"]
                    desc = pydref_api.get_description_from_idref_notice(soup)
                    alt = pydref_api.get_alternative_names_from_idref_notice(soup)
                    nameinfo = pydref_api.get_name_from_idref_notice(soup)
                    if not match_info:
                        match_info = f"{nameinfo.get('first_name','')} {nameinfo.get('last_name','')}".strip()
                    if isinstance(desc, list): descs += desc
                    if isinstance(alt, list): alts += alt
                except Exception:
                    continue
            if parsed_any:
                result.update({
                    "idref_ppn_list": "|".join(ppns),
                    "idref_status": "found",
                    "nb_match": len(ppns),
                    "match_info": match_info,
                    "alt_names": "; ".join(sorted(set(alts))) if alts else None,
                    "idref_orcid": orcid,
                    "idref_description": "; ".join(descs) if descs else None,
                    "idref_idhal": idhal,
                })
                return result
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
                    if "orcid" in ident and not orcid: orcid = ident["orcid"]
                if "idhal" in m and not idhal: idhal = m["idhal"]
                if not match_info:
                    match_info = f"{m.get('first_name','')} {m.get('last_name','')}".strip()
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
        futures={ex.submit(process_hal_row,row,min_birth,min_death):i for i,row in hal_df.iterrows()}
        done=0
        for fut in concurrent.futures.as_completed(futures):
            i=futures[fut]
            try:results.append((i,fut.result()))
            except Exception as e:
                results.append((i,{"error_log": str(e)})) 
            done+=1
            if done%5==0 or done==total:prog.progress(done/total)
    prog.empty()
    for i,res in results:
        for k,v in res.items():hal_df.at[i,k]=v
    return hal_df

def process_file_row(row, min_birth, min_death):
    first = str(row.get("Prénom", "")).strip()
    last = str(row.get("Nom", "")).strip()
    full = f"{first} {last}".strip()
    matches = search_idref_for_person(full, min_birth, min_death)
    nb = len(matches)
    info = {c: row.get(c) for c in row.keys() if c not in ["Nom", "Prénom"]}
    info.update({"Nom": last, "Prénom": first, "idref_ppn_list": None, "idref_status": "not_found",
                "nb_match": nb, "match_info": None, "alt_names": None, "idref_orcid": None,
                "idref_description": None, "idref_idhal": None})
    if nb:
        ppns = [m.get("idref","").replace("idref","") for m in matches if m.get("idref")]
        info["idref_ppn_list"] = "|".join(ppns)
        info["idref_status"] = "found" if nb == 1 else "ambiguous"
        info["match_info"] = "; ".join([f"{m.get('first_name','')} {m.get('last_name','')}" for m in matches])
        desc, alt = [], []
        orcid, idhal = None, None
        for m in matches:
            if isinstance(m.get("description"), list): desc += m["description"]
            if isinstance(m.get("alt_names"), list): alt += m["alt_names"]
            for ident in m.get("identifiers", []):
                if "orcid" in ident and not orcid: orcid = ident["orcid"]
            if "idhal" in m and not idhal: idhal = m.get("idhal")
        info["idref_description"] = "; ".join(desc) if desc else None
        info["alt_names"] = "; ".join(sorted(set(alt))) if alt else None
        info["idref_orcid"] = orcid
        info["idref_idhal"] = idhal
    return info

def enrich_file_rows_with_idref_parallel(df, min_birth, min_death, max_workers=8):
    st.info(f"🔄 Recherche IdRef parallèle pour le fichier ({len(df)} auteurs)...")
    total = len(df)
    results = []
    prog = st.progress(0)
    rows_list = df.to_dict('records') 
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(process_file_row, row, min_birth, min_death): i for i, row in enumerate(rows_list)}
        done = 0
        for fut in concurrent.futures.as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                st.warning(f"⚠️ Erreur lors du traitement d'un auteur: {e}")
            done += 1
            if done % 5 == 0 or done == total:
                prog.progress(done / total)
    prog.empty()
    st.success("Recherche IdRef terminée ✅")
    return pd.DataFrame(results)

def fuzzy_merge_file_hal(df_file, df_hal, threshold=90):
    hal_keep = ["form_i","person_i","lastName_s","firstName_s","valid_s","idHal_s","halId_s","idrefId_s","orcidId_s","emailDomain_s",
                "idref_ppn_list", "idref_status", "nb_match", "match_info", "alt_names", "idref_orcid", "idref_description", "idref_idhal"]
    hal_keep = [c for c in hal_keep if c in df_hal.columns] 
    df_file = df_file.copy()
    df_hal = df_hal.copy()
    df_file["norm_full"] = (df_file["Prénom"].fillna("").apply(normalize_text)+" "+df_file["Nom"].fillna("").apply(normalize_text)).str.strip()
    df_hal["norm_full"] = (df_hal["firstName_s"].fillna("").apply(normalize_text)+" "+df_hal["lastName_s"].fillna("").apply(normalize_text)).str.strip()
    df_hal["__matched"] = False
    df_hal = df_hal.sort_values(by=["valid_s","idHal_s"], ascending=[False, False])
    df_hal = df_hal.drop_duplicates(subset=["norm_full"], keep="first")
    file_cols_keep = [c for c in df_file.columns if c not in ["Nom", "Prénom", "norm_full", "idref_ppn_list", "idref_status", "nb_match", "match_info", "alt_names", "idref_orcid", "idref_description", "idref_idhal"]]
    hal_pref = [f"HAL_{c}" for c in hal_keep if c not in ["idref_ppn_list","idref_status","nb_match","match_info","alt_names","idref_orcid","idref_description","idref_idhal"]]
    merged = []
    st.info("🔄 Tentative de fusion floue (Fichier → HAL)...")
    prog = st.progress(0, text="Fusion...")
    total_file = len(df_file)
    for i, fr in df_file.iterrows():
        row = {"Nom": fr.get("Nom"), "Prénom": fr.get("Prénom")}
        for c in file_cols_keep: row[c] = fr.get(c)
        idref_cols = ["idref_ppn_list","idref_status","nb_match","match_info","alt_names","idref_orcid","idref_description","idref_idhal"]
        for c in idref_cols: row[c] = fr.get(c)
        for c_hal_pref in hal_pref: row[c_hal_pref] = None
        row["source"] = "Fichier"
        row["match_score"] = None
        best_score, best_idx = -1, None
        f_norm = fr.get("norm_full","")
        if f_norm:
            for i_hal, hr in df_hal[df_hal["__matched"] == False].iterrows():
                score = similarity_score(f_norm, hr.get("norm_full",""))
                if score > best_score:
                    best_score, best_idx = score, i_hal
        if best_idx is not None and best_score >= threshold:
            h = df_hal.loc[best_idx]
            for c in hal_keep:
                if c in idref_cols:
                    if h.get("idref_ppn_list") is not None:
                        row[c] = h.get(c)
                else:
                    row[f"HAL_{c}"] = h.get(c)
            row["source"] = "Fichier + HAL"
            row["match_score"] = best_score
            df_hal.at[best_idx, "__matched"] = True
        merged.append(row)
        prog.progress(min((i+1)/total_file, 1.0))
    prog.empty()
    st.info("➕ Ajout des auteurs HAL non-appariés...")
    for _, h in df_hal[df_hal["__matched"] == False].iterrows():
        row = {c: None for c in file_cols_keep + hal_pref + ["source","match_score", "Nom", "Prénom"]}
        row["Nom"] = h.get("lastName_s") or ""
        row["Prénom"] = h.get("firstName_s") or ""
        for c in hal_keep:
            if c in idref_cols:
                row[c] = h.get(c)
            else:
                row[f"HAL_{c}"] = h.get(c)
        row["source"] = "HAL"
        row["match_score"] = None
        merged.append(row)

    CORE_REQUESTED_COLUMNS_ORDER = [
        "Nom", "Prénom", "idref_ppn_list", "HAL_idrefId_s", "HAL_idHal_s",
        "idref_idhal", "idref_orcid", "HAL_orcidId_s", "HAL_valid_s",
        "HAL_docid", "HAL_emailDomain_s", "idref_description", "source"
    ]

    file_specific_cols = [c for c in df_file.columns if c not in ["Nom", "Prénom", "norm_full", "idref_ppn_list", "idref_status", "nb_match", "match_info", "alt_names", "idref_orcid", "idref_description", "idref_idhal"]]
    final_cols_order = ["Nom", "Prénom"] + file_specific_cols + [c for c in CORE_REQUESTED_COLUMNS_ORDER if c not in ["Nom", "Prénom"]] + ["match_score"]
    df_final = pd.DataFrame(merged)
    final_cols_to_keep = [c for c in final_cols_order if c in df_final.columns]
    df_final = df_final.loc[:, final_cols_to_keep]
    df_final = df_final.loc[:, ~df_final.columns.duplicated()]

    def clean_export(df):
        df2 = df.copy()
        for c in df2.columns:
            if df2[c].dtype == object:
                df2[c] = df2[c].astype(str).replace("nan", "").replace("None", "").replace("NoneType", "")
                df2[c] = df2[c].str.replace(r"\s*;\s*", "; ", regex=True)
        return df2

    df_final = clean_export(df_final)
    return df_final.sort_values(by=["Nom", "Prénom"])


def export_xlsx(fusion, idref_df=None, hal_df=None, params=None):
    out = BytesIO()
    engine_to_use = EXCEL_ENGINE or "openpyxl"

    def clean_export(df):
        df2 = df.copy()
        for c in df2.columns:
            if df2[c].dtype == object:
                df2[c] = df2[c].astype(str).replace("nan", "").replace("None", "").replace("NoneType", "")
                df2[c] = df2[c].str.replace(r"\s*;\s*", "; ", regex=True)
        return df2

    with pd.ExcelWriter(out, engine=engine_to_use) as w:
        if isinstance(fusion, pd.DataFrame):
            clean_fusion = clean_export(fusion)
            clean_fusion.to_excel(w, sheet_name="Résultats", index=False)

        if idref_df is not None and hal_df is not None:
            # Peupler les colonnes Nom et Prénom dans HAL avant concaténation
            if "Nom" not in hal_df.columns:
                hal_df["Nom"] = hal_df.get("lastName_s", None)
            if "Prénom" not in hal_df.columns:
                hal_df["Prénom"] = hal_df.get("firstName_s", None)
        
            all_idref = pd.concat([idref_df, hal_df], ignore_index=True, sort=False)
            idref_cols = ["Nom", "Prénom", "idref_ppn_list", "idref_status", "nb_match", "match_info",
                          "alt_names", "idref_orcid", "idref_description", "idref_idhal"]
            existing = [c for c in idref_cols if c in all_idref.columns]
            all_idref = all_idref[existing].copy()
            rename_map = {}
            if "nb_match" in all_idref.columns:
                rename_map["nb_match"] = "idref_nb_match"
            if "match_info" in all_idref.columns:
                rename_map["match_info"] = "idref_match_info"
            if "alt_names" in all_idref.columns:
                rename_map["alt_names"] = "idref_alt_names"
            all_idref = all_idref.rename(columns=rename_map)
            all_idref = clean_export(all_idref)
            all_idref.to_excel(w, sheet_name="Extraction IdRef", index=False)
        elif idref_df is not None:
            idref_df_clean = clean_export(idref_df)
            idref_df_clean.to_excel(w, sheet_name="Extraction IdRef", index=False)

        if hal_df is not None:
            hal_cols = ["firstName_s", "lastName_s", "valid_s", "docid", "idHal_s",
                        "orcidId_s", "idrefId_s", "emailDomain_s"]
            existing = [c for c in hal_cols if c in hal_df.columns]
            hal_export = hal_df[existing].copy()
            rename_map = {
                "firstName_s": "HAL_firstName_s",
                "lastName_s": "HAL_lastName_s",
                "valid_s": "HAL_valid_s",
                "docid": "HAL_docid",
                "idHal_s": "HAL_idHal_s",
                "orcidId_s": "HAL_orcidId_s",
                "idrefId_s": "HAL_idrefId_s",
                "emailDomain_s": "HAL_emailDomain_s",
            }
            hal_export = hal_export.rename(columns={k: v for k, v in rename_map.items() if k in hal_export.columns})
            hal_export = clean_export(hal_export)
            hal_export.to_excel(w, sheet_name="Extraction HAL", index=False)

        if params is not None:
            pd.DataFrame([params]).to_excel(w, sheet_name="Paramètres", index=False)

    out.seek(0)
    return out

def export_csv(fusion, idref_df=None, hal_df=None, params=None):
    """Prépare un export CSV (mêmes données que l'onglet Résultats)."""
    from io import StringIO

    def clean_export(df):
        df2 = df.copy()
        for c in df2.columns:
            if df2[c].dtype == object:
                df2[c] = df2[c].astype(str).replace("nan", "").replace("None", "")
        return df2

    csv_buffers = {}
    if fusion is not None:
        clean_fusion = clean_export(fusion)
        buffer = StringIO()
        clean_fusion.to_csv(buffer, index=False, sep=";", encoding="utf-8")
        csv_buffers["Résultats"] = buffer.getvalue()

    if idref_df is not None:
        clean_idref = clean_export(idref_df)
        buffer = StringIO()
        clean_idref.to_csv(buffer, index=False, sep=";", encoding="utf-8")
        csv_buffers["Extraction_IdRef"] = buffer.getvalue()

    if hal_df is not None:
        clean_hal = clean_export(hal_df)
        buffer = StringIO()
        clean_hal.to_csv(buffer, index=False, sep=";", encoding="utf-8")
        csv_buffers["Extraction_HAL"] = buffer.getvalue()

    return csv_buffers


st.title("🔗 Alignement Annuaire de chercheurs ↔ IdRef ↔ HAL")

uploaded_file = st.file_uploader(
    '📄 Fichier auteurs. Doit contenir au moins une colonne "Nom" et une colonne "Prénom"',
    type=["csv", "xlsx"]
)
structure_ids = st.text_input(
    "🏛️ Identifiants de structures HAL (par exemple : 1088607,95668)",
    help="Identifiants HAL des structures dont vous voulez récupérer les auteurs. "
         "Utilisez AuréHAL pour le trouver. Séparez plusieurs identifiants par des virgules sans espace."
)

col_nom_choice = col_pre_choice = None
df_preview = None
if uploaded_file is not None:
    try:
        if uploaded_file.name.endswith(".csv"):
            df_preview = pd.read_csv(uploaded_file, encoding_errors='ignore')
        else:
            df_preview = pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"Erreur lors de la lecture du fichier : {e}")
        st.stop()

    st.write("Aperçu du fichier téléversé :")
    st.dataframe(df_preview.head(5))
    cols = df_preview.columns.tolist()

    def norm_col(c):
        c = unicodedata.normalize("NFD", str(c))
        return "".join(ch for ch in c if unicodedata.category(ch) != "Mn").lower()

    nom_candidates = [c for c in cols if any(k in norm_col(c) for k in ["nom", "last", "surname"])]
    pre_candidates = [c for c in cols if any(k in norm_col(c) for k in ["prenom", "first", "given"])]

    default_nom = nom_candidates[0] if nom_candidates else cols[0]
    default_pre = pre_candidates[0] if pre_candidates else (cols[1] if len(cols) > 1 else cols[0])

    st.info(f"🔍 Colonnes détectées automatiquement : **Nom → {default_nom}**, **Prénom → {default_pre}**")

    col_nom_choice = st.selectbox("Colonne NOM", options=cols, index=cols.index(default_nom))
    col_pre_choice = st.selectbox("Colonne PRÉNOM", options=cols, index=cols.index(default_pre))

minb = 1920
mind = 2005
threads = 8
similarity_threshold = 90

st.header("⚙️ Paramètres")

col3, col4 = st.columns(2)
cur = datetime.datetime.now().year
ymin = col3.number_input("Année min HAL", 1900, cur, 2015)
ymax = col4.number_input("Année max HAL", 1900, cur + 5, cur)

st.caption(f"""
Dates IdRef fixées : Naissance min **{minb}**, Décès min **{mind}**.  
Paramètres de calcul fixés : Threads **{threads}**, Seuil de similarité **{similarity_threshold}%**.
""")

if st.button("🚀 Lancer l’analyse"):
    file_provided = uploaded_file is not None and df_preview is not None
    hal_provided = bool(structure_ids.strip())

    if not file_provided and not hal_provided:
        st.warning("Veuillez fournir un fichier ou des identifiants de structures HAL.")
        st.stop()

    if file_provided and (col_nom_choice is None or col_pre_choice is None):
        st.error("Sélectionnez d'abord les colonnes Nom et Prénom.")
        st.stop()

    def clean_idref(val):
        if val is None:
            return None
        if isinstance(val, (list, tuple, set)):
            val = " ".join(map(str, val))
        try:
            if pd.isna(val):
                return None
        except Exception:
            pass
        matches = re.findall(r"([0-9]{6,}[A-ZX]?)", str(val))
        return "|".join(sorted(set(matches))) if matches else None

    def clean_orcid(val):
        if val is None:
            return None
        if isinstance(val, (list, tuple, set)):
            val = " ".join(map(str, val))
        try:
            if pd.isna(val):
                return None
        except Exception:
            pass
        match = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[0-9X])", str(val))
        return match.group(1) if match else None

    if hal_provided and not file_provided:
        st.header("🏛️ Mode 2 : Structures HAL seules")
        pubs = fetch_publications_for_structures(structure_ids, ymin, ymax)
        ids = extract_author_ids(pubs, struct_ids=structure_ids)
        hal_auths = fetch_author_details_batch(ids,
            "docid,form_i,person_i,lastName_s,firstName_s,valid_s,idHal_s,halId_s,idrefId_s,orcidId_s,emailDomain_s")
        hal_df = pd.DataFrame(hal_auths)

        if "idrefId_s" in hal_df.columns:
            hal_df["idrefId_s"] = hal_df["idrefId_s"].apply(clean_idref)
        if "orcidId_s" in hal_df.columns:
            hal_df["orcidId_s"] = hal_df["orcidId_s"].apply(clean_orcid)

        initial_count = len(hal_df)
        if "valid_s" in hal_df.columns:
            hal_df = hal_df[hal_df["valid_s"].isin(["INCOMING", "PREFERRED"])]
            st.info(f"Filtre HAL appliqué : **{len(hal_df)}** formes-auteurs (sur {initial_count} initialement) avec statut **INCOMING** ou **PREFERRED**.")

        if "lastName_s" not in hal_df.columns:
            hal_df["lastName_s"] = None
        if "firstName_s" not in hal_df.columns:
            hal_df["firstName_s"] = None

        hal_df = enrich_hal_rows_with_idref_parallel(hal_df, minb, mind, threads)
        st.success("Extraction HAL et enrichissement IdRef terminés ✅")
        st.dataframe(hal_df.head(20))
        params = {"structures": structure_ids, "year_min": ymin, "year_max": ymax}
        xlsx = export_xlsx(hal_df, hal_df=hal_df, params=params)
        st.download_button("⬇️ Télécharger XLSX", xlsx, file_name="hal_idref_structures.xlsx")
        csv_files = export_csv(hal_df, hal_df=hal_df, params=params)
        st.download_button("⬇️ Télécharger CSV (Résultats)", 
                   csv_files["Résultats"], 
                   file_name="hal_idref_structures.csv", 
                   mime="text/csv")

    elif file_provided and not hal_provided:
        st.header("🧾 Mode 1 : Fichier seul (recherche IdRef)")
        df = df_preview.copy()
        df = df.rename(columns={col_nom_choice: "Nom", col_pre_choice: "Prénom"})
        idref_df = enrich_file_rows_with_idref_parallel(df, minb, mind, threads)
        st.dataframe(idref_df.head(20))
        params = {"mode": "Fichier seul"}
        xlsx = export_xlsx(idref_df, idref_df=idref_df, params=params)
        st.download_button("⬇️ Télécharger XLSX", xlsx, file_name="idref_only.xlsx")
        csv_files = export_csv(idref_df, idref_df=idref_df, params=params)
        st.download_button("⬇️ Télécharger CSV (Résultats)", 
                           csv_files["Résultats"], 
                           file_name="idref_only_resultats.csv", 
                           mime="text/csv")
       
        
    elif file_provided and hal_provided:
        st.header("🧩 Mode 3 : Fichier + HAL (fusion complète)")
        st.info("📥 Extraction HAL + enrichissement IdRef...")
        pubs = fetch_publications_for_structures(structure_ids, ymin, ymax)
        ids = extract_author_ids(pubs, struct_ids=structure_ids)
        hal_auths = fetch_author_details_batch(ids,
            "docid,form_i,person_i,lastName_s,firstName_s,valid_s,idHal_s,halId_s,idrefId_s,orcidId_s,emailDomain_s")
        hal_df = pd.DataFrame(hal_auths)

        if "idrefId_s" in hal_df.columns:
            hal_df["idrefId_s"] = hal_df["idrefId_s"].apply(clean_idref)
        if "orcidId_s" in hal_df.columns:
            hal_df["orcidId_s"] = hal_df["orcidId_s"].apply(clean_orcid)

        initial_count = len(hal_df)
        if "valid_s" in hal_df.columns:
            hal_df = hal_df[hal_df["valid_s"].isin(["INCOMING", "PREFERRED"])]
            st.info(f"Filtre HAL appliqué : **{len(hal_df)}** formes-auteurs (sur {initial_count} initialement) avec statut **INCOMING** ou **PREFERRED**.")

        if "lastName_s" not in hal_df.columns:
            hal_df["lastName_s"] = None
        if "firstName_s" not in hal_df.columns:
            hal_df["firstName_s"] = None

        hal_df = enrich_hal_rows_with_idref_parallel(hal_df, minb, mind, threads)

        df_in = df_preview.copy()
        df_in = df_in.rename(columns={col_nom_choice: "Nom", col_pre_choice: "Prénom"})
        idref_df = enrich_file_rows_with_idref_parallel(df_in, minb, mind, threads)

        st.info("⚙️ Fusion floue...")
        fusion = fuzzy_merge_file_hal(idref_df, hal_df, threshold=similarity_threshold)
        st.dataframe(fusion.head(20))
        st.success("✅ Fusion terminée")

        params = {"structures": structure_ids, "year_min": ymin, "year_max": ymax,
                  "similarity_threshold": similarity_threshold, "threads": threads,
                  "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

        xlsx = export_xlsx(fusion, idref_df=idref_df, hal_df=hal_df, params=params)
        st.download_button("⬇️ Télécharger XLSX fusion", xlsx, file_name="fusion_idref_hal.xlsx")
        csv_files = export_csv(fusion, idref_df=idref_df, hal_df=hal_df, params=params)
        st.download_button("⬇️ Télécharger CSV (Résultats)", csv_files["Résultats"], file_name="fusion_idref_hal.csv", mime="text/csv")
    

