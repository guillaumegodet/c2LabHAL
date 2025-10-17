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
from urllib.parse import urlencode 
from bs4 import BeautifulSoup 

# ----- optional fuzzy match -----
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

# ===== CONFIG =====
st.set_page_config(page_title="Alignement Annuaire de chercheurs ↔ IdRef ↔ Collection HAL", layout="wide")

# ===== SIDEBAR =====
def add_sidebar_menu():
    st.sidebar.header("À propos")
    st.sidebar.info(
        """
        **c2LabHAL - Alignement IdRef ↔ HAL**

        Cet outil permet :
        - de rechercher des correspondances entre des auteurs d’un fichier et IdRef,
        - d’extraire les formes-auteurs associées à des structures de recherche HAL,
        - et de fusionner les deux sources.

        Il peut être utilisé :
        - avec un fichier seul,
        - avec une collection HAL seule,
        - ou en combinant les deux.
        """
    )
    st.sidebar.markdown("---")
    st.sidebar.header("Autres applications c2LabHAL")
    st.sidebar.markdown("📖 [c2LabHAL - Application Principale](https://c2labhal.streamlit.app/)")
    st.sidebar.markdown("🏛️ [Version Nantes Université](https://c2labhal-nantes.streamlit.app/)")
    st.sidebar.markdown("🔗 [Alignement IdRef ↔ HAL](https://c2labhal-idref-hal-alignment.streamlit.app/)")
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Présentation du projet :")
    st.sidebar.markdown("[📊 Voir les diapositives](https://slides.com/guillaumegodet/deck-d5bc03#/2)")
    st.sidebar.markdown("### Code source :")
    st.sidebar.markdown("[🐙 GitHub](https://github.com/GuillaumeGodet/c2labhal)")

add_sidebar_menu()

# ===== UTILS =====
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

# ===== HAL FUNCTIONS =====
def fetch_publications_for_structures(struct_ids, year_min=None, year_max=None):
    """Récupère les publications liées à une ou plusieurs structures HAL (structId_i)."""
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
                person_block = after_join.split("_FacetSep_")[0]
                # ex : 414751-863157 → 863157
                person_i = person_block.split("-")[-1]
                if person_i.isdigit():
                    ids.add(person_i)
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
        q = " OR ".join([f'person_i:\"{x}\"' for x in batch])
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

# ===== IDREF enrichissement pour HAL (Advanced logic) =====
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
        # Clean idrefIds_s (which might be a string list/array)
        ppns = re.findall(r"([0-9]{6,}[A-ZX]?)", str(hal_idrefs))
        descs, alts = [], []
        orcid, idhal, match_info = None, None, None
        parsed_any = False
        
        # Try to enrich using the PPN from the HAL record by fetching the full notice
        for ppn in ppns:
            try:
                # pydref_api.get_idref_notice only returns XML, need BeautifulSoup
                xml = pydref_api.get_idref_notice(ppn)
                if not xml:
                    continue
                parsed_any = True
                soup = BeautifulSoup(xml, "lxml")
                
                # Extract details from XML notice
                desc = pydref_api.get_description_from_idref_notice(soup)
                alt = pydref_api.get_alternative_names_from_idref_notice(soup)
                ids = pydref_api.get_identifiers_from_idref_notice(soup)
                
                for ident in ids:
                    if "orcid" in ident and not orcid: # Keep first ORCID found
                        orcid = ident["orcid"]
                    if "idhal" in ident and not idhal: # Keep first IdHAL found
                        idhal = ident["idhal"]
                
                nameinfo = pydref_api.get_name_from_idref_notice(soup)
                if not match_info: # Keep name from first notice
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

    # Fallback: search by name in IdRef (if no PPN or PPN enrichment failed)
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
                    if "orcid" in ident and not orcid:
                        orcid = ident["orcid"]
                if not match_info:
                    match_info = f"{m.get('first_name','')} {m.get('last_name','')}".strip()
                if "idhal" in m and not idhal:
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
        futures={ex.submit(process_hal_row,row,min_birth,min_death):i for i,row in hal_df.iterrows()}
        done=0
        for fut in concurrent.futures.as_completed(futures):
            i=futures[fut]
            try:results.append((i,fut.result()))
            except Exception as e:
                # Log the error in the result for debugging if needed, but for now just empty dict
                results.append((i,{})) 
            done+=1
            if done%5==0 or done==total:prog.progress(done/total)
    prog.empty()
    for i,res in results:
        for k,v in res.items():hal_df.at[i,k]=v
    return hal_df

# ===== FUZZY MERGE (MODIFIED COLUMN SELECTION LOGIC) =====
def fuzzy_merge_file_hal(df_file, df_hal, threshold=85):
    """
    Fait une fusion floue des auteurs du fichier (enrichi IdRef) avec les auteurs HAL (enrichi IdRef).
    """
    hal_keep = ["form_i","person_i","lastName_s","firstName_s","valid_s","idHal_s","halId_s","idrefId_s","orcidId_s","emailDomain_s",
                "idref_ppn_list", "idref_status", "nb_match", "match_info", "alt_names", "idref_orcid", "idref_description", "idref_idhal"]
    # Keep only columns present in hal_df (especially important for enrichment columns)
    hal_keep = [c for c in hal_keep if c in df_hal.columns]
    
    df_file = df_file.copy()
    df_hal = df_hal.copy()
    
    # Use full name normalization for similarity calculation
    df_file["norm_full"] = (df_file["Prénom"].fillna("").apply(normalize_text)+" "+df_file["Nom"].fillna("").apply(normalize_text)).str.strip()
    df_hal["norm_full"] = (df_hal["firstName_s"].fillna("").apply(normalize_text)+" "+df_hal["lastName_s"].fillna("").apply(normalize_text)).str.strip()
    df_hal["__matched"] = False
    
    # Columns from file/IdRef enrichment that are not Nom/Prénom
    file_cols_keep = [c for c in df_file.columns if c not in ["Nom", "Prénom", "norm_full"] and not c.startswith("idref_")]
    
    # Add HAL columns with prefix
    hal_pref = [f"HAL_{c}" for c in hal_keep if c not in ["idref_ppn_list","idref_status","nb_match","match_info","alt_names","idref_orcid","idref_description","idref_idhal"]]
    
    merged = []

    # 1. Merge file rows with the best matching, unmatched HAL row
    st.info("🔄 Tentative de fusion floue (Fichier → HAL)...")
    prog = st.progress(0, text="Fusion...")
    total_file = len(df_file)
    for i, fr in df_file.iterrows():
        # Initialize row with file data
        row = {"Nom": fr.get("Nom"), "Prénom": fr.get("Prénom")}
        for c in file_cols_keep:
            row[c] = fr.get(c)

        # Add IdRef enrichment columns from file
        for c in ["idref_ppn_list","idref_status","nb_match","match_info","alt_names","idref_orcid","idref_description","idref_idhal"]:
             row[c] = fr.get(c)

        # Initialize HAL columns to None
        for c_hal_pref in hal_pref:
            row[c_hal_pref] = None

        row["source"] = "Fichier"
        row["match_score"] = None
        
        best_score, best_idx = -1, None
        f_norm = fr.get("norm_full","")

        if f_norm:
            # Only iterate over unmatched HAL rows
            for i_hal, hr in df_hal[df_hal["__matched"] == False].iterrows():
                score = similarity_score(f_norm, hr.get("norm_full",""))
                if score > best_score:
                    best_score, best_idx = score, i_hal
        
        if best_idx is not None and best_score >= threshold:
            h = df_hal.loc[best_idx]
            # Add HAL data with prefix
            for c in hal_keep:
                # If HAL column name is one of the standard IdRef enrichment columns, use it without prefix
                if c in ["idref_ppn_list","idref_status","nb_match","match_info","alt_names","idref_orcid","idref_description","idref_idhal"]:
                    # ONLY replace file-based IdRef info if HAL has a match (non-None PPN list)
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

    # 2. Add remaining HAL-only rows
    st.info("➕ Ajout des auteurs HAL non-appariés...")
    for _, h in df_hal[df_hal["__matched"] == False].iterrows():
        # Initialize row with Nom/Prénom from HAL
        row = {c: None for c in file_cols_keep + hal_pref + ["source","match_score", "Nom", "Prénom"]}
        row["Nom"] = h.get("lastName_s") or ""
        row["Prénom"] = h.get("firstName_s") or ""

        # Add HAL data (enrichment columns without prefix, other HAL columns with prefix)
        for c in hal_keep:
            if c in ["idref_ppn_list","idref_status","nb_match","match_info","alt_names","idref_orcid","idref_description","idref_idhal"]:
                row[c] = h.get(c)
            else:
                row[f"HAL_{c}"] = h.get(c)

        row["source"] = "HAL"
        row["match_score"] = None
        merged.append(row)

    # 3. Final column selection and reordering based on user request
    CORE_REQUESTED_COLUMNS_ORDER = [
        "Nom",
        "Prénom",
        "idref_ppn_list",
        "HAL_idHal_s",
        "idref_idhal",
        "idref_orcid",
        "HAL_orcidId_s",
        "HAL_valid_s",
        "HAL_form_i",
        "HAL_person_i",
        "HAL_idrefId_s",
        "HAL_emailDomain_s",
        "source",
    ]

    # Get the list of original file columns (excluding IdRef/HAL standard ones)
    file_specific_cols = [c for c in df_file.columns if c not in ["Nom", "Prénom", "norm_full", "idref_ppn_list", "idref_status", "nb_match", "match_info", "alt_names", "idref_orcid", "idref_description", "idref_idhal"]]
    
    # Build the final column order: Nom, Prénom, [File Specific], [Requested Core], match_score
    final_cols_order = (
        ["Nom", "Prénom"] + 
        file_specific_cols + 
        [c for c in CORE_REQUESTED_COLUMNS_ORDER if c not in ["Nom", "Prénom"]] + 
        ["match_score"]
    )

    df_final = pd.DataFrame(merged)
    
    # Filter and reorder
    final_cols_to_keep = [c for c in final_cols_order if c in df_final.columns]
    
    df_final = df_final.loc[:, final_cols_to_keep]
    df_final = df_final.loc[:, ~df_final.columns.duplicated()] # Final cleanup
    
    return df_final


# ===== EXPORT =====
def export_xlsx(fusion,idref_df=None,hal_df=None,params=None):
    out=BytesIO()
    # Use selected engine or fallback
    engine_to_use = EXCEL_ENGINE or "xlsxwriter"
    with pd.ExcelWriter(out,engine=engine_to_use) as w:
        # Onglet 1 : Les résultats de la fusion
        fusion.to_excel(w,sheet_name="Résultats",index=False)
        # Onglet 2 : L'extraction du fichier/IdRef
        if idref_df is not None:idref_df.to_excel(w,sheet_name="extraction IdRef",index=False)
        # Onglet 3 : L'extraction des auteurs HAL
        if hal_df is not None:hal_df.to_excel(w,sheet_name="extraction HAL",index=False)
        # Onglet 4 : Les paramètres
        if params is not None:pd.DataFrame([params]).to_excel(w,sheet_name="Paramètres",index=False)
    out.seek(0);return out

# ===== INTERFACE =====
st.title("🔗 Alignement Annuaire de chercheurs ↔ IdRef ↔ HAL")

uploaded_file = st.file_uploader(
    '📄 Fichier auteurs (facultatif), doit contenir au moins une colonne "Nom" et une colonne "Prénom"',
    type=["csv","xlsx"]
)
structure_ids = st.text_input(
    "🏛️ Identifiants structures HAL",
    help="Identifiant HAL de la structure dont vous voulez récupérer les auteurs (ex: 91134). "
         "Utilisez AuréHAL pour le trouver. Séparez plusieurs identifiants par des virgules sans espace."
)

# ===== Détection Nom/Prénom (and file reading) =====
col_nom_choice = col_pre_choice = None
df_preview = None
if uploaded_file is not None:
    try:
        df_preview = pd.read_csv(uploaded_file) if uploaded_file.name.endswith(".csv") else pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"Erreur lors de la lecture du fichier : {e}")
        st.stop()
        
    st.write("Aperçu du fichier téléversé :")
    st.dataframe(df_preview.head(5))
    cols = df_preview.columns.tolist()
    
    def norm_col(c):
        c = unicodedata.normalize("NFD", str(c))
        return "".join(ch for ch in c if unicodedata.category(ch) != "Mn").lower()
    
    nom_candidates = [c for c in cols if any(k in norm_col(c) for k in ["nom","last","surname"])]
    pre_candidates = [c for c in cols if any(k in norm_col(c) for k in ["prenom","first","given"])]
    
    default_nom = nom_candidates[0] if nom_candidates else cols[0]
    default_pre = pre_candidates[0] if pre_candidates else (cols[1] if len(cols)>1 else cols[0])

    st.info(f"🔍 Colonnes détectées automatiquement : **Nom → {default_nom}**, **Prénom → {default_pre}**")
    
    col_nom_choice = st.selectbox("Colonne NOM", options=cols, index=cols.index(default_nom))
    col_pre_choice = st.selectbox("Colonne PRÉNOM", options=cols, index=cols.index(default_pre))

# ===== Paramètres =====
col1,col2 = st.columns(2)
cur = datetime.datetime.now().year
minb = col1.number_input("Année naissance min (IdRef)",1900,cur,1920)
mind = col2.number_input("Année décès min (IdRef)",1900,cur+5,2005)
col3,col4 = st.columns(2)
ymin = col3.number_input("Année min HAL",1900,cur,2015)
ymax = col4.number_input("Année max HAL",1900,cur+5,cur)
threads = st.slider("Threads IdRef HAL",2,16,8)
similarity_threshold = st.slider("Seuil similarité (%) pour fusion", 60, 100, 85)


# ===== LANCEMENT =====
if st.button("🚀 Lancer l’analyse"):
    file_provided = uploaded_file is not None and df_preview is not None
    hal_provided = bool(structure_ids.strip())
    
    if not file_provided and not hal_provided:
        st.warning("Veuillez fournir un fichier ou des identifiants de structures HAL.")
        st.stop()
        
    if file_provided and (col_nom_choice is None or col_pre_choice is None):
        st.error("Sélectionnez d'abord les colonnes Nom et Prénom.")
        st.stop()

    # --- Nettoyage des fonctions de nettoyage ---
    def clean_idref(val):
        if val is None: return None
        if isinstance(val, (list, tuple, set)): val = " ".join(map(str, val))
        try:
            if pd.isna(val): return None
        except Exception: pass
        # Extraire les identifiants IdRef valides
        matches = re.findall(r"([0-9]{6,}[A-ZX]?)", str(val))
        return "|".join(sorted(set(matches))) if matches else None

    def clean_orcid(val):
        if val is None: return None
        if isinstance(val, (list, tuple, set)): val = " ".join(map(str, val))
        try:
            if pd.isna(val): return None
        except Exception: pass
        # Extraire la séquence ORCID standard (4x4 chiffres ou X)
        match = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[0-9X])", str(val))
        return match.group(1) if match else None

    # MODE 2 — HAL SEUL
    if hal_provided and not file_provided:
        st.header("🏛️ Mode 2 : Structures HAL seules")
        pubs = fetch_publications_for_structures(structure_ids,ymin,ymax)
        ids = extract_author_ids(pubs, struct_ids=structure_ids)
        hal_auths = fetch_author_details_batch(ids,
            "docid,form_i,person_i,lastName_s,firstName_s,valid_s,idHal_s,halId_s,idrefId_s,orcidId_s,emailDomain_s")
        hal_df = pd.DataFrame(hal_auths)
        
        # --- Nettoyage des identifiants HAL ---
        if "idrefId_s" in hal_df.columns: hal_df["idrefId_s"] = hal_df["idrefId_s"].apply(clean_idref)
        if "orcidId_s" in hal_df.columns: hal_df["orcidId_s"] = hal_df["orcidId_s"].apply(clean_orcid)
        
        # --- FILTRAGE PAR STATUT (INCOMING/PREFERRED) ---
        initial_count = len(hal_df)
        if "valid_s" in hal_df.columns:
            hal_df = hal_df[hal_df["valid_s"].isin(["INCOMING", "PREFERRED"])]
            st.info(f"Filtre HAL appliqué : **{len(hal_df)}** formes-auteurs (sur {initial_count} initialement) avec statut **INCOMING** ou **PREFERRED**.")
        
        # Ensure name columns exist for parallel enrichment
        if "lastName_s" not in hal_df.columns: hal_df["lastName_s"] = None
        if "firstName_s" not in hal_df.columns: hal_df["firstName_s"] = None

        hal_df = enrich_hal_rows_with_idref_parallel(hal_df,minb,mind,threads)
        st.success("Extraction HAL et enrichissement IdRef terminés ✅")
        st.dataframe(hal_df.head(20))
        params = {"structures":structure_ids,"year_min":ymin,"year_max":ymax}
        # CORRECTION DU TYPEERROR: suppression de l'argument mot-clé fusion=hal_df redondant
        xlsx = export_xlsx(hal_df, hal_df=hal_df, params=params) 
        st.download_button("⬇️ Télécharger XLSX",xlsx,file_name="hal_idref_structures.xlsx")

    # MODE 1 — FICHIER SEUL
    elif file_provided and not hal_provided:
        st.header("🧾 Mode 1 : Fichier seul (recherche IdRef)")
        df = df_preview.copy()
        df = df.rename(columns={col_nom_choice:"Nom", col_pre_choice:"Prénom"})
        
        res = []
        prog = st.progress(0, text="Recherche IdRef pour le fichier...")
        for i, r in df.iterrows():
            first = str(r.get("Prénom", "")).strip()
            last = str(r.get("Nom", "")).strip()
            full = f"{first} {last}".strip()
            matches = search_idref_for_person(full, minb, mind)
            nb = len(matches)
            # Initialize with non-IdRef data
            info = {c:r.get(c) for c in df.columns if c not in ["Nom", "Prénom"]}
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
            res.append(info)
            prog.progress(min((i+1)/len(df), 1.0))

        prog.empty()
        idref_df = pd.DataFrame(res)
        st.dataframe(idref_df.head(20))
        params={"mode":"Fichier seul"}
        # CORRECTION DU TYPEERROR: suppression de l'argument mot-clé fusion=idref_df redondant
        xlsx = export_xlsx(idref_df, idref_df=idref_df, params=params)
        st.download_button("⬇️ Télécharger XLSX",xlsx,file_name="idref_only.xlsx")

    # MODE 3 — FUSION
    elif file_provided and hal_provided:
        st.header("🧩 Mode 3 : Fichier + HAL (fusion complète)")
        
        # 1. HAL extraction and enrichment
        st.info("📥 Extraction HAL + enrichissement IdRef...")
        pubs = fetch_publications_for_structures(structure_ids,ymin,ymax)
        ids = extract_author_ids(pubs, struct_ids=structure_ids)
        hal_auths = fetch_author_details_batch(ids,
            "docid,form_i,person_i,lastName_s,firstName_s,valid_s,idHal_s,halId_s,idrefId_s,orcidId_s,emailDomain_s")
        hal_df = pd.DataFrame(hal_auths)
        
        if "idrefId_s" in hal_df.columns: hal_df["idrefId_s"] = hal_df["idrefId_s"].apply(clean_idref)
        if "orcidId_s" in hal_df.columns: hal_df["orcidId_s"] = hal_df["orcidId_s"].apply(clean_orcid)
        
        # --- FILTRAGE PAR STATUT (INCOMING/PREFERRED) ---
        initial_count = len(hal_df)
        if "valid_s" in hal_df.columns:
            hal_df = hal_df[hal_df["valid_s"].isin(["INCOMING", "PREFERRED"])]
            st.info(f"Filtre HAL appliqué : **{len(hal_df)}** formes-auteurs (sur {initial_count} initialement) avec statut **INCOMING** ou **PREFERRED**.")
        
        if "lastName_s" not in hal_df.columns: hal_df["lastName_s"] = None
        if "firstName_s" not in hal_df.columns: hal_df["firstName_s"] = None
        
        hal_df = enrich_hal_rows_with_idref_parallel(hal_df,minb,mind,threads)

        # 2. File extraction and enrichment (similar to Mode 1)
        st.info("🔍 Recherche IdRef sur fichier...")
        df_in = df_preview.copy()
        df_in = df_in.rename(columns={col_nom_choice:"Nom", col_pre_choice:"Prénom"})
        
        res = []
        prog = st.progress(0, text="Recherche IdRef pour le fichier...")
        for i, r in df_in.iterrows():
            first = str(r.get("Prénom", "")).strip()
            last = str(r.get("Nom", "")).strip()
            full = f"{first} {last}".strip()
            matches = search_idref_for_person(full, minb, mind)
            nb = len(matches)
            
            # Initialize with non-IdRef data
            info = {c:r.get(c) for c in df_in.columns if c not in ["Nom", "Prénom"]}
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
            res.append(info)
            prog.progress(min((i+1)/len(df_in), 1.0))
        prog.empty()
        idref_df = pd.DataFrame(res)
        
        # 3. Fuzzy Merge
        st.info("⚙️ Fusion floue...")
        fusion = fuzzy_merge_file_hal(idref_df, hal_df, threshold=similarity_threshold)
        st.dataframe(fusion.head(20))
        st.success("✅ Fusion terminée")

        # 4. Export
        params = {"structures": structure_ids, "year_min": ymin, "year_max": ymax,
                  "similarity_threshold": similarity_threshold, "threads": threads,
                  "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        xlsx = export_xlsx(fusion,idref_df=idref_df,hal_df=hal_df,params=params) 
        st.download_button("⬇️ Télécharger XLSX fusion",xlsx,file_name="fusion_idref_hal.xlsx")
