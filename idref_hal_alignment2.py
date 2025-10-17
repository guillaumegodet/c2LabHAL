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
    EXCEL_ENGINE = "openpyxl"

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

# ===== IDREF enrichissement pour HAL =====
def process_hal_row(row, min_birth, min_death):
    hal_first, hal_last = row.get("firstName_s",""), row.get("lastName_s","")
    hal_full = f"{hal_first} {hal_last}".strip()
    hal_idrefs = row.get("idrefId_s")
    result = {"idref_ppn_list":None,"idref_status":"not_found","nb_match":0,
              "idref_description":None,"idref_orcid":None,"idref_idhal":None}
    # Si déjà présent dans HAL
    if pd.notna(hal_idrefs) and str(hal_idrefs).strip():
        ids = re.findall(r"([0-9]{6,}[A-ZX]?)", str(hal_idrefs))
        if ids:
            result["idref_ppn_list"]="|".join(sorted(set(ids)))
            result["idref_status"]="found"
            result["nb_match"]=len(ids)
            return result
    # Sinon requête IdRef
    if hal_full:
        matches = search_idref_for_person(hal_full, min_birth, min_death)
        if matches:
            ppns=[m.get("idref","").replace("idref","") for m in matches if m.get("idref")]
            descs=[d for m in matches for d in m.get("description",[])]
            result.update({
                "idref_ppn_list":"|".join(ppns),
                "idref_status":"found" if len(matches)==1 else "ambiguous",
                "nb_match":len(matches),
                "idref_description":"; ".join(descs) if descs else None,
                "idref_orcid":next((i.get("orcid") for m in matches for i in m.get("identifiers",[]) if "orcid" in i),None),
                "idref_idhal":next((m.get("idhal") for m in matches if m.get("idhal")),None)
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
            except Exception:results.append((i,{}))
            done+=1
            if done%5==0 or done==total:prog.progress(done/total)
    prog.empty()
    for i,res in results:
        for k,v in res.items():hal_df.at[i,k]=v
    return hal_df

# ===== EXPORT =====
def export_xlsx(fusion,idref_df=None,hal_df=None,params=None):
    out=BytesIO()
    with pd.ExcelWriter(out,engine=EXCEL_ENGINE) as w:
        fusion.to_excel(w,sheet_name="Résultats",index=False)
        if idref_df is not None:idref_df.to_excel(w,sheet_name="extraction IdRef",index=False)
        if hal_df is not None:hal_df.to_excel(w,sheet_name="extraction HAL",index=False)
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

# ===== Détection Nom/Prénom =====
col_nom_choice = col_pre_choice = None
if uploaded_file is not None:
    df_preview = pd.read_csv(uploaded_file) if uploaded_file.name.endswith(".csv") else pd.read_excel(uploaded_file)
    st.dataframe(df_preview.head(5))
    cols = df_preview.columns.tolist()
    def norm_col(c):
        c = unicodedata.normalize("NFD", str(c))
        return "".join(ch for ch in c if unicodedata.category(ch) != "Mn").lower()
    nom_candidates = [c for c in cols if any(k in norm_col(c) for k in ["nom","last"])]
    pre_candidates = [c for c in cols if any(k in norm_col(c) for k in ["prenom","first"])]
    default_nom = nom_candidates[0] if nom_candidates else cols[0]
    default_pre = pre_candidates[0] if pre_candidates else (cols[1] if len(cols)>1 else cols[0])
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

# ===== LANCEMENT =====
if st.button("🚀 Lancer l’analyse"):
    file_provided = uploaded_file is not None
    hal_provided = bool(structure_ids.strip())
    if not file_provided and not hal_provided:
        st.warning("Veuillez fournir un fichier ou des identifiants de structures HAL.")
        st.stop()

    # MODE 2 — HAL SEUL
    if hal_provided and not file_provided:
        pubs = fetch_publications_for_structures(structure_ids,ymin,ymax)
        ids = extract_author_ids(pubs, struct_ids=structure_ids)
        hal_auths = fetch_author_details_batch(ids,
            "docid,form_i,person_i,lastName_s,firstName_s,valid_s,idHal_s,halId_s,idrefId_s,orcidId_s,emailDomain_s")
        hal_df = pd.DataFrame(hal_auths)
        # --- Nettoyage des identifiants HAL ---
        if "idrefId_s" in hal_df.columns:
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
                # Extraire les identifiants IdRef valides
                matches = re.findall(r"([0-9]{6,}[A-ZX]?)", str(val))
                return "|".join(sorted(set(matches))) if matches else None
            hal_df["idrefId_s"] = hal_df["idrefId_s"].apply(clean_idref)
        
        if "orcidId_s" in hal_df.columns:
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
                # Extraire la séquence ORCID standard
                match = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[0-9X])", str(val))
                return match.group(1) if match else None
            hal_df["orcidId_s"] = hal_df["orcidId_s"].apply(clean_orcid)
    
        hal_df = enrich_hal_rows_with_idref_parallel(hal_df,minb,mind,threads)
        st.dataframe(hal_df.head(20))
        xlsx = export_xlsx(hal_df,hal_df=hal_df,params={"structures":structure_ids})
        st.download_button("⬇️ Télécharger XLSX",xlsx,file_name="hal_idref_structures.xlsx")

    # MODE 1 — FICHIER SEUL
    elif file_provided and not hal_provided:
        df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith(".csv") else pd.read_excel(uploaded_file)
        df = df.rename(columns={col_nom_choice:"Nom", col_pre_choice:"Prénom"})
        st.info("Recherche IdRef sur le fichier...")
        for i,row in df.iterrows():
            full = f"{row['Prénom']} {row['Nom']}".strip()
            res = search_idref_for_person(full,minb,mind)
            if res:
                ppn = [m.get("idref","").replace("idref","") for m in res if m.get("idref")]
                df.at[i,"idref_ppn_list"] = "|".join(ppn)
                df.at[i,"idref_status"] = "found" if len(res)==1 else "ambiguous"
                df.at[i,"nb_match"] = len(res)
        st.dataframe(df.head(20))
        xlsx = export_xlsx(df,idref_df=df,params={"mode":"Fichier seul"})
        st.download_button("⬇️ Télécharger XLSX",xlsx,file_name="idref_only.xlsx")

    # MODE 3 — FUSION
    elif file_provided and hal_provided:
        pubs = fetch_publications_for_structures(structure_ids,ymin,ymax)
        ids = extract_author_ids(pubs, struct_ids=structure_ids)
        hal_auths = fetch_author_details_batch(ids,
            "docid,form_i,person_i,lastName_s,firstName_s,valid_s,idHal_s,halId_s,idrefId_s,orcidId_s,emailDomain_s")
        hal_df = pd.DataFrame(hal_auths)
        # --- Nettoyage des identifiants HAL ---
        if "idrefId_s" in hal_df.columns:
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
                # Extraire les identifiants IdRef valides
                matches = re.findall(r"([0-9]{6,}[A-ZX]?)", str(val))
                return "|".join(sorted(set(matches))) if matches else None
            hal_df["idrefId_s"] = hal_df["idrefId_s"].apply(clean_idref)
        
        if "orcidId_s" in hal_df.columns:
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
                # Extraire la séquence ORCID standard
                match = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[0-9X])", str(val))
                return match.group(1) if match else None
            hal_df["orcidId_s"] = hal_df["orcidId_s"].apply(clean_orcid)

        hal_df = enrich_hal_rows_with_idref_parallel(hal_df,minb,mind,threads)
        df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith(".csv") else pd.read_excel(uploaded_file)
        df = df.rename(columns={col_nom_choice:"Nom", col_pre_choice:"Prénom"})
        df["norm_first"] = df["Prénom"].apply(normalize_text)
        df["norm_last"] = df["Nom"].apply(normalize_text)
        hal_df["norm_first"] = hal_df["firstName_s"].apply(normalize_text)
        hal_df["norm_last"] = hal_df["lastName_s"].apply(normalize_text)
        merged_rows=[]
        for _,row in df.iterrows():
            best=None;score=0
            for _,h in hal_df.iterrows():
                s=(similarity_score(row["norm_first"],h["norm_first"])+similarity_score(row["norm_last"],h["norm_last"]))/2
                if s>score:
                    score=s;best=h
            if best is not None and score>=90:
                merged_rows.append({**row,**{f"HAL_{c}":best.get(c) for c in hal_df.columns},"match_score":score})
        fusion = pd.DataFrame(merged_rows)
        st.dataframe(fusion.head(20))
        xlsx = export_xlsx(fusion,idref_df=df,hal_df=hal_df,params={"structures":structure_ids,"fusion":"oui"})
        st.download_button("⬇️ Télécharger XLSX fusion",xlsx,file_name="fusion_idref_hal.xlsx")
