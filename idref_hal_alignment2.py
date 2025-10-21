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
        # Utiliser un score plus strict pour un matching précis
        return fuzz.WRatio(a or "", b or "") 
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

# ===== HAL FUNCTIONS (Extraction des formes auteurs) =====
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
                # CORRECTION : on récupère l'ID complet de la forme auteur (le docid) pour inclure les INCOMING
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
        # CORRECTION : Utilisation de docid au lieu de person_i pour interroger l'API
        # On ajoute docid dans les champs retournés
        q = " OR ".join([f'docid:\"{x}\"' for x in batch]) 
        params = {"q": q, "wt": "json", "fl": fields + ",docid", "rows": batch_size}
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

# ===== IDREF enrichment (HAL - Parallel) =====

def process_hal_row(row, min_birth, min_death):
    """
    Tente l'enrichissement IdRef pour un auteur HAL.
    Priorité : 1. PPN(s) dans le champ idrefId_s 2. Recherche par nom.
    """
    hal_first = row.get("firstName_s") or ""
    hal_last = row.get("lastName_s") or ""
    hal_full = f"{hal_first} {hal_last}".strip()
    hal_idrefs = row.get("idrefId_s")

    result = {"idref_ppn_list": None, "idref_status": "not_found", "nb_match": 0,
              "match_info": None, "alt_names": None, "idref_orcid": None,
              "idref_description": None, "idref_idhal": None}
    
    # ----------------------------------------------------
    # 1. Tenter l'enrichissement via le PPN fourni par HAL (si disponible)
    # ----------------------------------------------------
    if pd.notna(hal_idrefs) and str(hal_idrefs).strip():
        # Extrait tous les PPN valides
        ppns = re.findall(r"([0-9]{6,}[A-ZX]?)", str(hal_idrefs))
        if ppns:
            descs, alts = [], []
            orcid, idhal, match_info = None, None, None
            parsed_any = False
            
            # Tente d'enrichir en récupérant la notice complète pour chaque PPN
            for ppn in ppns:
                try:
                    xml = pydref_api.get_idref_notice(ppn)
                    if not xml: continue
                    parsed_any = True
                    # Assurez-vous que l'implémentation de Pydref supprime les namespaces XML
                    # Sinon, il faut faire xml.replace('xmlns="..."', '')
                    xml_clean = xml.replace('xmlns="http://www.loc.gov/MARC21/slim"', '')
                    soup = BeautifulSoup(xml_clean, "xml") 
                    
                    ids_details = pydref_api.get_identifiers_from_idref_notice(soup)
                    
                    # Extrait ORCID et IdHAL des identifiants détaillés
                    for ident in ids_details:
                        if "orcid" in ident and not orcid:
                            orcid = ident["orcid"]
                        if "idhal" in ident and not idhal:
                            idhal = ident["idhal"]
                    
                    # Extrait d'autres infos
                    desc = pydref_api.get_description_from_idref_notice(soup)
                    alt = pydref_api.get_alternative_names_from_idref_notice(soup)
                    nameinfo = pydref_api.get_name_from_idref_notice(soup)

                    if not match_info: # Garde le nom de la première notice
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

    # ----------------------------------------------------
    # 2. Fallback : Recherche par nom dans IdRef (si pas de PPN ou échec de l'enrichissement PPN)
    # ----------------------------------------------------
    if hal_full:
        matches = search_idref_for_person(hal_full, min_birth, min_death)
        nb = len(matches)
        if nb > 0:
            ppns = [m.get("idref","").replace("idref","") for m in matches if m.get("idref")]
            descs, alts = [], []
            orcid, idhal, match_info = None, None, None
            for m in matches:
                # Le résultat de search_idref_for_person inclut souvent des identifiants
                if isinstance(m.get("description"), list): descs += m["description"]
                if isinstance(m.get("alt_names"), list): alts += m["alt_names"]
                
                # Extraction des identifiants disponibles (IdHAL/ORCID)
                # Ces champs sont ajoutés à la racine du dict par la fonction get_idref corrigée
                if "orcid" in m and not orcid: orcid = m["orcid"]
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
    # Ajout d'une colonne "auteur_hal_complet" pour l'onglet extraction
    hal_df['auteur_hal_complet'] = hal_df['firstName_s'] + ' ' + hal_df['lastName_s']
    st.info(f"🔄 Enrichissement IdRef ({len(hal_df)} auteurs HAL)...")
    total = len(hal_df)
    results = []
    prog = st.progress(0)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures={ex.submit(process_hal_row,row,min_birth,min_death):i for i,row in hal_df.iterrows()}
        done=0
        for fut in concurrent.futures.as_completed(futures):
            i=futures[fut]
            try:
                result_dict = fut.result()
                # Fusionner le résultat avec les données HAL existantes
                hal_row = hal_df.iloc[i].to_dict()
                hal_row.update(result_dict)
                results.append(hal_row)
            except Exception as e:
                # Gérer l'erreur si nécessaire
                results.append(hal_df.iloc[i].to_dict()) 
            done+=1
            if done%5==0 or done==total:prog.progress(done/total)
    prog.empty()
    return pd.DataFrame(results)

# ===== IDREF enrichment (FILE - Parallel) =====

def process_file_row(row, min_birth, min_death):
    """Effectue la recherche IdRef détaillée pour une seule ligne du fichier (utilisé en parallèle)."""
    first = str(row.get("Prénom", "")).strip()
    last = str(row.get("Nom", "")).strip()
    full = f"{first} {last}".strip()
    matches = search_idref_for_person(full, min_birth, min_death)
    nb = len(matches)

    # Récupération des données originales et initialisation des champs IdRef
    info = {c: row.get(c) for c in row.keys() if c not in ["Nom", "Prénom"]}
    info.update({"Nom": last, "Prénom": first, "idref_ppn_list": None, "idref_status": "not_found",
                "nb_match": nb, "match_info": None, "alt_names": None, "idref_orcid": None,
                "idref_description": None, "idref_idhal": None})

    if nb:
        ppns = [m.get("idref","").replace("idref","") for m in matches if m.get("idref")]
        info["idref_ppn_list"] = "|".join(ppns)
        info["idref_status"] = "found" if nb == 1 else "ambiguous"
        # Afficher tous les noms trouvés pour match_info
        info["match_info"] = "; ".join([f"{m.get('first_name','')} {m.get('last_name','')}" for m in matches])
        desc, alt = [], []
        orcid, idhal = None, None
        
        for m in matches:
            if isinstance(m.get("description"), list): desc += m["description"]
            if isinstance(m.get("alt_names"), list): alt += m["alt_names"]
            
            # Extraction des identifiants de la racine
            if "orcid" in m and not orcid: orcid = m["orcid"]
            if "idhal" in m and not idhal: idhal = m["idhal"]
        
        info["idref_description"] = "; ".join(desc) if desc else None
        info["alt_names"] = "; ".join(sorted(set(alt))) if alt else None
        info["idref_orcid"] = orcid
        info["idref_idhal"] = idhal
        
    return info

def enrich_file_rows_with_idref_parallel(df, min_birth, min_death, max_workers=8):
    """Gère l'exécution parallèle de la recherche IdRef pour les lignes du fichier."""
    st.info(f"🔄 Recherche IdRef parallèle pour le fichier ({len(df)} auteurs)...")
    total = len(df)
    results = []
    prog = st.progress(0)
    
    # Convertir en liste de dictionnaires pour un accès sûr par les threads
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

# ===== FUZZY MERGE (Version corrigée et prioritaire) =====
def fuzzy_merge_file_hal(df_file, df_hal, threshold=90):
    """
    Fusion floue des auteurs du fichier avec les auteurs HAL.
    Priorise le statut PREFERRED et garantit un match unique (1:1).
    """
    # 1. Préparation et normalisation des données
    df_file = df_file.copy()
    df_hal = df_hal.copy()
    
    # Colonnes HAL pertinentes, y compris le nouveau nom docid
    HAL_CORE_COLS = ["docid","person_i","lastName_s","firstName_s","valid_s","idHal_s","halId_s","idrefId_s","orcidId_s","emailDomain_s"]
    HAL_IDREF_COLS = ["idref_ppn_list", "idref_status", "nb_match", "match_info", "alt_names", "idref_orcid", "idref_description", "idref_idhal"]

    # Création des colonnes normalisées pour la comparaison
    df_file["norm_full"] = (df_file["Prénom"].fillna("").apply(normalize_text)+" "+df_file["Nom"].fillna("").apply(normalize_text)).str.strip()
    df_hal["norm_full"] = (df_hal["firstName_s"].fillna("").apply(normalize_text)+" "+df_hal["lastName_s"].fillna("").apply(normalize_text)).str.strip()
    df_hal["__matched"] = False
    
    # Priorisation des formes HAL: PREFERRED (2) > INCOMING (1)
    df_hal['__priority'] = df_hal['valid_s'].map({'PREFERRED': 2, 'INCOMING': 1}).fillna(0)
    df_hal = df_hal.sort_values(by=['__priority', 'norm_full'], ascending=[False, True]).reset_index(drop=True)
    
    merged = []
    st.info("🔄 Tentative de fusion floue 1:1 (Fichier → HAL, Priorité PREFERRED)...")
    prog = st.progress(0, text="Fusion...")
    total_file = len(df_file)
    
    for i, fr in df_file.iterrows():
        row = fr.to_dict()
        row["source"] = "Fichier"
        row["match_score"] = None
        
        best_score = -1
        best_match_hal = None
        f_norm = fr.get("norm_full","")

        if f_norm:
            # Recherche du meilleur match unique
            for i_hal, hr in df_hal[df_hal["__matched"] == False].iterrows():
                score = similarity_score(f_norm, hr.get("norm_full",""))
                
                # Critère de sélection : Meilleur score et supérieur au seuil
                if score >= threshold and score > best_score:
                    best_score = score
                    best_match_hal = hr

        # Si un match valide et unique est trouvé
        if best_match_hal is not None:
            h = best_match_hal
            
            # Marquer la ligne HAL comme utilisée (pour garantir 1:1)
            df_hal.loc[h.name, "__matched"] = True
            
            # Remplacement/Ajout des données HAL
            for col in HAL_CORE_COLS:
                row[f"HAL_{col}"] = h.get(col)
            
            # Remplacement des infos IdRef du fichier par celles de HAL si HAL les a trouvées
            hal_ppn_found = pd.notna(h.get("idref_ppn_list"))
            for col in HAL_IDREF_COLS:
                if hal_ppn_found and pd.notna(h.get(col)):
                     row[col] = h.get(col)
                # Sinon, on garde la valeur IdRef du fichier (déjà dans 'row')

            row["source"] = "Fichier + HAL"
            row["match_score"] = best_score
        
        # Supprimer les colonnes temporaires
        row.pop("norm_full", None)
        
        merged.append(row)
        prog.progress(min((i+1)/total_file, 1.0))
    
    prog.empty()

    # 2. Ajout des lignes HAL-only restantes
    st.info("➕ Ajout des auteurs HAL non-appariés...")
    
    # Colonnes spécifiques au fichier (hors Nom, Prénom, norm_full et les colonnes IdRef standard)
    file_specific_cols = [c for c in df_file.columns if c not in ["Nom", "Prénom", "norm_full"] + HAL_IDREF_COLS]
    
    # Colonnes HAL avec préfixe HAL_
    hal_pref_cols = [f"HAL_{c}" for c in HAL_CORE_COLS]
    
    for _, h in df_hal[df_hal["__matched"] == False].iterrows():
        # Ligne de base (initialisée à None pour les colonnes Fichier)
        row = {c: None for c in file_specific_cols + hal_pref_cols + ["Nom", "Prénom", "match_score", "source"]}

        # Ajout des données de base
        row["Nom"] = h.get("lastName_s") or ""
        row["Prénom"] = h.get("firstName_s") or ""
        row["source"] = "HAL"
        
        # Ajout des données IdRef enrichies par HAL
        for col in HAL_IDREF_COLS:
             row[col] = h.get(col)

        # Ajout des données HAL
        for col in HAL_CORE_COLS:
            row[f"HAL_{col}"] = h.get(col)

        merged.append(row)

    # 3. Finalisation des colonnes pour l'onglet "Résultats"
    # COLONNES DEMANDÉES PAR L'UTILISATEUR
    CORE_REQUESTED_COLUMNS_ORDER = [
        "Nom", "Prénom", "idref_ppn_list", "HAL_idrefId_s", "HAL_idHal_s", 
        "idref_idhal", "idref_orcid", "HAL_orcidId_s", "HAL_valid_s", 
        "HAL_docid", "HAL_emailDomain_s", "idref_description", "source"
    ]
    
    df_final = pd.DataFrame(merged)
    # Ajouter les colonnes spécifiques au fichier à l'ordre
    file_specific_cols_in_df = [c for c in file_specific_cols if c in df_final.columns]
    final_cols_order = ["Nom", "Prénom"] + file_specific_cols_in_df + [c for c in CORE_REQUESTED_COLUMNS_ORDER if c not in ["Nom", "Prénom"]] + ["match_score"]

    final_cols_to_keep = [c for c in final_cols_order if c in df_final.columns]
    df_final = df_final.loc[:, final_cols_to_keep]
    df_final = df_final.loc[:, ~df_final.columns.duplicated()]
    
    return df_final.sort_values(by=["Nom", "Prénom"])


# ===== EXPORT (Mise à jour des onglets et colonnes) =====
def export_xlsx(fusion, idref_df=None, hal_df=None, idref_hal_df_for_extraction=None, params=None):
    out = BytesIO()
    engine_to_use = EXCEL_ENGINE or "openpyxl"
    
        
    # 1. Onglet Résultats (fusion)
    FUSION_COLS = [
        "Nom", "Prénom", "idref_ppn_list", "HAL_idrefId_s", "HAL_idHal_s", "idref_idhal", 
        "idref_orcid", "HAL_orcidId_s", "HAL_valid_s", "HAL_docid", "HAL_emailDomain_s", 
        "idref_description", "source", "match_score"
    ]
    
    # On ajoute les colonnes spécifiques au fichier (Nom/Prénom non inclus)
    file_specific_cols = [c for c in fusion.columns if c not in [
        "Nom", "Prénom", "match_score"] + FUSION_COLS]
    FUSION_COLS_FINAL = [c for c in ["Nom", "Prénom"] + file_specific_cols + FUSION_COLS if c in fusion.columns and c not in ["Nom", "Prénom"]]
    FUSION_COLS_FINAL = ["Nom", "Prénom"] + [c for c in FUSION_COLS_FINAL if c not in ["Nom", "Prénom"]]
    
    # 2. Onglet Extraction IdRef (Fichier + HAL enrichis)
    IDREF_EXTRACTION_COLS = [
        "Nom", "Prénom", "idref_ppn_list", "idref_status", "nb_match", "match_info", 
        "alt_names", "idref_orcid", "idref_description", "idref_idhal"
    ]
    
    # 3. Onglet Extraction HAL
    HAL_EXTRACTION_COLS = [
        "firstName_s", "lastName_s", "valid_s", "docid", "idHal_s", "orcidId_s", 
        "idrefId_s", "emailDomain_s", "idref_ppn_list", "idref_idhal", "idref_orcid" # on ajoute les champs enrichis pour le contexte
    ]

    with pd.ExcelWriter(out, engine=engine_to_use) as w:
        
        # Onglet Résultats
        fusion_to_export = fusion.loc[:, [c for c in FUSION_COLS_FINAL if c in fusion.columns]]
        fusion_to_export.to_excel(w, sheet_name="Résultats", index=False)
        
        # Onglet Extraction IdRef (toutes les entrées Fichier + HAL)
        if idref_hal_df_for_extraction is not None:
            # On conserve que les colonnes pertinentes à l'extraction IdRef
            idref_extraction_to_export = idref_hal_df_for_extraction.loc[:, [c for c in IDREF_EXTRACTION_COLS if c in idref_hal_df_for_extraction.columns]]
            idref_extraction_to_export = idref_extraction_to_export.rename(columns={"nb_match": "idref_nb_match"})
            idref_extraction_to_export.to_excel(w, sheet_name="Extraction IdRef", index=False)
            
        # Onglet Extraction HAL
        if hal_df is not None:
            hal_extraction_to_export = hal_df.loc[:, [c for c in HAL_EXTRACTION_COLS if c in hal_df.columns]]
            # Renommage des colonnes HAL selon la demande
            rename_map = {
                "firstName_s": "HAL_firstName_s",
                "lastName_s": "HAL_lastName_s",
                "valid_s": "HAL_valid_s",
                "docid": "HAL_docid",
                "idHal_s": "HAL_idHal_s",
                "orcidId_s": "HAL_orcidId_s",
                "idrefId_s": "HAL_idrefId_s",
                "emailDomain_s": "HAL_emailDomain_s"
            }
            hal_extraction_to_export = hal_extraction_to_export.rename(columns=rename_map)
            hal_extraction_to_export.to_excel(w, sheet_name="Extraction HAL", index=False)
            
        if params is not None:
            pd.DataFrame([params]).to_excel(w, sheet_name="Paramètres", index=False)
            
    out.seek(0)
    return out

# ... (Fonctions utilitaires et de traitement HAL/IdRef inchangées) ...

# ===== LAUNCHING THE APP (partie principale du script Streamlit) =====
if __name__ == "__main__":
    st.title("Alignement Auteurs Annuaire ↔ IdRef ↔ HAL")
    
    # ----------------------------------------------------
    # 1. PARAMETERS INPUT
    # ----------------------------------------------------
    with st.expander("⚙️ Paramètres d'exécution et de recherche", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            uploaded_file = st.file_uploader("📂 Uploader un fichier CSV/XLSX (Annuaire ou liste)", type=["csv", "xlsx"])
            file_provided = uploaded_file is not None
            if file_provided:
                df_preview = pd.read_csv(uploaded_file) if uploaded_file.name.endswith(".csv") else pd.read_excel(uploaded_file)
                st.info(f"{len(df_preview)} lignes chargées. Colonnes: {', '.join(df_preview.columns)}")
                col_nom_choice = st.selectbox("Sélectionnez la colonne **Nom**:", df_preview.columns)
                col_pre_choice = st.selectbox("Sélectionnez la colonne **Prénom**:", df_preview.columns, index=0 if "Prénom" in df_preview.columns else 0)
        
        with col2:
            structure_ids = st.text_area("🏛️ IDs Structures HAL (ex: UMR1234, CNRS_TOLOUSE)", height=100)
            hal_provided = bool(structure_ids.strip())
            year_min = st.number_input("Année min publication HAL", value=2015, min_value=1900, max_value=datetime.datetime.now().year)
            year_max = st.number_input("Année max publication HAL", value=datetime.datetime.now().year, min_value=1900, max_value=datetime.datetime.now().year)
            
        with col3:
            minb = st.number_input("Année de naissance min (IdRef)", value=1920, min_value=1900)
            mind = st.number_input("Année de décès min (IdRef)", value=2005, min_value=1900)
            similarity_threshold = st.slider("Seuil de similarité (Fusion Nom/Prénom)", min_value=70, max_value=100, value=90)
            threads = st.slider("Threads (Recherche parallèle)", min_value=1, max_value=16, value=8)

    st.markdown("---")
    
    # ----------------------------------------------------
    # 2. EXECUTION LOGIC
    # ----------------------------------------------------

    if not file_provided and not hal_provided:
        st.warning("Veuillez uploader un fichier ou spécifier des IDs de structure HAL pour commencer.")
        
    elif st.button("🚀 Lancer l'alignement et l'extraction"):

        # MODE 1 — FICHIER SEUL (enrichissement IdRef)
        if file_provided and not hal_provided:
            st.header("🔍 Mode 1 : Fichier Annuaire seul (enrichissement IdRef)")
            
            df_in = df_preview.copy()
            df_in = df_in.rename(columns={col_nom_choice:"Nom", col_pre_choice:"Prénom"})
            
            # Enrichissement IdRef
            idref_df = enrich_file_rows_with_idref_parallel(df_in, minb, mind, threads)
            
            # Affichage et Export
            st.dataframe(idref_df.head(20))
            
            # Création de l'onglet Extraction IdRef (seulement le fichier ici)
            idref_extraction_to_export = idref_df.rename(columns={"nb_match": "idref_nb_match"})
            
            params = {"date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            xlsx = export_xlsx(idref_df, idref_df=idref_df, hal_df=None, idref_hal_df_for_extraction=idref_extraction_to_export, params=params)
            st.download_button("⬇️ Télécharger XLSX (Extraction IdRef)", xlsx, file_name="extraction_idref_fichier.xlsx")

        # MODE 2 — HAL SEUL (extraction formes auteurs + enrichissement IdRef)
        elif hal_provided and not file_provided:
            st.header("🏛️ Mode 2 : Collection HAL seule (Extraction + Enrichissement IdRef)")
            
            # 1. Extraction HAL
            docs = fetch_publications_for_structures(structure_ids, year_min, year_max)
            author_ids = extract_author_ids(docs, structure_ids)
            st.info(f"✨ {len(author_ids)} formes-auteurs uniques trouvées. Récupération des détails...")
            
            HAL_FIELDS = ["lastName_s", "firstName_s", "valid_s", "idHal_s", "halId_s", "idrefId_s", "orcidId_s", "emailDomain_s"]
            hal_authors = fetch_author_details_batch(author_ids, HAL_FIELDS)
            hal_df = pd.DataFrame(hal_authors)
            
            # Nettoyage ORCID (au cas où il y a d'autres textes)
            if "orcidId_s" in hal_df.columns:
                def clean_orcid(val):
                    h = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[0-9X])", str(val))
                    return h.group(1) if h else None
                hal_df["orcidId_s"] = hal_df["orcidId_s"].apply(clean_orcid)
            
            # 2. Enrichissement IdRef des auteurs HAL
            hal_df = enrich_hal_rows_with_idref_parallel(hal_df, minb, mind, threads)
            
            # Affichage et Export
            st.dataframe(hal_df.head(20))
            
            # Préparation pour l'onglet Extraction IdRef (on renomme les colonnes)
            hal_for_idref_extraction = hal_df.rename(columns={
                "lastName_s": "Nom", "firstName_s": "Prénom", "nb_match": "idref_nb_match"
            })

            params = {"structures": structure_ids, "year_min": year_min, "year_max": year_max,
                      "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            # On utilise le même df pour l'onglet Résultats et l'onglet Extraction HAL
            xlsx = export_xlsx(hal_df, idref_df=None, hal_df=hal_df, idref_hal_df_for_extraction=hal_for_idref_extraction, params=params)
            st.download_button("⬇️ Télécharger XLSX (Extraction HAL + IdRef)", xlsx, file_name="extraction_hal_idref.xlsx")
            
        # MODE 3 — FUSION
        elif file_provided and hal_provided:
            st.header("🧩 Mode 3 : Fichier + HAL (fusion complète)")
            
            # 1. HAL extraction and enrichment 
            # ----------------------------------------------------
            # CODE MANQUANT RÉINTÉGRÉ ICI
            # ----------------------------------------------------
            docs = fetch_publications_for_structures(structure_ids, year_min, year_max)
            author_ids = extract_author_ids(docs, structure_ids)
            st.info(f"✨ {len(author_ids)} formes-auteurs uniques trouvées. Récupération des détails...")
            
            HAL_FIELDS = ["lastName_s", "firstName_s", "valid_s", "idHal_s", "halId_s", "idrefId_s", "orcidId_s", "emailDomain_s"]
            hal_authors = fetch_author_details_batch(author_ids, HAL_FIELDS)
            hal_df = pd.DataFrame(hal_authors) # <--- hal_df est maintenant DEFINI

            # Nettoyage ORCID
            if "orcidId_s" in hal_df.columns:
                def clean_orcid(val):
                    h = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[0-9X])", str(val))
                    return h.group(1) if h else None
                hal_df["orcidId_s"] = hal_df["orcidId_s"].apply(clean_orcid)

            # Enrichissement IdRef des auteurs HAL
            hal_df = enrich_hal_rows_with_idref_parallel(hal_df,minb,mind,threads) # <--- Ligne qui provoquait l'erreur
            
            # 2. File extraction and enrichment
            df_in = df_preview.copy()
            df_in = df_in.rename(columns={col_nom_choice:"Nom", col_pre_choice:"Prénom"})
            idref_df = enrich_file_rows_with_idref_parallel(df_in, minb, mind, threads)
            
            # 3. Préparation du DataFrame pour l'onglet "Extraction IdRef"
            
            # Préparation des colonnes du fichier pour la concaténation
            # On ne garde que les colonnes pertinentes de l'enrichissement IdRef
            idref_df_base = idref_df[["Nom", "Prénom", "idref_ppn_list", "idref_status", "nb_match", "match_info", "alt_names", "idref_orcid", "idref_description", "idref_idhal"]].copy()

            # On renomme les colonnes HAL pour matcher les colonnes Fichier (Nom/Prénom/IdRef)
            hal_for_idref_extraction = hal_df.rename(columns={
                "lastName_s": "Nom", "firstName_s": "Prénom"
            })
            # Sélection des colonnes IdRef/Nom/Prénom pour les auteurs HAL
            hal_for_idref_extraction = hal_for_idref_extraction[[
                 "Nom", "Prénom", "idref_ppn_list", "idref_status", "nb_match", "match_info",
                 "alt_names", "idref_orcid", "idref_description", "idref_idhal"
            ]].copy()
            
            # Union des deux DataFrames pour l'onglet d'extraction
            idref_hal_df_for_extraction = pd.concat([idref_df_base, hal_for_idref_extraction], ignore_index=True)
            # Renommer la colonne 'nb_match' pour l'export
            idref_hal_df_for_extraction = idref_hal_df_for_extraction.rename(columns={"nb_match": "idref_nb_match"})

            # 4. Fuzzy Merge (Logique 1:1 et PREFERRED)
            st.info("⚙️ Fusion floue...")
            # Note: on utilise idref_df (le fichier enrichi) et hal_df (le hal enrichi)
            fusion = fuzzy_merge_file_hal(idref_df, hal_df, threshold=similarity_threshold)
            st.dataframe(fusion.head(20))
            st.success("✅ Fusion terminée")

            # 5. Export
            params = {"structures": structure_ids, "year_min": year_min, "year_max": year_max,
                      "similarity_threshold": similarity_threshold, "threads": threads,
                      "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            
            xlsx = export_xlsx(fusion, idref_df=None, hal_df=hal_df, idref_hal_df_for_extraction=idref_hal_df_for_extraction, params=params) 
            st.download_button("⬇️ Télécharger XLSX fusion",xlsx,file_name="fusion_idref_hal.xlsx")

# ... (Fin du script) ...
