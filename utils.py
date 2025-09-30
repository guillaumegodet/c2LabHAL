import streamlit as st
import pandas as pd
import requests
import json
from metapub import PubMedFetcher
import regex as re
from unidecode import unidecode
import unicodedata
from difflib import get_close_matches
from langdetect import detect # Bien que non utilisé directement, gardé si une fonction importée en dépend
from tqdm import tqdm 
from concurrent.futures import ThreadPoolExecutor

tqdm.pandas()

# --- Constantes Partagées ---
HAL_API_ENDPOINT = "http://api.archives-ouvertes.fr/search/"
# Ajout de uri_s pour récupérer l'URL directe de la notice HAL
HAL_FIELDS_TO_FETCH = "docid,doiId_s,title_s,submitType_s,linkExtUrl_s,linkExtId_s,uri_s"
DEFAULT_START_YEAR = 2018
DEFAULT_END_YEAR = '*' 

SOLR_ESCAPE_RULES = {
    '+': r'\+', '-': r'\-', '&': r'\&', '|': r'\|', '!': r'\!', '(': r'\(',
    ')': r'\)', '{': r'\{', '}': r'\}', '[': r'\[', ']': r'\]', '^': r'\^',
    '~': r'\~', '*': r'\*', '?': r'\?', ':': r'\:', '"': r'\"'
}

# --- Fonctions Utilitaires ---

def _display_long_warning(base_message, item_identifier, item_value, exception_details, max_len=70):
    """
    Helper function to display a potentially long warning message with an expander.
    """
    full_error_message = f"{base_message} pour {item_identifier} '{item_value}': {exception_details}"
    item_value_str = str(item_value) 

    if len(item_value_str) > max_len:
        short_item_value = item_value_str[:max_len-3] + "..."
        st.warning(f"{base_message} pour {item_identifier} '{short_item_value}' (détails ci-dessous).")
        with st.expander("Voir les détails de l'erreur"):
            st.error(full_error_message)
    else:
        st.warning(full_error_message)


def get_scopus_data(api_key, query, max_items=10000):
    found_items_num = -1 
    start_item = 0
    items_per_query = 25 
    results_json = []
    processed_items = 0

    while True:
        if found_items_num != -1 and (processed_items >= found_items_num or processed_items >= max_items) :
            break 

        try:
            resp = requests.get(
                'https://api.elsevier.com/content/search/scopus',
                headers={'Accept': 'application/json', 'X-ELS-APIKey': api_key},
                params={'query': query, 'count': items_per_query, 'start': start_item},
                timeout=30 
            )
            resp.raise_for_status()  
            data = resp.json()
        except requests.exceptions.RequestException as e:
            st.error(f"Erreur lors de la requête Scopus (start_item: {start_item}): {e}")
            return results_json 

        search_results = data.get('search-results', {})
        
        if found_items_num == -1: 
            try:
                found_items_num = int(search_results.get('opensearch:totalResults', 0))
                if found_items_num == 0:
                    st.info("Aucun résultat trouvé sur Scopus pour cette requête.")
                    return []
            except (ValueError, TypeError):
                st.error("Réponse inattendue de Scopus (totalResults non trouvé ou invalide).")
                return []
        
        entries = search_results.get('entry')
        if entries:
            results_json.extend(entries)
            processed_items += len(entries)
        else: 
            if found_items_num > 0 and not entries and start_item < found_items_num :
                 st.warning(f"Scopus: {found_items_num} résultats attendus, mais 'entry' est vide à start_item {start_item}. Arrêt.")
            break 

        start_item += items_per_query
        
        if not entries and start_item > found_items_num : 
            break

    return results_json[:max_items]

def get_openalex_data(query, max_items=10000):
    url = 'https://api.openalex.org/works'
    email = "hal.dbm@listes.u-paris.fr" 
    params = {'filter': query, 'per-page': 200, 'mailto': email} 
    results_json = []
    next_cursor = "*" 

    retries = 3 
    
    while len(results_json) < max_items:
        current_try = 0
        if not next_cursor: 
            break
        
        params['cursor'] = next_cursor

        while current_try < retries:
            try:
                resp = requests.get(url, params=params, timeout=30) 
                resp.raise_for_status() 
                data = resp.json()
                
                if 'results' in data:
                    results_json.extend(data['results'])
                
                next_cursor = data.get('meta', {}).get('next_cursor')
                break 
            
            except requests.exceptions.RequestException as e:
                current_try += 1
                st.warning(f"Erreur OpenAlex (tentative {current_try}/{retries}): {e}. Réessai...")
                if current_try >= retries:
                    st.error(f"Échec de la récupération des données OpenAlex après {retries} tentatives.")
                    return results_json[:max_items] 
            except json.JSONDecodeError:
                current_try +=1
                st.warning(f"Erreur de décodage JSON OpenAlex (tentative {current_try}/{retries}). Réessai...")
                if current_try >= retries:
                    st.error("Échec du décodage JSON OpenAlex.")
                    return results_json[:max_items]
        
        if current_try >= retries: 
            break
            
    return results_json[:max_items] 


def get_pubmed_data(query, max_items=10000):
    fetch = PubMedFetcher()
    data = []
    try:
        pmids = fetch.pmids_for_query(query, retmax=max_items)
        
        for pmid in tqdm(pmids, desc="Récupération des articles PubMed"):
            try:
                article = fetch.article_by_pmid(pmid)
                pub_date_obj = article.history.get('pubmed') if article.history else None
                pub_date_str = pub_date_obj.date().isoformat() if pub_date_obj and hasattr(pub_date_obj, 'date') else 'N/A'
                
                data.append({
                    'Data source': 'pubmed',
                    'Title': article.title if article.title else "N/A",
                    'doi': article.doi if article.doi else None,
                    'id': pmid, 
                    'Source title': article.journal if article.journal else "N/A", 
                    'Date': pub_date_str
                })
            except Exception as e_article:
                st.warning(f"Erreur lors de la récupération des détails pour l'article PubMed (PMID: {pmid}): {e_article}")
                data.append({
                    'Data source': 'pubmed', 'Title': "Erreur de récupération", 'doi': None,
                    'id': pmid, 'Source title': "N/A", 'Date': "N/A"
                })
        return data
    except Exception as e_query:
        st.error(f"Erreur lors de la requête PMIDs à PubMed: {e_query}")
        return [] 

def convert_to_dataframe(data, source_name):
    if not data: 
        return pd.DataFrame() 
    df = pd.DataFrame(data)
    df['Data source'] = source_name 
    return df

def clean_doi(doi_value):
    if isinstance(doi_value, str):
        doi_value = doi_value.strip() 
        if doi_value.startswith('https://doi.org/'):
            return doi_value[len('https://doi.org/'):]
    return doi_value


def escapedSeq(term_char_list):
    for char in term_char_list:
        yield SOLR_ESCAPE_RULES.get(char, char)

def escapeSolrArg(term_to_escape):
    if not isinstance(term_to_escape, str):
        return "" 
    term_escaped = term_to_escape.replace('\\', r'\\')
    return "".join(list(escapedSeq(term_escaped)))


def normalise(text_to_normalise):
    if not isinstance(text_to_normalise, str):
        return "" 
    text_unaccented = unidecode(text_to_normalise)
    text_alphanum_spaces = re.sub(r'[^\w\s]', ' ', text_unaccented)
    text_normalised = re.sub(r'\s+', ' ', text_alphanum_spaces).lower().strip()
    return text_normalised

def compare_inex(norm_title1, norm_title2, threshold_strict=0.9, threshold_short=0.85, short_len_def=20):
    if not norm_title1 or not norm_title2: 
        return False
    
    shorter_len = min(len(norm_title1), len(norm_title2))
    current_threshold = threshold_strict if shorter_len > short_len_def else threshold_short
        
    matches = get_close_matches(norm_title1, [norm_title2], n=1, cutoff=current_threshold)
    return bool(matches)


def ex_in_coll(original_title_to_check, collection_df):
    if 'Titres' not in collection_df.columns or collection_df.empty:
        return False 
    
    match_df = collection_df[collection_df['Titres'] == original_title_to_check]
    if not match_df.empty:
        row = match_df.iloc[0]
        return [
            "Titre trouvé dans la collection : probablement déjà présent",
            original_title_to_check, 
            row.get('Hal_ids', ''),
            row.get('Types de dépôts', ''),
            row.get('HAL Link', ''), 
            row.get('HAL Ext ID', ''),
            row.get('HAL_URI', '') 
        ]
    return False

def inex_in_coll(normalised_title_to_check, original_title, collection_df):
    if 'nti' not in collection_df.columns or collection_df.empty:
        return False
        
    for idx, hal_title_norm_from_coll in enumerate(collection_df['nti']):
        if compare_inex(normalised_title_to_check, hal_title_norm_from_coll): 
            row = collection_df.iloc[idx]
            return [
                "Titre approchant trouvé dans la collection : à vérifier",
                row.get('Titres', ''), 
                row.get('Hal_ids', ''),
                row.get('Types de dépôts', ''),
                row.get('HAL Link', ''), 
                row.get('HAL Ext ID', ''),
                row.get('HAL_URI', '') 
            ]
    return False


def in_hal(title_solr_escaped_exact, original_title_to_check):
    default_return = ["Hors HAL", original_title_to_check, "", "", "", "", ""]
    try:
        query_exact = f'title_t:({title_solr_escaped_exact})' 
        
        r_exact_req = requests.get(f"{HAL_API_ENDPOINT}?q={query_exact}&rows=1&fl={HAL_FIELDS_TO_FETCH}", timeout=10)
        r_exact_req.raise_for_status()
        r_exact_json = r_exact_req.json()
        
        if r_exact_json.get('response', {}).get('numFound', 0) > 0:
            doc_exact = r_exact_json['response']['docs'][0]
            if any(original_title_to_check == hal_title for hal_title in doc_exact.get('title_s', [])):
                return [
                    "Titre trouvé dans HAL mais hors de la collection : affiliation probablement à corriger",
                    doc_exact.get('title_s', [""])[0],
                    doc_exact.get('docid', ''),
                    doc_exact.get('submitType_s', ''),
                    doc_exact.get('linkExtUrl_s', ''), 
                    doc_exact.get('linkExtId_s', ''),
                    doc_exact.get('uri_s', '') 
                ]

        query_approx = f'title_t:({escapeSolrArg(original_title_to_check)})'

        r_approx_req = requests.get(f"{HAL_API_ENDPOINT}?q={query_approx}&rows=1&fl={HAL_FIELDS_TO_FETCH}", timeout=10)
        r_approx_req.raise_for_status()
        r_approx_json = r_approx_req.json()

        if r_approx_json.get('response', {}).get('numFound', 0) > 0:
            doc_approx = r_approx_json['response']['docs'][0]
            title_orig_norm = normalise(original_title_to_check)
            if any(compare_inex(title_orig_norm, normalise(hal_title)) for hal_title in doc_approx.get('title_s', [])):
                return [
                    "Titre approchant trouvé dans HAL mais hors de la collection : vérifier les affiliations",
                    doc_approx.get('title_s', [""])[0],
                    doc_approx.get('docid', ''),
                    doc_approx.get('submitType_s', ''),
                    doc_approx.get('linkExtUrl_s', ''), 
                    doc_approx.get('linkExtId_s', ''),
                    doc_approx.get('uri_s', '') 
                ]
    except requests.exceptions.RequestException as e:
        _display_long_warning("Erreur de requête à l'API HAL", "titre", original_title_to_check, e)
    except (KeyError, IndexError, json.JSONDecodeError) as e_json:
        _display_long_warning("Structure de réponse HAL inattendue ou erreur JSON", "titre", original_title_to_check, e_json)
    
    return default_return


def statut_titre(title_to_check, collection_df):
    default_return_statut = ["Titre invalide", "", "", "", "", "", ""]
    if not isinstance(title_to_check, str) or not title_to_check.strip():
        return default_return_statut

    original_title = title_to_check 
    processed_title_for_norm = original_title
    try:
        if original_title.endswith("]") and '[' in original_title:
            match_bracket = re.match(r"(.*)\[", original_title) 
            if match_bracket:
                part_before_bracket = match_bracket.group(1).strip()
                if part_before_bracket : 
                    processed_title_for_norm = part_before_bracket
    except Exception: 
        processed_title_for_norm = original_title 

    title_normalised = normalise(processed_title_for_norm) 

    res_ex_coll = ex_in_coll(original_title, collection_df)
    if res_ex_coll: 
        return res_ex_coll

    res_inex_coll = inex_in_coll(title_normalised, original_title, collection_df)
    if res_inex_coll: 
        return res_inex_coll
        
    res_hal_global = in_hal(escapeSolrArg(original_title), original_title) 
    return res_hal_global


def statut_doi(doi_to_check, collection_df):
    default_return_doi = ["Pas de DOI valide", "", "", "", "", "", ""]
    if pd.isna(doi_to_check) or not str(doi_to_check).strip():
        return default_return_doi

    doi_cleaned_lower = str(doi_to_check).lower().strip()
    
    if 'DOIs' in collection_df.columns and not collection_df.empty:
        dois_coll_set = set(collection_df['DOIs'].dropna().astype(str).str.lower().str.strip())
        if doi_cleaned_lower in dois_coll_set:
            match_series = collection_df[collection_df['DOIs'].astype(str).str.lower().str.strip() == doi_cleaned_lower].iloc[0]
            return [
                "Dans la collection",
                match_series.get('Titres', ''), 
                match_series.get('Hal_ids', ''),
                match_series.get('Types de dépôts', ''),
                match_series.get('HAL Link', ''), 
                match_series.get('HAL Ext ID', ''),
                match_series.get('HAL_URI', '') 
            ]

    solr_doi_query_val = escapeSolrArg(doi_cleaned_lower.replace("https://doi.org/", ""))
    
    try:
        r_req = requests.get(f"{HAL_API_ENDPOINT}?q=doiId_s:\"{solr_doi_query_val}\"&rows=1&fl={HAL_FIELDS_TO_FETCH}", timeout=10)
        r_req.raise_for_status()
        r_json = r_req.json()
        
        if r_json.get('response', {}).get('numFound', 0) > 0:
            doc = r_json['response']['docs'][0]
            return [
                "Dans HAL mais hors de la collection", 
                doc.get('title_s', [""])[0], 
                doc.get('docid', ''),
                doc.get('submitType_s', ''),
                doc.get('linkExtUrl_s', ''), 
                doc.get('linkExtId_s', ''),
                doc.get('uri_s', '') 
            ]
    except requests.exceptions.RequestException as e:
        _display_long_warning("Erreur de requête à l'API HAL", "DOI", doi_to_check, e)
    except (KeyError, IndexError, json.JSONDecodeError) as e_json:
        _display_long_warning("Structure de réponse HAL inattendue ou erreur JSON", "DOI", doi_to_check, e_json)
        
    return default_return_doi 


def query_upw(doi_value):
    if pd.isna(doi_value) or not str(doi_value).strip():
        return {"Statut Unpaywall": "DOI manquant", "doi_interroge": str(doi_value)}
    
    doi_cleaned = str(doi_value).strip()
    email = "hal.dbm@listes.u-paris.fr" 
    
    try:
        req = requests.get(f"https://api.unpaywall.org/v2/{doi_cleaned}?email={email}", timeout=15)
        req.raise_for_status()
        res = req.json()
    except requests.exceptions.Timeout:
        return {"Statut Unpaywall": "timeout Unpaywall", "doi_interroge": doi_cleaned}
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return {"Statut Unpaywall": "non trouvé dans Unpaywall", "doi_interroge": doi_cleaned}
        return {"Statut Unpaywall": f"erreur HTTP Unpaywall ({e.response.status_code})", "doi_interroge": doi_cleaned}
    except requests.exceptions.RequestException as e:
        return {"Statut Unpaywall": f"erreur requête Unpaywall: {type(e).__name__}", "doi_interroge": doi_cleaned}
    except json.JSONDecodeError:
        return {"Statut Unpaywall": "erreur JSON Unpaywall", "doi_interroge": doi_cleaned}

    if res.get("message") and "isn't in Unpaywall" in res.get("message", "").lower():
        return {"Statut Unpaywall": "non trouvé dans Unpaywall (message API)", "doi_interroge": doi_cleaned}

    upw_info = {
        "Statut Unpaywall": "closed" if not res.get("is_oa") else "open",
        "oa_status": res.get("oa_status", ""), 
        "oa_publisher_license": "",
        "oa_publisher_link": "",
        "oa_repo_link": "",
        "publisher": res.get("publisher", ""),
        "doi_interroge": doi_cleaned 
    }

    best_oa_loc = res.get("best_oa_location")
    if best_oa_loc:
        host_type = best_oa_loc.get("host_type", "")
        license_val = best_oa_loc.get("license") 
        url_pdf = best_oa_loc.get("url_for_pdf")
        url_landing = best_oa_loc.get("url") 

        if host_type == "publisher":
            upw_info["oa_publisher_license"] = license_val if license_val else ""
            upw_info["oa_publisher_link"] = url_pdf or url_landing or ""
        elif host_type == "repository":
            upw_info["oa_repo_link"] = str(url_pdf or url_landing or "")

    return upw_info


def enrich_w_upw_parallel(input_df):
    if input_df.empty or 'doi' not in input_df.columns:
        st.warning("DataFrame vide ou colonne 'doi' manquante pour l'enrichissement Unpaywall.")
        upw_cols = ["Statut Unpaywall", "oa_status", "oa_publisher_license", "oa_publisher_link", "oa_repo_link", "publisher", "doi_interroge"]
        for col in upw_cols:
            if col not in input_df.columns:
                input_df[col] = pd.NA
        return input_df

    df_copy = input_df.copy() 
    df_copy.reset_index(drop=True, inplace=True)

    dois_to_query = df_copy['doi'].fillna("").tolist()

    results = []
    with ThreadPoolExecutor(max_workers=10) as executor: 
        results = list(tqdm(executor.map(query_upw, dois_to_query), total=len(dois_to_query), desc="Enrichissement Unpaywall"))

    if results:
        upw_results_df = pd.DataFrame(results)
        for col in upw_results_df.columns:
            if col not in df_copy.columns: 
                 df_copy[col] = pd.NA 
            df_copy[col] = upw_results_df[col].values 
    else: 
        st.info("Aucun résultat d'enrichissement Unpaywall à ajouter.")
        upw_cols = ["Statut Unpaywall", "oa_status", "oa_publisher_license", "oa_publisher_link", "oa_repo_link", "publisher", "doi_interroge"]
        for col in upw_cols:
            if col not in df_copy.columns:
                df_copy[col] = pd.NA
                
    return df_copy


def add_permissions(row_series_data):
    doi_val = row_series_data.get('doi') 
    if pd.isna(doi_val) or not str(doi_val).strip():
        return "DOI manquant pour permissions"

    doi_cleaned_for_api = str(doi_val).strip()
    permissions_api_url = f"https://bg.api.oa.works/permissions/{doi_cleaned_for_api}"
    try:
        req = requests.get(permissions_api_url, timeout=15)
        req.raise_for_status() 
        res_json = req.json()
        
        best_permission_info = res_json.get("best_permission") 
        if not best_permission_info:
            return "Aucune permission trouvée (oa.works)"

    except requests.exceptions.Timeout:
        return f"Timeout permissions (oa.works) pour DOI {doi_cleaned_for_api}"
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if hasattr(e, 'response') and e.response is not None else 'N/A'
        if status_code == 404:
            return f"Permissions non trouvées (404 oa.works) pour DOI {doi_cleaned_for_api}"
        elif status_code == 501: 
            return f"Permissions API non applicable pour ce type de document (501 oa.works) pour DOI {doi_cleaned_for_api}"
        else:
            return f"Erreur HTTP {status_code} permissions (oa.works) pour DOI {doi_cleaned_for_api}: {str(e)}"
    except requests.exceptions.RequestException as e:
        return f"Erreur requête permissions (oa.works) pour DOI {doi_cleaned_for_api}: {type(e).__name__}"
    except json.JSONDecodeError:
        return f"Erreur JSON permissions (oa.works) pour DOI {doi_cleaned_for_api}"

    locations_allowed = best_permission_info.get("locations", [])
    if not any("repository" in str(loc).lower() for loc in locations_allowed):
        return "Dépôt en archive non listé dans les permissions (oa.works)"

    version_allowed = best_permission_info.get("version", "Version inconnue")
    licence_info = best_permission_info.get("licence", "Licence inconnue")
    embargo_months_val = best_permission_info.get("embargo_months") 

    embargo_display_str = "Pas d'embargo spécifié"
    if isinstance(embargo_months_val, int):
        if embargo_months_val == 0:
            embargo_display_str = "Pas d'embargo"
        elif embargo_months_val > 0:
            embargo_display_str = f"{embargo_months_val} mois d'embargo"
    
    if str(version_allowed).lower() in ["publishedversion", "acceptedversion"]:
        return f"Version autorisée (oa.works): {version_allowed} ; Licence: {licence_info} ; Embargo: {embargo_display_str}"
    
    return f"Info permission (oa.works): {version_allowed} ; {licence_info} ; {embargo_display_str}"


def add_permissions_parallel(input_df):
    if input_df.empty or 'doi' not in input_df.columns: 
        st.warning("DataFrame vide ou colonne 'doi' manquante pour l'ajout des permissions.")
        if 'deposit_condition' not in input_df.columns and not input_df.empty:
             input_df['deposit_condition'] = pd.NA 
        return input_df

    df_copy = input_df.copy() 
    
    if 'deposit_condition' not in df_copy.columns:
        df_copy['deposit_condition'] = pd.NA

    def apply_add_permissions_to_row(row_as_series):
        return add_permissions(row_as_series)

    rows_as_series_list = [row_data for _, row_data in df_copy.iterrows()]
    
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor: 
        results = list(tqdm(executor.map(apply_add_permissions_to_row, rows_as_series_list), total=len(df_copy), desc="Ajout des permissions de dépôt"))

    if results:
        df_copy['deposit_condition'] = results
    else: 
        st.info("Aucun résultat d'ajout de permissions.")
        if 'deposit_condition' not in df_copy.columns:
            df_copy['deposit_condition'] = pd.NA
            
    return df_copy


def deduce_todo(row_data):
    doi_val = row_data.get("doi") 
    has_doi = pd.notna(doi_val) and str(doi_val).strip() != ""

    statut_hal_val = str(row_data.get("Statut_HAL", "")).strip()
    type_depot_hal_val = str(row_data.get("type_dépôt_si_trouvé", "")).strip().lower()
    id_hal_val = str(row_data.get("identifiant_hal_si_trouvé", "")).strip()
    hal_uri_val = str(row_data.get("HAL_URI", "")).strip() 

    statut_upw_val = str(row_data.get("Statut Unpaywall", "")).strip().lower()
    oa_repo_link_val = str(row_data.get("oa_repo_link", "") or "").strip()
    oa_publisher_link_val = str(row_data.get("oa_publisher_link", "") or "").strip()
    deposit_condition_val = str(row_data.get("deposit_condition", "")).lower()

    is_hal_ok_with_file = (statut_hal_val == "Dans la collection" and type_depot_hal_val == "file") or \
                          (statut_hal_val == "Titre trouvé dans la collection : probablement déjà présent" and type_depot_hal_val == "file")
    
    needs_hal_creation = (statut_hal_val in ["Hors HAL", "Titre incorrect, probablement absent de HAL"] and not id_hal_val) or \
                         (statut_hal_val == "Pas de DOI valide" and not id_hal_val)

    is_in_collection_as_notice = (
        (statut_hal_val == "Dans la collection" or \
         statut_hal_val == "Titre trouvé dans la collection : probablement déjà présent") and \
        type_depot_hal_val == "notice" and \
        id_hal_val
    )
    
    needs_affiliation_check = statut_hal_val in [
        "Dans HAL mais hors de la collection",
        "Titre trouvé dans HAL mais hors de la collection : affiliation probablement à corriger",
        "Titre approchant trouvé dans HAL mais hors de la collection : vérifier les affiliations"
    ]

    can_deposit_published_oaw = "version autorisée (oa.works): publishedversion" in deposit_condition_val
    can_deposit_accepted_oaw = "version autorisée (oa.works): acceptedversion" in deposit_condition_val

    action_parts = []
    primary_hal_action_taken = False 
    notice_link_text = hal_uri_val if hal_uri_val else (f"https://hal.science/{id_hal_val}" if id_hal_val else id_hal_val)

    if is_hal_ok_with_file:
        base_message = "✅ Dépôt HAL OK (avec fichier)."
        if "probablement déjà déposé" in statut_hal_val:
            base_message = "✅ Titre probablement déjà déposé dans la collection (avec fichier)."
        action_parts.append(base_message)
        if needs_affiliation_check: 
            affiliation_text = "🏷️ Affiliation à vérifier dans HAL"
            if hal_uri_val: affiliation_text += f" : {hal_uri_val}"
            elif id_hal_val: affiliation_text += f" : https://hal.science/{id_hal_val}"
            action_parts.append(affiliation_text)
        # Early exit if HAL is OK with file, no further Unpaywall/OA.works actions are needed for the "Action" column.
        final_actions_ok = []
        seen_ok = set()
        for part in action_parts:
            if part and part not in seen_ok:
                final_actions_ok.append(part)
                seen_ok.add(part)
        return " | ".join(final_actions_ok)

    if needs_hal_creation:
        primary_hal_action_taken = True
        if has_doi and can_deposit_published_oaw:
            action_parts.append("📥 Créer la notice et déposer la version éditeur dans HAL" + (f" (source: {oa_publisher_link_val})" if oa_publisher_link_val else "."))
        elif has_doi and can_deposit_accepted_oaw:
            action_parts.append("📥 Créer la notice et déposer la version postprint dans HAL.")
        else: 
            action_parts.append("📥 Créer la notice HAL.") # Simplified
            # If generic creation, add Unpaywall info if it provides an OA link not already covered
            # This part is now handled later if has_doi is true and not is_hal_ok_with_file

    elif is_in_collection_as_notice:
        primary_hal_action_taken = True
        base_text = f"📄 Notice HAL ({notice_link_text}) sans fichier."
        deposit_suggestion = ""
        if has_doi and can_deposit_published_oaw:
            deposit_suggestion = "Déposer la version éditeur" + (f" (source: {oa_publisher_link_val})" if oa_publisher_link_val else ".")
        elif has_doi and can_deposit_accepted_oaw:
            deposit_suggestion = "Déposer la version postprint."
        # If no specific deposit version, the base_text itself is the action.
        # The phrase "Vérifier possibilité d'ajout de fichier" is removed.
        action_parts.append(f"{base_text} {deposit_suggestion}".strip())


    elif statut_hal_val == "Titre approchant trouvé dans la collection : à vérifier":
        primary_hal_action_taken = True
        action_parts.append(f"🧐 Titre approchant dans la collection ({notice_link_text}).") # Removed "Vérifier si c'est une variante..."
        if type_depot_hal_val == "notice" and id_hal_val : # If this approaching title is a notice
            action_parts.append(f"Cette notice HAL est sans fichier.")
            deposit_suggestion_for_approaching = ""
            if has_doi and can_deposit_published_oaw:
                deposit_suggestion_for_approaching = "Si correspondance confirmée, déposer la version éditeur" + (f" (source: {oa_publisher_link_val})" if oa_publisher_link_val else ".")
            elif has_doi and can_deposit_accepted_oaw:
                deposit_suggestion_for_approaching = "Si correspondance confirmée, déposer la version postprint."
            # Removed "Option pour ce HAL ID: Vérifier possibilité d'ajout de fichier."
            if deposit_suggestion_for_approaching:
                action_parts.append(deposit_suggestion_for_approaching)


    if needs_affiliation_check:
        affiliation_text = "🏷️ Affiliation à vérifier dans HAL"
        if hal_uri_val: affiliation_text += f" : {hal_uri_val}"
        elif id_hal_val: affiliation_text += f" : https://hal.science/{id_hal_val}"
        
        is_affiliation_msg_present = any(affiliation_text_part in " | ".join(action_parts) for affiliation_text_part in ["Affiliation à vérifier", notice_link_text if notice_link_text != id_hal_val else ""])
        if not is_affiliation_msg_present:
            action_parts.append(affiliation_text)
        if not primary_hal_action_taken and not is_hal_ok_with_file : primary_hal_action_taken = True
            
    if not primary_hal_action_taken and not is_hal_ok_with_file:
        if statut_hal_val == "Titre invalide":
            action_parts.append("❌ Titre considéré invalide par le script. Vérifier/corriger le titre source.")
        # "Titre approchant..." without being a notice was handled above if it's the primary action.

    # --- Add complementary informational messages (Unpaywall, OA.works errors/infos) ---
    # Only if HAL is NOT OK with file AND a DOI exists for these services to be queried
    if not is_hal_ok_with_file and has_doi:
        is_specific_deposit_action_formed_using_oaw = can_deposit_published_oaw or can_deposit_accepted_oaw
        
        # Unpaywall info (if not used in a primary deposit action)
        if oa_repo_link_val and oa_repo_link_val not in " | ".join(action_parts):
            action_parts.append(f"🔗 OA via archive (Unpaywall): {oa_repo_link_val}.")
        
        if oa_publisher_link_val and not (primary_hal_action_taken and (can_deposit_published_oaw or can_deposit_accepted_oaw) and oa_publisher_link_val in " | ".join(action_parts)):
             if not any(oa_publisher_link_val in act for act in action_parts): # Avoid repeating if already part of a deposit suggestion
                 action_parts.append(f"🔗 Lien éditeur (Unpaywall): {oa_publisher_link_val}.")

        
        is_oa_path_identified_for_contact = is_specific_deposit_action_formed_using_oaw or \
                                oa_repo_link_val or \
                                oa_publisher_link_val or \
                                any(info_type in deposit_condition_val for info_type in ["501 oa.works", "404 oa.works"]) or \
                                any("déposer la version" in act for act in action_parts)

        if statut_upw_val == "closed" and not is_oa_path_identified_for_contact :
            action_parts.append("📧 Article fermé (Unpaywall) et pas de permission claire. Contacter auteur pour LRN/dépôt.")
            
    if not action_parts:
        return "🛠️ À vérifier manuellement (aucune action spécifique déduite)."

    final_actions = []
    seen = set()
    for part in action_parts:
        if part and part not in seen: 
            final_actions.append(part)
            seen.add(part)
    return " | ".join(final_actions)


def addCaclLinkFormula(pre_url_str, post_url_str, text_for_link):
    if post_url_str and text_for_link: 
        pre_url_cleaned = str(pre_url_str if pre_url_str else "").strip()
        post_url_cleaned = str(post_url_str).strip()
        text_cleaned = str(text_for_link).strip().replace('"', '""') 

        full_url = f"{pre_url_cleaned}{post_url_cleaned}"
        
        display_text_final = text_cleaned
        if len(text_cleaned) > 50: 
            display_text_final = text_cleaned[:47] + "..."
            
        return f'=HYPERLINK("{full_url}";"{display_text_final}")'
    return "" 


def check_df(input_df_to_check, hal_collection_df, progress_bar_st=None, progress_text_st=None):
    if input_df_to_check.empty:
        st.info("Le DataFrame d'entrée pour check_df est vide. Aucune vérification HAL à effectuer.")
        hal_output_cols = ['Statut_HAL', 'titre_HAL_si_trouvé', 'identifiant_hal_si_trouvé', 
                           'type_dépôt_si_trouvé', 'HAL Link', 'HAL Ext ID', 'HAL_URI']
        for col_name in hal_output_cols:
            if col_name not in input_df_to_check.columns:
                input_df_to_check[col_name] = pd.NA
        return input_df_to_check

    df_to_process = input_df_to_check.copy() 

    statuts_hal_list = []
    titres_hal_list = []
    ids_hal_list = []
    types_depot_hal_list = []
    links_hal_list = [] 
    ext_ids_hal_list = []
    hal_uris_list = [] 


    total_rows_to_process = len(df_to_process)
    for index, row_to_check in tqdm(df_to_process.iterrows(), total=total_rows_to_process, desc="Vérification HAL (check_df)"):
        doi_value_from_row = row_to_check.get('doi') 
        title_value_from_row = row_to_check.get('Title') 

        hal_status_result = ["Pas de DOI valide", "", "", "", "", "", ""] 
        
        if pd.notna(doi_value_from_row) and str(doi_value_from_row).strip():
            hal_status_result = statut_doi(str(doi_value_from_row), hal_collection_df)
        
        if hal_status_result[0] not in ("Dans la collection", "Dans HAL mais hors de la collection"):
            if pd.notna(title_value_from_row) and str(title_value_from_row).strip():
                hal_status_result = statut_titre(str(title_value_from_row), hal_collection_df)
            elif not (pd.notna(doi_value_from_row) and str(doi_value_from_row).strip()): 
                hal_status_result = ["Données d'entrée insuffisantes (ni DOI ni Titre)", "", "", "", "", "", ""]
        
        statuts_hal_list.append(hal_status_result[0])
        titres_hal_list.append(hal_status_result[1]) 
        ids_hal_list.append(hal_status_result[2])
        types_depot_hal_list.append(hal_status_result[3])
        links_hal_list.append(hal_status_result[4]) 
        ext_ids_hal_list.append(hal_status_result[5])
        hal_uris_list.append(hal_status_result[6]) 
        
        if progress_bar_st is not None and progress_text_st is not None:
            current_progress_val = (index + 1) / total_rows_to_process
            progress_bar_st.progress(int(current_progress_val * 100))

    df_to_process['Statut_HAL'] = statuts_hal_list
    df_to_process['titre_HAL_si_trouvé'] = titres_hal_list
    df_to_process['identifiant_hal_si_trouvé'] = ids_hal_list
    df_to_process['type_dépôt_si_trouvé'] = types_depot_hal_list
    df_to_process['HAL Link'] = links_hal_list 
    df_to_process['HAL Ext ID'] = ext_ids_hal_list
    df_to_process['HAL_URI'] = hal_uris_list 
    
    if progress_bar_st: progress_bar_st.progress(100) 
    return df_to_process


class HalCollImporter:
    def __init__(self, collection_code: str, start_year_val=None, end_year_val=None):
        self.collection_code = str(collection_code).strip() if collection_code else "" 
        self.start_year = start_year_val if start_year_val is not None else DEFAULT_START_YEAR
        self.end_year = end_year_val if end_year_val is not None else DEFAULT_END_YEAR 
        
        self.num_docs_in_collection = self._get_num_docs()

    def _get_num_docs(self):
        try:
            query_params_count = {
                'q': '*:*', 
                'fq': f'publicationDateY_i:[{self.start_year} TO {self.end_year}]',
                'rows': 0, 
                'wt': 'json'
            }
            base_search_url = f"{HAL_API_ENDPOINT}{self.collection_code}/" if self.collection_code else HAL_API_ENDPOINT
            
            response_count = requests.get(base_search_url, params=query_params_count, timeout=15)
            response_count.raise_for_status()
            return response_count.json().get('response', {}).get('numFound', 0)
        except requests.exceptions.RequestException as e:
            st.error(f"Erreur API HAL (comptage) pour '{self.collection_code or 'HAL global'}': {e}")
            return 0
        except (KeyError, json.JSONDecodeError):
            st.error(f"Réponse API HAL (comptage) inattendue pour '{self.collection_code or 'HAL global'}'.")
            return 0

    def import_data(self):
        expected_cols = ['Hal_ids', 'DOIs', 'Titres', 'Types de dépôts', 
                         'HAL Link', 'HAL Ext ID', 'HAL_URI', 'nti']
        if self.num_docs_in_collection == 0:
            st.info(f"Aucun document trouvé pour la collection '{self.collection_code or 'HAL global'}' entre {self.start_year} et {self.end_year}.")
            return pd.DataFrame(columns=expected_cols)

        all_docs_list = []
        rows_per_api_page = 1000 
        current_api_cursor = "*" 

        base_search_url = f"{HAL_API_ENDPOINT}{self.collection_code}/" if self.collection_code else HAL_API_ENDPOINT

        with tqdm(total=self.num_docs_in_collection, desc=f"Import HAL ({self.collection_code or 'Global'})") as pbar_hal:
            while True:
                query_params_page = {
                    'q': '*:*',
                    'fq': f'publicationDateY_i:[{self.start_year} TO {self.end_year}]',
                    'fl': HAL_FIELDS_TO_FETCH, 
                    'rows': rows_per_api_page,
                    'sort': 'docid asc', 
                    'cursorMark': current_api_cursor,
                    'wt': 'json'
                }
                try:
                    response_page = requests.get(base_search_url, params=query_params_page, timeout=45) 
                    response_page.raise_for_status()
                    data_page = response_page.json()
                except requests.exceptions.RequestException as e:
                    st.error(f"Erreur API HAL (import page, curseur {current_api_cursor}): {e}")
                    break 
                except json.JSONDecodeError:
                    st.error(f"Erreur décodage JSON (import page HAL, curseur {current_api_cursor}).")
                    break

                docs_on_current_page = data_page.get('response', {}).get('docs', [])
                if not docs_on_current_page: 
                    break

                for doc_data in docs_on_current_page:
                    hal_titles_list = doc_data.get('title_s', [""]) 
                    if not isinstance(hal_titles_list, list): hal_titles_list = [str(hal_titles_list)] 

                    for title_item in hal_titles_list:
                        all_docs_list.append({
                            'Hal_ids': doc_data.get('docid', ''),
                            'DOIs': str(doc_data.get('doiId_s', '')).lower() if doc_data.get('doiId_s') else '', 
                            'Titres': str(title_item), 
                            'Types de dépôts': doc_data.get('submitType_s', ''),
                            'HAL Link': doc_data.get('linkExtUrl_s', ''), 
                            'HAL Ext ID': doc_data.get('linkExtId_s', ''),
                            'HAL_URI': doc_data.get('uri_s', '') 
                        })
                pbar_hal.update(len(docs_on_current_page)) 

                next_api_cursor = data_page.get('nextCursorMark')
                if current_api_cursor == next_api_cursor or not next_api_cursor:
                    break
                current_api_cursor = next_api_cursor
        
        if not all_docs_list: 
             return pd.DataFrame(columns=expected_cols)

        df_collection_hal = pd.DataFrame(all_docs_list)
        if 'Titres' in df_collection_hal.columns:
            df_collection_hal['nti'] = df_collection_hal['Titres'].apply(normalise)
        else: 
            df_collection_hal['nti'] = ""
            
        return df_collection_hal


def merge_rows_with_sources(grouped_data):
    merged_ids_str = '|'.join(map(str, grouped_data['id'].dropna().astype(str).unique())) if 'id' in grouped_data.columns else None
    merged_sources_str = '|'.join(grouped_data['Data source'].dropna().astype(str).unique()) if 'Data source' in grouped_data.columns else None
    merged_row_content_dict = {}

    for column_name in grouped_data.columns:
        if column_name not in ['id', 'Data source']:
            unique_values_in_col = grouped_data[column_name].dropna().astype(str).unique()
            
            if len(unique_values_in_col) == 1:
                merged_row_content_dict[column_name] = unique_values_in_col[0]
            elif len(unique_values_in_col) > 1:
                merged_row_content_dict[column_name] = '|'.join(sorted(list(unique_values_in_col)))
            else: 
                merged_row_content_dict[column_name] = pd.NA 
    
    if merged_ids_str is not None: merged_row_content_dict['id'] = merged_ids_str
    if merged_sources_str is not None: merged_row_content_dict['Data source'] = merged_sources_str
    
    return pd.Series(merged_row_content_dict)


def get_authors_from_crossref(doi_value):
    if pd.isna(doi_value) or not str(doi_value).strip():
        return ["DOI manquant pour Crossref"]

    doi_cleaned_for_api = str(doi_value).strip()
    headers = {
        'User-Agent': 'c2LabHAL/1.0 (mailto:YOUR_EMAIL@example.com; https://github.com/GuillaumeGodet/c2labhal)', 
        'Accept': 'application/json'
    }
    url_crossref = f"https://api.crossref.org/works/{doi_cleaned_for_api}"
    
    try:
        response_crossref = requests.get(url_crossref, headers=headers, timeout=10)
        response_crossref.raise_for_status()
        data_crossref = response_crossref.json()
    except requests.exceptions.Timeout:
        return ["Timeout Crossref"]
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if hasattr(e.response, 'status_code') else 'N/A'
        return [f"Erreur HTTP Crossref ({status_code})"]
    except requests.exceptions.RequestException as e_req:
        return [f"Erreur requête Crossref: {type(e_req).__name__}"]
    except json.JSONDecodeError:
        return ["Erreur JSON Crossref"]

    authors_data_list = data_crossref.get('message', {}).get('author', [])
    if not authors_data_list:
        return [] 

    author_names_list = []
    for author_entry in authors_data_list:
        if not isinstance(author_entry, dict): continue 

        given_name = str(author_entry.get('given', '')).strip()
        family_name = str(author_entry.get('family', '')).strip()
        
        full_name = ""
        if given_name and family_name:
            full_name = f"{given_name} {family_name}"
        elif family_name: 
            full_name = family_name
        elif given_name: 
            full_name = given_name
        
        if full_name: 
            author_names_list.append(full_name)

    return author_names_list


def normalize_name(name_to_normalize):
    if not isinstance(name_to_normalize, str): return ""
    
    name_lower = name_to_normalize.strip().lower()
    name_unaccented = ''.join(c for c in unicodedata.normalize('NFD', name_lower) 
                              if unicodedata.category(c) != 'Mn')
    name_cleaned_spaces = name_unaccented.replace('-', ' ').replace('.', ' ')
    name_single_spaced = re.sub(r'\s+', ' ', name_cleaned_spaces).strip()

    if ',' in name_single_spaced:
        parts = [part.strip() for part in name_single_spaced.split(',', 1)]
        if len(parts) == 2 and parts[0] and parts[1]: 
            return f"{parts[1]} {parts[0]}"
            
    return name_single_spaced


def get_initial_form(normalised_author_name):
    if not normalised_author_name: return ""
    
    name_parts = normalised_author_name.split()
    if len(name_parts) >= 2: 
        return f"{name_parts[0][0]} {name_parts[-1]}" 
    elif len(name_parts) == 1: 
        return normalised_author_name 
    return "" 
