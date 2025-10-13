import requests
import pandas as pd
import time
import streamlit as st
from urllib.parse import urlencode

# Le point d'entrée de l'API de recherche HAL pour les documents
HAL_SEARCH_API = "http://api.archives-ouvertes.fr/search/"
# Le point d'entrée de l'API de référence HAL pour les auteurs (pour les détails)
HAL_AUTHOR_API = "http://api.archives-ouvertes.fr/ref/author/"

# --- Fonctions d'extraction HAL (Adaptées à la Collection) ---

def fetch_publications_for_collection(collection_code, years="", fields="structHasAuthId_fs"):
    """
    Récupère tous les documents (publications) pour un code de collection donné
    en utilisant l'API de recherche HAL.
    """
    all_docs = []
    rows = 10000  # Nombre maximal de documents par requête
    start = 0
    
    # La requête utilise le code de collection dans le chemin de l'URL pour un ciblage précis
    query_params = {
        'q': '*:*',
        'wt': 'json',
        'fl': fields,
        'rows': rows
    }

    if years:
        query_params['fq'] = f'producedDateY_i:{years}'
        
    st.toast(f"Démarrage de la récupération pour la collection '{collection_code}' (Année(s): {years if years else 'Toutes'})")

    while True:
        query_params['start'] = start
        
        # URL de l'API de recherche HAL, avec le code de collection dans le chemin
        url = f"{HAL_SEARCH_API}{collection_code}/?{urlencode(query_params)}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            docs = data.get('response', {}).get('docs', [])
            num_found = data.get('response', {}).get('numFound', 0)
            all_docs.extend(docs)
            
            found_publications = len(all_docs)
            
            if found_publications >= num_found or not docs:
                break
            
            start += rows
            time.sleep(0.5) 

        except requests.exceptions.RequestException as e:
            st.error(f"Erreur lors de la requête API (Recherche publications): {e}")
            break
            
    return all_docs

def extract_author_ids(publications):
    """
    Extrait les identifiants uniques des formes-auteurs (docid du référentiel auteur)
    à partir de la liste des publications.
    """
    author_ids = set()
    for doc in publications:
        authors = doc.get('structHasAuthId_fs', [])
        for author_str in authors:
            # Le format est typiquement "STRUCTID_HALID_JoinSep_AUTHORID_FacetSep"
            parts = author_str.split('_JoinSep_')
            if len(parts) > 1:
                # Capture de l'ID de la forme-auteur (ce qui correspond au docid dans /ref/author)
                author_id_part = parts[1].split('_FacetSep')[0]
                author_ids.add(author_id_part)
                
    return list(author_ids)

def fetch_author_details(author_ids, fields):
    """
    Récupère les détails de chaque forme-auteur (par son docid)
    en utilisant l'API de référence HAL.
    """
    authors_details = []
    chunk_size = 50 
    total_authors = len(author_ids)
    
    st.toast(f"Récupération des détails pour {total_authors} formes-auteurs...")
    
    for i in range(0, total_authors, chunk_size):
        chunk = author_ids[i:i + chunk_size]
        # Construction de la requête Solr pour les docid
        docid_query = '%22 OR docid:%22'.join(chunk)
        
        query_params = {
            'q': f'docid:"{docid_query}"',
            'wt': 'json',
            'fl': fields
        }
        
        url = f"{HAL_AUTHOR_API}?{urlencode(query_params)}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            docs = data.get('response', {}).get('docs', [])
            
            # Application de la logique de transformation du statut de validation (comme dans le script JS original)
            for doc in docs:
                if 'valid_s' in doc:
                    validity_status = doc['valid_s']
                    if validity_status == "VALID":
                        doc['valid_s'] = "forme auteur principale d'un IdHAL"
                    elif validity_status == "OLD":
                        doc['valid_s'] = "forme auteur alternative d'un IdHAL"
                    elif validity_status == "INCOMING":
                        doc['valid_s'] = "forme auteur sans IdHAL associé"
                authors_details.append(doc)

            time.sleep(0.3) 

        except requests.exceptions.RequestException as e:
            st.error(f"Erreur lors de la requête API (Détails Auteur): {e}")
            break
            
    return authors_details

def get_all_author_forms_data(collection_code, years="", fields_list="docid,fullName_s,valid_s,halId_s,orcidId_s,firstName_s,lastName_s"):
    """
    Fonction principale pour orchestrer l'extraction de TOUTES les formes-auteurs 
    (sans déduplication) pour une collection.
    """
    
    # 1. Récupérer les publications et les identifiants d'auteurs
    publications = fetch_publications_for_collection(collection_code, years)
    
    # --- AJOUT DE DÉBOGAGE 1 ---
    print(f"DEBUG: Nombre de publications trouvées par l'API: {len(publications)}")
    # --- FIN DÉBOGAGE 1 ---
    
    if not publications:
        return pd.DataFrame()
        
    author_ids = extract_author_ids(publications)

    # --- AJOUT DE DÉBOGAGE 2 ---
    print(f"DEBUG: Nombre d'IDs de formes-auteurs extraits des publications: {len(author_ids)}")
    # --- FIN DÉBOGAGE 2 ---
    
    if not author_ids:
        st.info("Aucune forme-auteur trouvée pour la collection et l'année(s) spécifiées.")
        return pd.DataFrame()

    # 2. Récupérer les détails des auteurs (formes-auteurs)
    author_details = fetch_author_details(author_ids, fields_list)

    if not author_details:
        return pd.DataFrame()

    # 3. Créer le DataFrame final (pas de déduplication)
    df = pd.DataFrame(author_details)
    
    # S'assurer que les colonnes sont dans l'ordre demandé
    requested_fields = fields_list.split(',')
    final_cols = [col for col in requested_fields if col in df.columns]
    
    return df[final_cols]

# --- Fonctions utilitaires pour Streamlit ---
@st.cache_data
def convert_df(df):
    """Convertit un DataFrame en CSV pour le téléchargement."""
    # Utilisation du point-virgule comme séparateur pour la compatibilité Excel en français
    return df.to_csv(index=False, sep=';').encode('utf-8')

def build_fields_list(fields_selected):
    """Construit la chaîne de champs à partir des sélections Streamlit."""
    # Ajoute les champs obligatoires pour l'affichage
    mandatory_fields = ['docid', 'fullName_s', 'valid_s']
    
    final_fields = list(set(mandatory_fields + fields_selected))
    
    return ','.join(final_fields)

# --- Application Streamlit ---
def main():
    st.set_page_config(page_title="Extracteur Formes-Auteurs HAL (Collection)", layout="wide")
    st.title("Extracteur de Formes-Auteurs par Collection HAL")
    st.markdown("Extrait **toutes les formes-auteurs** rattachées aux publications de la collection. Utile pour l'identification des doublons.")

    # Options des champs (similaire aux champs courants du référentiel)
    all_available_fields = [
        'halId_s', 'orcidId_s', 'firstName_s', 'lastName_s', 
        'email_s', 'hasCV_bool', 'birthDateY_i'
    ]
    
    with st.sidebar:
        st.header("Paramètres de l'Extraction")
        collection_code = st.text_input("Code Collection HAL (ex: CRAO, LEMNA)", value="LEMNA")
        years = st.text_input("Période visée (ex: [2016 TO 2018], 2023)", value="")
        
        st.subheader("Champs d'Auteur à Récupérer")
        
        default_fields = ['halId_s', 'orcidId_s', 'firstName_s', 'lastName_s']
        
        fields_selected = st.multiselect(
            "Sélectionnez les champs additionnels (le 'docid' est l'ID de la forme-auteur)", 
            options=all_available_fields,
            default=default_fields
        )
        
        # Bouton d'extraction
        if st.button("Lancer l'Extraction", type="primary"):
            if not collection_code:
                st.sidebar.error("Veuillez entrer un code de collection HAL.")
                return
            
            # Préparer les champs pour l'API
            api_fields = build_fields_list(fields_selected)
            
            with st.spinner(f"Récupération en cours pour la collection **{collection_code}**..."):
                # Exécution de la logique d'extraction sans déduplication
                df_authors = get_all_author_forms_data(collection_code, years, api_fields)
            
            if df_authors.empty:
                st.warning("Aucun résultat trouvé pour cette collection et ces critères.")
            else:
                st.session_state['df_authors'] = df_authors
                st.session_state['collection_code'] = collection_code
                st.session_state['years'] = years

    # --- Affichage des résultats ---
    if 'df_authors' in st.session_state and not st.session_state['df_authors'].empty:
        df_authors = st.session_state['df_authors']
        collection_code = st.session_state['collection_code']
        years = st.session_state['years']
        
        st.subheader(f"Résultats pour Collection : **{collection_code}** ({'Année(s) : **' + years + '**' if years else '**Toutes années**'})")
        
        # L'affichage des doublons est utile pour repérer les doublons par nom
        df_display = df_authors.sort_values(by='fullName_s') 
        
        st.info(f"Nombre total de **formes-auteurs** trouvées : **{len(df_authors)}**")
        st.markdown("_Les lignes ayant le même `fullName_s` mais un `docid` (ID de forme-auteur) différent sont des doublons potentiels._")
        
        # Afficher le DataFrame
        st.dataframe(df_display, use_container_width=True)
        
        # Préparer et afficher le bouton de téléchargement CSV
        csv_data = convert_df(df_authors)
        filename = f'toutes_formes_auteurs_{collection_code}_{years.replace(" ", "_") if years else "all"}.csv'
        
        st.download_button(
            label="💾 Télécharger la liste de TOUTES les formes-auteurs (CSV)",
            data=csv_data,
            file_name=filename,
            mime='text/csv',
            key='download_button'
        )
        st.markdown("_Le séparateur est le point-virgule (`;`) pour Excel._")

if __name__ == '__main__':
    main()
