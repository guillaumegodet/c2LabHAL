import requests
import pandas as pd
import time
import streamlit as st
from urllib.parse import urlencode

# Le point d'entrée de l'API de recherche HAL pour les documents
HAL_SEARCH_API = "http://api.archives-ouvertes.fr/search/"
# Le point d'entrée de l'API de référence HAL pour les auteurs (pour les détails)
HAL_AUTHOR_API = "http://api.archives-ouvertes.fr/ref/author/"

# --- Fonctions d'extraction HAL ---

def fetch_publications_for_collection(collection_code, years="", fields="structHasAuthId_fs"):
    """
    Récupère tous les documents (publications) pour un code de collection donné
    en utilisant l'API de recherche HAL.
    """
    all_docs = []
    rows = 10000  # Nombre maximal de documents par requête
    start = 0
    
    # Construction des filtres de la requête
    query_params = {
        'q': '*:*',
        'wt': 'json',
        'fl': fields,
        'rows': rows,
        'fq': f'collCode_s:"{collection_code}"'
    }

    if years:
        query_params['fq'] += f' AND producedDateY_i:{years}'

    st.toast(f"Démarrage de la récupération pour la collection '{collection_code}' (Année(s): {years if years else 'Toutes'})")

    while True:
        query_params['start'] = start
        
        # L'URL de l'API de recherche HAL (pour la collection, le code est dans le chemin)
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
    Extrait les identifiants uniques des auteurs à partir de la liste des publications.
    """
    author_ids = set()
    for doc in publications:
        authors = doc.get('structHasAuthId_fs', [])
        for author_str in authors:
            parts = author_str.split('_JoinSep_')
            if len(parts) > 1:
                author_id_part = parts[1].split('_FacetSep')[0]
                author_ids.add(author_id_part)
                
    return list(author_ids)

def fetch_author_details(author_ids, fields="docid,fullName_s,valid_s,halId_s,orcidId_s,firstName_s,lastName_s"):
    """
    Récupère les détails de chaque auteur (forme-auteur) à partir de leur docid
    en utilisant l'API de référence HAL.
    """
    authors_details = []
    chunk_size = 50 
    total_authors = len(author_ids)
    
    st.toast(f"Récupération des détails pour {total_authors} auteurs...")
    
    for i in range(0, total_authors, chunk_size):
        chunk = author_ids[i:i + chunk_size]
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

def create_unique_authors_dataframe(authors_details):
    """
    Crée un DataFrame final en sélectionnant la 'meilleure' forme-auteur
    (VALID > OLD > INCOMING) pour chaque nom complet (`fullName_s`).
    """
    df = pd.DataFrame(authors_details)
    if df.empty:
        return df

    # Définir l'ordre de priorité pour le statut de validation
    validity_order = {
        "forme auteur principale d'un IdHAL": 1,
        "forme auteur alternative d'un IdHAL": 2,
        "forme auteur sans IdHAL associé": 3
    }
    
    df['validity_rank'] = df['valid_s'].apply(lambda x: validity_order.get(x, 4))
    
    # Trier par nom complet, puis par rang de validité (le plus petit est le meilleur)
    df_sorted = df.sort_values(by=['fullName_s', 'validity_rank'], ascending=[True, True])
    
    # Garder la meilleure forme-auteur unique pour chaque nom complet
    df_unique = df_sorted.drop_duplicates(subset=['fullName_s'], keep='first')
    
    df_unique = df_unique.drop(columns=['validity_rank']).reset_index(drop=True)
    
    return df_unique

def get_authors_data(collection_code, years="", fields_list="docid,fullName_s,valid_s,halId_s,orcidId_s,firstName_s,lastName_s"):
    """Fonction principale pour orchestrer l'extraction des auteurs."""
    
    # 1. Récupérer les publications et les identifiants d'auteurs
    publications = fetch_publications_for_collection(collection_code, years)
    if not publications:
        return pd.DataFrame()
        
    author_ids = extract_author_ids(publications)

    if not author_ids:
        return pd.DataFrame()

    # 2. Récupérer les détails des auteurs (formes-auteurs)
    author_details = fetch_author_details(author_ids, fields_list)

    if not author_details:
        return pd.DataFrame()

    # 3. Créer le DataFrame final unique 
    df_unique = create_unique_authors_dataframe(author_details)
    
    # S'assurer que les colonnes sont dans l'ordre demandé
    requested_fields = fields_list.split(',')
    final_cols = [col for col in requested_fields if col in df_unique.columns]
    
    return df_unique[final_cols]

# --- Fonctions utilitaires pour Streamlit ---
@st.cache_data
def convert_df(df):
    """Convertit un DataFrame en CSV pour le téléchargement."""
    # Utilisation du point-virgule comme séparateur pour la compatibilité Excel en français
    return df.to_csv(index=False, sep=';').encode('utf-8')

def build_fields_list(fields_selected):
    """Construit la chaîne de champs à partir des sélections Streamlit."""
    # Ajoute les champs obligatoires pour la logique de déduplication/affichge
    mandatory_fields = ['docid', 'fullName_s', 'valid_s']
    
    final_fields = list(set(mandatory_fields + fields_selected))
    
    return ','.join(final_fields)

# --- Application Streamlit ---
def main():
    st.set_page_config(page_title="Extracteur Auteurs HAL (Collection)", layout="wide")
    st.title("Extracteur d'Auteurs par Collection HAL")
    st.markdown("Utilise l'API HAL pour extraire la liste des **formes-auteurs uniques** d'une collection donnée.")

    # Options des champs
    all_available_fields = [
        'halId_s', 'orcidId_s', 'firstName_s', 'lastName_s', 
        'email_s', 'structHasAuthId_fs', 'birthDateY_i'
    ]
    
    with st.sidebar:
        st.header("Paramètres de l'Extraction")
        collection_code = st.text_input("Code Collection HAL (ex: CRAO, TEL)", value="CRAO")
        years = st.text_input("Année(s) (ex: 2023, 2020-2023)", value="")
        
        st.subheader("Champs d'Auteur à Récupérer")
        # Sélection des champs.
        fields_selected = st.multiselect(
            "Sélectionnez les champs additionnels", 
            options=all_available_fields,
            default=['halId_s', 'orcidId_s', 'firstName_s', 'lastName_s']
        )
        
        # Bouton d'extraction
        if st.button("Lancer l'Extraction", type="primary"):
            if not collection_code:
                st.sidebar.error("Veuillez entrer un code de collection HAL.")
                return
            
            # Préparer les champs pour l'API
            api_fields = build_fields_list(fields_selected)
            
            with st.spinner(f"Récupération en cours pour **{collection_code}**..."):
                # Exécution de la logique d'extraction
                df_authors = get_authors_data(collection_code, years, api_fields)
            
            if df_authors.empty:
                st.warning("Aucun résultat trouvé pour cette collection ou une erreur est survenue.")
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
        st.info(f"Nombre de formes-auteurs uniques (dédupliquées par nom complet) : **{len(df_authors)}**")
        
        # Afficher le DataFrame
        st.dataframe(df_authors, use_container_width=True)
        
        # Préparer et afficher le bouton de téléchargement CSV
        csv_data = convert_df(df_authors)
        filename = f'auteurs_uniques_{collection_code}_{years if years else "all"}.csv'
        
        st.download_button(
            label="💾 Télécharger la liste des auteurs (CSV)",
            data=csv_data,
            file_name=filename,
            mime='text/csv',
            key='download_button'
        )
        st.markdown("_Le séparateur utilisé est le point-virgule (`;`) pour une meilleure compatibilité avec Excel en français._")


if __name__ == '__main__':
    main()
