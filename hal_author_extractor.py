import requests
import pandas as pd
import time
from urllib.parse import urlencode

# Le point d'entrée de l'API de recherche HAL pour les documents
HAL_SEARCH_API = "http://api.archives-ouvertes.fr/search/"
# Le point d'entrée de l'API de référence HAL pour les auteurs (pour les détails)
HAL_AUTHOR_API = "http://api.archives-ouvertes.fr/ref/author/"

def fetch_publications_for_collection(collection_code, years="", fields="structHasAuthId_fs"):
    """
    Récupère tous les documents (publications) pour un code de collection donné
    en utilisant l'API de recherche HAL.
    """
    found_publications = 0
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

    print(f"Démarrage de la récupération pour la collection '{collection_code}' (Année(s): {years if years else 'Toutes'})")

    while True:
        query_params['start'] = start
        
        # L'URL de l'API de recherche HAL (pour la collection, le code est dans le chemin)
        url = f"{HAL_SEARCH_API}{collection_code}/?{urlencode(query_params)}"
        
        try:
            response = requests.get(url)
            response.raise_for_status() # Lève une exception pour les codes d'erreur HTTP (4xx ou 5xx)
            data = response.json()
            
            docs = data.get('response', {}).get('docs', [])
            num_found = data.get('response', {}).get('numFound', 0)
            all_docs.extend(docs)
            
            found_publications += len(docs)
            
            print(f"Publications récupérées: {found_publications} / {num_found}")

            if found_publications >= num_found or not docs:
                break
            
            start += rows
            # Petite pause pour ne pas surcharger l'API
            time.sleep(0.5) 

        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la requête API (Recherche): {e}")
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
            # Le format est typiquement "STRUCTID_HALID_JoinSep_AUTHORID_FacetSep"
            # On cherche l'AUTHORID (l'ID de la forme auteur)
            # On utilise une regex simplifiée pour capturer l'ID juste après le dernier '_' avant 'FacetSep'
            parts = author_str.split('_JoinSep_')
            if len(parts) > 1:
                author_id_part = parts[1].split('_FacetSep')[0]
                author_ids.add(author_id_part)
                
    return list(author_ids)

def fetch_author_details(author_ids, fields="docid,fullName_s,valid_s,halId_s,orcidId_s,firstName_s,lastName_s"):
    """
    Récupère les détails de chaque auteur (forme-auteur) à partir de leur docid
    en utilisant l'API de référence HAL.
    Similaire à la fonction `sendQuery` du JS original.
    """
    authors_details = []
    
    # La requête de l'API `ref/author` prend une liste d'IDs (docid) séparés par OR
    # On va regrouper les requêtes pour respecter la limite de l'URL et optimiser
    chunk_size = 50 
    total_authors = len(author_ids)
    
    print(f"\nRécupération des détails pour {total_authors} auteurs...")
    
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
            
            # Traitement des champs comme dans le script JS original
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

            print(f"Progression: {min(i + chunk_size, total_authors)} / {total_authors}")
            time.sleep(0.3) # Pause entre les requêtes groupées

        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la requête API (Détails Auteur): {e}")
            break
            
    return authors_details

def create_unique_authors_dataframe(authors_details):
    """
    Crée un DataFrame final en sélectionnant la 'meilleure' forme-auteur
    (VALID > OLD > INCOMING) pour chaque nom complet (`fullName_s`),
    similaire à la logique `resultByName` du script JS.
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
    
    # Appliquer l'ordre de priorité aux données
    df['validity_rank'] = df['valid_s'].apply(lambda x: validity_order.get(x, 4))
    
    # Trier d'abord par nom complet, puis par rang de validité (le plus petit est le meilleur)
    df_sorted = df.sort_values(by=['fullName_s', 'validity_rank'], ascending=[True, True])
    
    # Garder la première occurrence de chaque 'fullName_s', qui sera la "meilleure"
    df_unique = df_sorted.drop_duplicates(subset=['fullName_s'], keep='first')
    
    # Nettoyer les colonnes temporaires
    df_unique = df_unique.drop(columns=['validity_rank']).reset_index(drop=True)
    
    return df_unique

def get_authors_data(collection_code, years="", fields_list="docid,fullName_s,valid_s,halId_s,orcidId_s,firstName_s,lastName_s"):
    """Fonction principale pour orchestrer l'extraction des auteurs."""
    
    # 1. Récupérer les publications et les identifiants d'auteurs
    publications = fetch_publications_for_collection(collection_code, years)
    author_ids = extract_author_ids(publications)

    if not author_ids:
        return pd.DataFrame()

    # 2. Récupérer les détails des auteurs (formes-auteurs)
    author_details = fetch_author_details(author_ids, fields_list)

    if not author_details:
        return pd.DataFrame()

    # 3. Créer le DataFrame final unique (similaire à resultByName)
    df_unique = create_unique_authors_dataframe(author_details)
    
    # S'assurer que les colonnes sont dans l'ordre demandé
    # Note: `docid` dans la liste des champs ici est l'ID de la forme-auteur
    requested_fields = fields_list.split(',')
    
    # S'assurer que toutes les colonnes demandées existent dans le DataFrame
    final_cols = [col for col in requested_fields if col in df_unique.columns]
    
    return df_unique[final_cols]

if __name__ == '__main__':
    # Exemple d'utilisation du script seul (sans Streamlit)
    CODE_COLLECTION = "CRAO" # Exemple : Collection du Centre de Recherche sur l'Asie Orientale
    ANNEES = "2023" # Année ou intervalle d'années, ex: "2022 OR 2023" ou "2020-2023"

    # Liste des champs à récupérer, `docid` (forme-auteur ID) est souvent utile en premier.
    CHAMPS_AUTEURS = "docid,fullName_s,valid_s,halId_s,orcidId_s,firstName_s,lastName_s"
    
    df_authors = get_authors_data(CODE_COLLECTION, ANNEES, CHAMPS_AUTEURS)
    
    if not df_authors.empty:
        filename = f'auteurs_{CODE_COLLECTION}_{ANNEES if ANNEES else "all"}.csv'
        df_authors.to_csv(filename, index=False, sep=';', encoding='utf-8')
        print(f"\nExtraction terminée. Données enregistrées dans {filename}")
    else:
        print("\nAucun auteur trouvé ou une erreur est survenue.")
