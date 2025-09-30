import os # Pour la variable d'environnement NCBI_API_KEY
import streamlit as st
import pandas as pd
import io
# Supprimé: requests, json, unicodedata, difflib, tqdm, concurrent
# Ces imports sont maintenant dans utils.py ou non nécessaires directement ici

# Importer les fonctions et constantes partagées depuis utils.py
from utils import (
    get_scopus_data, get_openalex_data, get_pubmed_data, convert_to_dataframe,
    clean_doi, HalCollImporter, merge_rows_with_sources, get_authors_from_crossref,
    check_df, enrich_w_upw_parallel, add_permissions_parallel, deduce_todo,
    normalise, normalize_name, get_initial_form # normalise est utilisé par HalCollImporter et check_df
)
# Les constantes comme HAL_API_ENDPOINT sont utilisées par les fonctions dans utils.py

# --- Définition de la liste des laboratoires (spécifique à cette application) ---
labos_list_nantes = [
    {
        "collection": "METIS", "scopus_id": "60105490", "openalex_id": "I4387152714",
        "pubmed_query": "(CAPHI[Affiliation]) OR (\"CENTRE ATLANTIQUE DE PHILOSOPHIE\"[Affiliation]) OR (\"EA 7463\" [Affiliation]) OR (EA7463[Affiliation]) OR (UR7463[Affiliation]) OR (\"UR 7463\"[Affiliation])"
    }
]
labos_df_nantes_global = pd.DataFrame(labos_list_nantes)


# Fonction pour ajouter le menu de navigation (spécifique à cette app)
def add_sidebar_menu():
    st.sidebar.header("À Propos")
    st.sidebar.info(
    """
    **c2LabHAL - Version Nantes Université** :
    Cette version est préconfigurée pour les laboratoires de Nantes Université.
    Sélectionnez un laboratoire dans la liste pour lancer la comparaison de ses publications
    (Scopus, OpenAlex, PubMed) avec sa collection HAL.
    """
)
    st.sidebar.markdown("---")

    st.sidebar.header("Autres applications c2LabHAL")
    st.sidebar.markdown("📖 [c2LabHAL - Application Principale](https://c2labhal.streamlit.app/)")
    st.sidebar.markdown("📄 [c2LabHAL version CSV](https://c2labhal-csv.streamlit.app/)")


    st.sidebar.markdown("---")
    
    st.sidebar.markdown("Présentation du projet :")
    st.sidebar.markdown("[📊 Voir les diapositives](https://slides.com/guillaumegodet/deck-d5bc03#/2)")
    st.sidebar.markdown("Code source :")
    st.sidebar.markdown("[🐙 Voir sur GitHub](https://github.com/GuillaumeGodet/c2labhal)")


def main():
    st.set_page_config(page_title="c2LabHAL - Nantes", layout="wide")
    add_sidebar_menu() 

    st.title("🥎 c2LabHAL - Version Nantes Université")
    st.subheader("Comparez les publications d’un laboratoire de Nantes Université avec sa collection HAL.")

    labo_choisi_nom_nantes = st.selectbox(
        "Choisissez une collection HAL de laboratoire (Nantes Université) :", 
        sorted(labos_df_nantes_global['collection'].unique())
    )

    labo_selectionne_details_nantes = labos_df_nantes_global[labos_df_nantes_global['collection'] == labo_choisi_nom_nantes].iloc[0]
    collection_a_chercher_nantes = labo_selectionne_details_nantes['collection']
    scopus_lab_id_nantes = labo_selectionne_details_nantes.get('scopus_id', '') 
    openalex_institution_id_nantes = labo_selectionne_details_nantes.get('openalex_id', '')
    pubmed_query_labo_nantes = labo_selectionne_details_nantes.get('pubmed_query', '')

    scopus_api_key_secret_nantes = st.secrets.get("SCOPUS_API_KEY")
    pubmed_api_key_secret_nantes = st.secrets.get("PUBMED_API_KEY")

    col1_dates_nantes, col2_dates_nantes = st.columns(2)
    with col1_dates_nantes:
        start_year_nantes = st.number_input("Année de début", min_value=1900, max_value=2100, value=2020, key="nantes_start_year")
    with col2_dates_nantes:
        end_year_nantes = st.number_input("Année de fin", min_value=1900, max_value=2100, value=pd.Timestamp.now().year, key="nantes_end_year")

    with st.expander("🔧 Options avancées pour les auteurs"):
        fetch_authors_nantes = st.checkbox("🧑‍🔬 Récupérer les auteurs via Crossref (peut ralentir)", value=False, key="nantes_fetch_authors_cb")
        compare_authors_nantes = False
        uploaded_authors_file_nantes = None
        if fetch_authors_nantes:
            compare_authors_nantes = st.checkbox("🔍 Comparer les auteurs avec une liste de chercheurs", value=False, key="nantes_compare_authors_cb")
            if compare_authors_nantes:
                uploaded_authors_file_nantes = st.file_uploader(
                    "📤 Téléversez un fichier CSV de chercheurs (colonnes: 'collection', 'prénom nom')", 
                    type=["csv"], 
                    key="nantes_upload_authors_fu",
                    help="Le fichier CSV doit avoir une colonne 'collection' (code de la collection HAL) et une colonne avec les noms des chercheurs."
                )
    
    progress_bar_nantes = st.progress(0)
    progress_text_area_nantes = st.empty() # Correction: Suffixe _nantes ajouté

    if st.button(f"🚀 Lancer la recherche pour {collection_a_chercher_nantes}"):
        if pubmed_api_key_secret_nantes and pubmed_query_labo_nantes:
            os.environ['NCBI_API_KEY'] = pubmed_api_key_secret_nantes

        scopus_df_nantes = pd.DataFrame()
        openalex_df_nantes = pd.DataFrame()
        pubmed_df_nantes = pd.DataFrame()

        # --- Étape 1 : Récupération OpenAlex ---
        if openalex_institution_id_nantes:
            with st.spinner(f"Récupération OpenAlex pour {collection_a_chercher_nantes}..."):
                progress_text_area_nantes.info("Étape 1/9 : Récupération des données OpenAlex...") # Corrigé
                progress_bar_nantes.progress(5) # Corrigé
                openalex_query_complet_nantes = f"authorships.institutions.id:{openalex_institution_id_nantes},publication_year:{start_year_nantes}-{end_year_nantes}"
                openalex_data_nantes = get_openalex_data(openalex_query_complet_nantes, max_items=5000)
                if openalex_data_nantes:
                    openalex_df_nantes = convert_to_dataframe(openalex_data_nantes, 'openalex')
                    openalex_df_nantes['Source title'] = openalex_df_nantes.apply(
                        lambda row: row.get('primary_location', {}).get('source', {}).get('display_name') if isinstance(row.get('primary_location'), dict) and row['primary_location'].get('source') else None, axis=1
                    )
                    openalex_df_nantes['Date'] = openalex_df_nantes.get('publication_date', pd.Series(index=openalex_df_nantes.index, dtype='object'))
                    openalex_df_nantes['doi'] = openalex_df_nantes.get('doi', pd.Series(index=openalex_df_nantes.index, dtype='object'))
                    openalex_df_nantes['id'] = openalex_df_nantes.get('id', pd.Series(index=openalex_df_nantes.index, dtype='object'))
                    openalex_df_nantes['Title'] = openalex_df_nantes.get('title', pd.Series(index=openalex_df_nantes.index, dtype='object'))
                    cols_to_keep_nantes = ['Data source', 'Title', 'doi', 'id', 'Source title', 'Date']
                    openalex_df_nantes = openalex_df_nantes[[col for col in cols_to_keep_nantes if col in openalex_df_nantes.columns]]
                    if 'doi' in openalex_df_nantes.columns:
                        openalex_df_nantes['doi'] = openalex_df_nantes['doi'].apply(clean_doi)
                st.success(f"{len(openalex_df_nantes)} publications OpenAlex trouvées pour {collection_a_chercher_nantes}.")
        progress_bar_nantes.progress(10) # Corrigé

        # --- Étape 2 : Récupération PubMed ---
        if pubmed_query_labo_nantes: 
            with st.spinner(f"Récupération PubMed pour {collection_a_chercher_nantes}..."):
                progress_text_area_nantes.info("Étape 2/9 : Récupération des données PubMed...") # Corrigé
                progress_bar_nantes.progress(20) # Corrigé (ajusté pour être après l'info)
                pubmed_full_query_nantes = f"({pubmed_query_labo_nantes}) AND ({start_year_nantes}/01/01[Date - Publication] : {end_year_nantes}/12/31[Date - Publication])"
                pubmed_data_nantes = get_pubmed_data(pubmed_full_query_nantes, max_items=5000)
                if pubmed_data_nantes:
                    pubmed_df_nantes = pd.DataFrame(pubmed_data_nantes)
                st.success(f"{len(pubmed_df_nantes)} publications PubMed trouvées pour {collection_a_chercher_nantes}.")
        else:
            st.info(f"Aucune requête PubMed configurée pour {collection_a_chercher_nantes}.")
        progress_bar_nantes.progress(20) # Corrigé (ou 25 si on veut marquer la fin de l'étape)

        # --- Étape 3 : Récupération Scopus ---
        if scopus_lab_id_nantes and scopus_api_key_secret_nantes:
            with st.spinner(f"Récupération Scopus pour {collection_a_chercher_nantes}..."):
                progress_text_area_nantes.info("Étape 3/9 : Récupération des données Scopus...") # Corrigé
                progress_bar_nantes.progress(25) # Corrigé (ajusté)
                scopus_query_complet_nantes = f"AF-ID({scopus_lab_id_nantes}) AND PUBYEAR > {start_year_nantes - 1} AND PUBYEAR < {end_year_nantes + 1}"
                scopus_data_nantes = get_scopus_data(scopus_api_key_secret_nantes, scopus_query_complet_nantes, max_items=5000)
                if scopus_data_nantes:
                    scopus_df_raw_nantes = convert_to_dataframe(scopus_data_nantes, 'scopus')
                    required_scopus_cols_nantes = {'dc:title', 'prism:doi', 'dc:identifier', 'prism:publicationName', 'prism:coverDate'}
                    if required_scopus_cols_nantes.issubset(scopus_df_raw_nantes.columns):
                        scopus_df_nantes = scopus_df_raw_nantes[['Data source', 'dc:title', 'prism:doi', 'dc:identifier', 'prism:publicationName', 'prism:coverDate']].copy()
                        scopus_df_nantes.columns = ['Data source', 'Title', 'doi', 'id', 'Source title', 'Date']
                        if 'doi' in scopus_df_nantes.columns:
                            scopus_df_nantes['doi'] = scopus_df_nantes['doi'].apply(clean_doi)
                    else:
                        st.warning(f"Données Scopus incomplètes pour {collection_a_chercher_nantes}. Scopus sera ignoré.")
                        scopus_df_nantes = pd.DataFrame()
                st.success(f"{len(scopus_df_nantes)} publications Scopus trouvées pour {collection_a_chercher_nantes}.")
        elif scopus_lab_id_nantes and not scopus_api_key_secret_nantes:
            st.warning(f"L'ID Scopus est fourni pour {collection_a_chercher_nantes} mais la clé API Scopus n'est pas configurée. Scopus sera ignoré.")
        progress_bar_nantes.progress(30) # Corrigé
        
        # --- Étape 4 : Combinaison des données ---
        progress_text_area_nantes.info("Étape 4/9 : Combinaison des données sources...") # Corrigé
        combined_df_nantes = pd.concat([scopus_df_nantes, openalex_df_nantes, pubmed_df_nantes], ignore_index=True)

        if combined_df_nantes.empty:
            st.error(f"Aucune publication récupérée pour {collection_a_chercher_nantes}. Vérifiez la configuration du laboratoire.")
            st.stop()
        
        if 'doi' not in combined_df_nantes.columns:
            combined_df_nantes['doi'] = pd.NA
        combined_df_nantes['doi'] = combined_df_nantes['doi'].astype(str).str.lower().str.strip().replace(['nan', 'none', 'NaN', ''], pd.NA, regex=False)


        # --- Étape 5 : Fusion des lignes en double ---
        progress_text_area_nantes.info("Étape 5/9 : Fusion des doublons...") # Corrigé
        progress_bar_nantes.progress(40) # Corrigé
        
        with_doi_df_nantes = combined_df_nantes[combined_df_nantes['doi'].notna()].copy()
        without_doi_df_nantes = combined_df_nantes[combined_df_nantes['doi'].isna()].copy()
        
        
        merged_data_doi_nantes = pd.DataFrame()
        if not with_doi_df_nantes.empty:
            merged_data_doi_nantes = with_doi_df_nantes.groupby('doi', as_index=False).apply(merge_rows_with_sources)
            if 'doi' not in merged_data_doi_nantes.columns and merged_data_doi_nantes.index.name == 'doi':
                merged_data_doi_nantes.reset_index(inplace=True)
            if isinstance(merged_data_doi_nantes.columns, pd.MultiIndex):
                 merged_data_doi_nantes.columns = merged_data_doi_nantes.columns.droplevel(0)
        
       
        merged_data_no_doi_nantes = pd.DataFrame()
        if not without_doi_df_nantes.empty:
            merged_data_no_doi_nantes = without_doi_df_nantes.copy() 
        
       
        final_merged_data_nantes = pd.concat([merged_data_doi_nantes, merged_data_no_doi_nantes], ignore_index=True)

        if final_merged_data_nantes.empty:
            st.error(f"Aucune donnée après fusion pour {collection_a_chercher_nantes}.")
            st.stop()
        st.success(f"{len(final_merged_data_nantes)} publications uniques après fusion pour {collection_a_chercher_nantes}.")
        progress_bar_nantes.progress(50) # Corrigé

        # --- Étape 6 : Comparaison HAL ---
        coll_df_hal_nantes = pd.DataFrame()
        with st.spinner(f"Importation de la collection HAL '{collection_a_chercher_nantes}'..."):
            progress_text_area_nantes.info(f"Étape 6a/9 : Importation de la collection HAL '{collection_a_chercher_nantes}'...") # Corrigé
            coll_importer_nantes_obj = HalCollImporter(collection_a_chercher_nantes, start_year_nantes, end_year_nantes)
            coll_df_hal_nantes = coll_importer_nantes_obj.import_data()
            if coll_df_hal_nantes.empty:
                st.warning(f"Collection HAL '{collection_a_chercher_nantes}' vide ou non chargée.")
            else:
                st.success(f"{len(coll_df_hal_nantes)} notices HAL pour {collection_a_chercher_nantes}.")
        
        progress_text_area_nantes.info("Étape 6b/9 : Comparaison avec les données HAL...") # Corrigé
        result_df_nantes = check_df(final_merged_data_nantes.copy(), coll_df_hal_nantes, progress_bar_st=progress_bar_nantes, progress_text_st=progress_text_area_nantes) # Passé les bons objets
        st.success(f"Comparaison HAL pour {collection_a_chercher_nantes} terminée.")
        # progress_bar_nantes est géré par check_df

        # --- Étape 7 : Enrichissement Unpaywall ---
        with st.spinner(f"Enrichissement Unpaywall pour {collection_a_chercher_nantes}..."):
            progress_text_area_nantes.info("Étape 7/9 : Enrichissement Unpaywall...") # Corrigé
            progress_bar_nantes.progress(70) # Corrigé (ajouté avant l'appel)
            result_df_nantes = enrich_w_upw_parallel(result_df_nantes.copy())
            st.success(f"Enrichissement Unpaywall pour {collection_a_chercher_nantes} terminé.")
        # progress_bar_nantes.progress(70) # Déplacé avant l'appel

        # --- Étape 8 : Permissions de dépôt ---
        with st.spinner(f"Récupération des permissions pour {collection_a_chercher_nantes}..."):
            progress_text_area_nantes.info("Étape 8/9 : Récupération des permissions de dépôt...") # Corrigé
            progress_bar_nantes.progress(80) # Corrigé (ajouté avant l'appel)
            result_df_nantes = add_permissions_parallel(result_df_nantes.copy())
            st.success(f"Permissions pour {collection_a_chercher_nantes} récupérées.")
        # progress_bar_nantes.progress(80) # Déplacé avant l'appel

        # --- Étape 9 : Déduction des actions et auteurs ---
        progress_text_area_nantes.info("Étape 9/9 : Déduction des actions et traitement des auteurs...") # Corrigé
        if 'Action' not in result_df_nantes.columns: result_df_nantes['Action'] = pd.NA
        result_df_nantes['Action'] = result_df_nantes.apply(deduce_todo, axis=1)

        if fetch_authors_nantes: 
            with st.spinner(f"Récupération des auteurs Crossref pour {collection_a_chercher_nantes}..."):
                if 'doi' in result_df_nantes.columns:
                    from concurrent.futures import ThreadPoolExecutor 
                    from tqdm import tqdm 

                    dois_for_authors_nantes = result_df_nantes['doi'].fillna("").tolist()
                    authors_results_nantes = []
                    with ThreadPoolExecutor(max_workers=10) as executor:
                        authors_results_nantes = list(tqdm(executor.map(get_authors_from_crossref, dois_for_authors_nantes), total=len(dois_for_authors_nantes), desc="Auteurs Crossref (Nantes)"))
                    
                    result_df_nantes['Auteurs_Crossref'] = ['; '.join(author_l) if isinstance(author_l, list) and not any("Erreur" in str(a) or "Timeout" in str(a) for a in author_l) else (author_l[0] if isinstance(author_l, list) and author_l else '') for author_l in authors_results_nantes]
                    st.success(f"Auteurs Crossref pour {collection_a_chercher_nantes} récupérés.")
                else:
                    st.warning("Colonne 'doi' non trouvée, impossible de récupérer les auteurs pour la version Nantes.")
                    result_df_nantes['Auteurs_Crossref'] = ''
            
            if compare_authors_nantes and uploaded_authors_file_nantes:
                with st.spinner(f"Comparaison des auteurs (fichier) pour {collection_a_chercher_nantes}..."):
                    try:
                        user_authors_df_nantes_file = pd.read_csv(uploaded_authors_file_nantes)
                        if not ({'collection', user_authors_df_nantes_file.columns[1]} <= set(user_authors_df_nantes_file.columns)):
                            st.error("Fichier CSV auteurs mal formaté pour la version Nantes.")
                        else:
                            author_name_col_nantes_file = user_authors_df_nantes_file.columns[1]
                            noms_ref_nantes_list = user_authors_df_nantes_file[user_authors_df_nantes_file["collection"].astype(str).str.lower() == str(collection_a_chercher_nantes).lower()][author_name_col_nantes_file].dropna().unique().tolist()
                            if not noms_ref_nantes_list:
                                st.warning(f"Aucun chercheur pour '{collection_a_chercher_nantes}' dans le fichier fourni (Nantes).")
                            else:
                                chercheur_map_nantes_file = {normalize_name(n): n for n in noms_ref_nantes_list}
                                initial_map_nantes_file = {get_initial_form(normalize_name(n)): n for n in noms_ref_nantes_list}
                                from difflib import get_close_matches 

                                def detect_known_authors_nantes_file(authors_str_nantes):
                                    if pd.isna(authors_str_nantes) or not str(authors_str_nantes).strip() or "Erreur" in authors_str_nantes or "Timeout" in authors_str_nantes: return ""
                                    authors_pub_nantes = [a.strip() for a in str(authors_str_nantes).split(';') if a.strip()]
                                    detectes_originaux_nantes = set()
                                    for author_o_nantes in authors_pub_nantes:
                                        author_n_nantes = normalize_name(author_o_nantes)
                                        author_i_n_nantes = get_initial_form(author_n_nantes)
                                        match_c_nantes = get_close_matches(author_n_nantes, chercheur_map_nantes_file.keys(), n=1, cutoff=0.85)
                                        if match_c_nantes:
                                            detectes_originaux_nantes.add(chercheur_map_nantes_file[match_c_nantes[0]])
                                            continue
                                        match_i_nantes = get_close_matches(author_i_n_nantes, initial_map_nantes_file.keys(), n=1, cutoff=0.9)
                                        if match_i_nantes:
                                            detectes_originaux_nantes.add(initial_map_nantes_file[match_i_nantes[0]])
                                    return "; ".join(sorted(list(detectes_originaux_nantes))) if detectes_originaux_nantes else ""
                                result_df_nantes['Auteurs_Laboratoire_Détectés'] = result_df_nantes['Auteurs_Crossref'].apply(detect_known_authors_nantes_file)
                                st.success(f"Comparaison auteurs (fichier) pour {collection_a_chercher_nantes} terminée.")
                    except Exception as e_auth_file_nantes_exc:
                        st.error(f"Erreur fichier auteurs (Nantes): {e_auth_file_nantes_exc}")
            elif compare_authors_nantes and not uploaded_authors_file_nantes:
                 st.warning("Veuillez téléverser un fichier CSV de chercheurs pour la comparaison des auteurs (Nantes).")

        progress_bar_nantes.progress(90) # Corrigé
        st.success(f"Déduction des actions et traitement des auteurs pour {collection_a_chercher_nantes} terminés.")
        
        st.dataframe(result_df_nantes)

        if not result_df_nantes.empty:
            csv_export_nantes_data = result_df_nantes.to_csv(index=False, encoding='utf-8-sig')
            output_filename_nantes_final = f"c2LabHAL_resultats_{collection_a_chercher_nantes.replace(' ', '_')}_{start_year_nantes}-{end_year_nantes}.csv"
            st.download_button(
                label=f"📥 Télécharger les résultats pour {collection_a_chercher_nantes}",
                data=csv_export_nantes_data,
                file_name=output_filename_nantes_final,
                mime="text/csv",
                key=f"download_nantes_{collection_a_chercher_nantes}"
            )
        progress_bar_nantes.progress(100) # Corrigé
        progress_text_area_nantes.success(f"🎉 Traitement pour {collection_a_chercher_nantes} terminé avec succès !") # Corrigé

if __name__ == "__main__":
    main()