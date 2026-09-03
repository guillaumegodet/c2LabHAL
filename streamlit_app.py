import streamlit as st
import pandas as pd
import io
import altair as alt
import plotly.graph_objects as go
# Supprimé: requests, json, metapub, regex, unidecode, unicodedata, difflib, langdetect, tqdm, concurrent
# Ces imports sont maintenant dans utils.py

# Importer les fonctions et constantes partagées depuis utils.py
from utils import (
    get_scopus_data, get_openalex_data, get_pubmed_data, convert_to_dataframe,
    clean_doi, HalCollImporter, merge_rows_with_sources, get_authors_from_crossref,
    check_df, enrich_w_upw_parallel, add_permissions_parallel, deduce_todo,
    normalise, normalize_name, get_initial_form # normalise est utilisé par HalCollImporter et check_df via statut_titre
)
# Les constantes comme HAL_API_ENDPOINT, etc., sont utilisées par les fonctions dans utils.py

# Fonction pour ajouter le menu de navigation dans la barre latérale (spécifique à cette app)
def add_sidebar_menu():
    
    st.sidebar.header("À Propos")
    st.sidebar.info(
        """
        **c2LabHAL** est un outil conçu pour aider les laboratoires de recherche à :
        - Comparer leurs listes de publications issues de diverses bases de données (Scopus, OpenAlex, PubMed) avec leur collection HAL.
        - Identifier les publications manquantes ou nécessitant une mise à jour dans HAL.
        - Obtenir des informations sur le statut Open Access (via Unpaywall) et les permissions de dépôt.
        """
    )
    st.sidebar.markdown("---")

    st.sidebar.header("Autres applications c2LabHAL")

    st.sidebar.markdown("📄 [c2LabHAL version CSV](https://c2labhal-csv.streamlit.app/)")
    st.sidebar.markdown("🏛️ [c2LabHAL version Nantes Université](https://c2labhal-nantes.streamlit.app/)")
    st.sidebar.markdown("🔗 [Alignez une liste de chercheurs avec IdRef et HAL](https://c2labhal-auteurs.streamlit.app/)")

    st.sidebar.markdown("---")
    
    st.sidebar.markdown("Présentation du projet :")
    st.sidebar.markdown("[📊 Voir les diapositives](https://slides.com/guillaumegodet/deck-d5bc03#/2)")
    st.sidebar.markdown("Code source :")
    st.sidebar.markdown("[🐙 Voir sur GitHub](https://github.com/GuillaumeGodet/c2labhal)")


# Fonction principale
def main():
    st.set_page_config(page_title="c2LabHAL", layout="wide")
    add_sidebar_menu() 

    st.title("🥎 c2LabHAL")
    st.subheader("Comparez les publications d'un labo dans Scopus, OpenAlex et Pubmed avec sa collection HAL")

    collection_a_chercher = st.text_input(
        "Collection HAL",
        value="", 
        key="collection_hal",
        help="Saisissez le code de la collection HAL du laboratoire (ex: MIP). Laissez vide pour comparer avec tout HAL (non recommandé, très long)."
    )

    openalex_institution_id = st.text_input("Identifiant OpenAlex du labo", help="Saisissez l'identifiant du labo dans OpenAlex (ex: i4392021216 pour MIP).")

    pubmed_query_input = st.text_input("Requête PubMed", help="Saisissez la requête Pubmed qui rassemble le mieux les publications du labo, par exemple ((MIP[Affiliation]) AND ((mans[Affiliation]) OR (nantes[Affiliation]))) OR (EA 4334[Affiliation]) OR (EA4334[Affiliation]) OR (UR 4334[Affiliation]) OR (UR4334[Affiliation]) OR (Movement Interactions Performance[Affiliation] OR (Motricité Interactions Performance[Affiliation]) OR (mouvement interactions performance[Affiliation])")
    
    scopus_lab_id = st.text_input("Identifiant Scopus du labo (AF-ID)", help="Saisissez le Scopus Affiliation Identifier (AF-ID) du laboratoire, par exemple 60105638.")
    
    
    col1_dates, col2_dates = st.columns(2)
    with col1_dates:
        start_year = st.number_input("Année de début", min_value=1900, max_value=2100, value=2020)
    with col2_dates:
        end_year = st.number_input("Année de fin", min_value=1900, max_value=2100, value=pd.Timestamp.now().year) 

    with st.expander("🔧 Options avancées"):
        fetch_authors = st.checkbox("🧑‍🔬 Récupérer les auteurs via Crossref", value=False)
        compare_authors = False
        uploaded_authors_file = None
        if fetch_authors:
            compare_authors = st.checkbox("🔍 Comparer les auteurs Crossref avec ma liste de chercheurs", value=False)
            if compare_authors:
                uploaded_authors_file = st.file_uploader(
                    "📤 Téléversez un fichier CSV avec la liste des chercheurs du labo (colonnes: 'collection', 'prénom nom')", 
                    type=["csv"],
                    help="Le fichier CSV doit avoir une colonne 'collection' (code de la collection HAL) et une colonne 'prénom nom' avec les noms des chercheurs."
                )

    progress_bar = st.progress(0)
    progress_text_area = st.empty() 

    if st.button("🚀 Lancer la recherche et la comparaison"):
        scopus_api_key_secret = st.secrets.get("SCOPUS_API_KEY")
        openalex_api_key_secret = st.secrets.get("OPENALEX_API_KEY")

        if not openalex_institution_id and not pubmed_query_input and not scopus_lab_id:
            st.error("Veuillez configurer au moins une source de données (OpenAlex, PubMed ou Scopus).")
            st.stop()

        scopus_df = pd.DataFrame()
        openalex_df = pd.DataFrame()
        pubmed_df = pd.DataFrame()
        
        # --- Étape 1 : Récupération des données OpenAlex ---
        if openalex_institution_id:
            with st.spinner("Récupération OpenAlex..."):
                progress_text_area.info("Étape 1/9 : Récupération des données OpenAlex...")
                progress_bar.progress(5)
                openalex_query = f"authorships.institutions.id:{openalex_institution_id},publication_year:{start_year}-{end_year}"
                openalex_data = get_openalex_data(openalex_query, max_items=5000, api_key=openalex_api_key_secret)
                if openalex_data:
                    openalex_df = convert_to_dataframe(openalex_data, 'openalex')
                    openalex_df['Source title'] = openalex_df.apply(
                        lambda row: row.get('primary_location', {}).get('source', {}).get('display_name') if isinstance(row.get('primary_location'), dict) and row['primary_location'].get('source') else None, axis=1
                    )
                    # Utiliser .get() pour éviter KeyError si la colonne manque après conversion
                    openalex_df['Date'] = openalex_df.get('publication_date', pd.Series(index=openalex_df.index, dtype='object'))
                    openalex_df['doi'] = openalex_df.get('doi', pd.Series(index=openalex_df.index, dtype='object'))
                    openalex_df['id'] = openalex_df.get('id', pd.Series(index=openalex_df.index, dtype='object')) 
                    openalex_df['Title'] = openalex_df.get('title', pd.Series(index=openalex_df.index, dtype='object'))
                    
                    cols_to_keep = ['Data source', 'Title', 'doi', 'id', 'Source title', 'Date'] # 'Data source' est déjà là
                    # S'assurer que toutes les colonnes à garder existent avant de les sélectionner
                    openalex_df = openalex_df[[col for col in cols_to_keep if col in openalex_df.columns]]
                    if 'doi' in openalex_df.columns:
                        openalex_df['doi'] = openalex_df['doi'].apply(clean_doi)
                st.success(f"{len(openalex_df)} publications trouvées sur OpenAlex.")
        progress_bar.progress(10)

        # --- Étape 2 : Récupération des données PubMed ---
        if pubmed_query_input:
            with st.spinner("Récupération PubMed..."):
                progress_text_area.info("Étape 2/9 : Récupération des données PubMed...")
                pubmed_full_query = f"({pubmed_query_input}) AND ({start_year}/01/01[Date - Publication] : {end_year}/12/31[Date - Publication])"
                pubmed_data = get_pubmed_data(pubmed_full_query, max_items=5000) 
                if pubmed_data:
                    pubmed_df = pd.DataFrame(pubmed_data) 
                st.success(f"{len(pubmed_df)} publications trouvées sur PubMed.")
        progress_bar.progress(20)

        # --- Étape 3 : Récupération des données Scopus ---
        if scopus_lab_id and scopus_api_key_secret:
            with st.spinner("Récupération Scopus..."):
                progress_text_area.info("Étape 3/9 : Récupération des données Scopus...")
                scopus_query = f"AF-ID({scopus_lab_id}) AND PUBYEAR > {start_year - 1} AND PUBYEAR < {end_year + 1}"
                scopus_data = get_scopus_data(scopus_api_key_secret, scopus_query, max_items=5000)
                if scopus_data:
                    scopus_df_raw = convert_to_dataframe(scopus_data, 'scopus')
                    required_scopus_cols = {'dc:title', 'prism:doi', 'dc:identifier', 'prism:publicationName', 'prism:coverDate'}
                    if required_scopus_cols.issubset(scopus_df_raw.columns):
                        scopus_df = scopus_df_raw[['Data source', 'dc:title', 'prism:doi', 'dc:identifier', 'prism:publicationName', 'prism:coverDate']].copy()
                        scopus_df.columns = ['Data source', 'Title', 'doi', 'id', 'Source title', 'Date']
                        if 'doi' in scopus_df.columns:
                             scopus_df['doi'] = scopus_df['doi'].apply(clean_doi)
                    else:
                        st.warning("Certaines colonnes attendues sont manquantes dans les données Scopus. Scopus ne sera pas inclus.")
                        scopus_df = pd.DataFrame()
                st.success(f"{len(scopus_df)} publications trouvées sur Scopus.")
        elif scopus_lab_id and not scopus_api_key_secret:
            st.warning("L'ID Scopus est fourni mais la clé API Scopus (SCOPUS_API_KEY) n'est pas configurée dans les secrets. Scopus sera ignoré.")
        progress_bar.progress(30)

        # --- Étape 4 : Combinaison des données ---
        progress_text_area.info("Étape 4/9 : Combinaison des données sources...")
        combined_df = pd.concat([scopus_df, openalex_df, pubmed_df], ignore_index=True)

        if combined_df.empty:
            st.error("Aucune publication n'a été récupérée depuis OpenAlex, PubMed ou Scopus. Vérifiez vos paramètres.")
            st.stop()
        
        if 'doi' in combined_df.columns:
            combined_df['doi'] = combined_df['doi'].astype(str).apply(clean_doi).str.lower().str.strip()
            combined_df['doi'] = combined_df['doi'].replace(['nan', ''], pd.NA)


      # --- Étape 5 : Fusion des lignes en double ---
        progress_text_area.info("Étape 5/9 : Fusion des doublons...")
        progress_bar.progress(40)
        
        # S'assurer que la colonne 'doi' existe
        if 'doi' not in combined_df.columns:
            combined_df['doi'] = pd.NA # Assigner pd.NA pour créer la colonne avec le bon type pour les NaN

        # 1. Appliquer clean_doi une seule fois (déjà fait lors de la création des df sources)
        #    Si ce n'est pas le cas, ou pour s'en assurer, on peut le remettre ici, mais
        #    il est préférable de le faire en amont.
        #    Pour l'instant, on suppose que clean_doi a été appliqué.

        # 2. Normaliser la colonne DOI pour la détection des valeurs manquantes
        #    Convertir en string, mettre en minuscule, enlever les espaces superflus.
        s_doi = combined_df['doi'].astype(str).str.lower().str.strip()

        # 3. Remplacer toutes les représentations textuelles courantes de "valeur manquante" 
        #    par pd.NA (la vraie valeur "Not Available" de Pandas).
        #    '<na>' peut être produit par astype(str) sur des pd.NA existants.
        #    'none' (minuscule) pour str(None).lower().
        #    'nan' (minuscule) pour str(np.nan).lower().
        valeurs_a_remplacer_par_na = ['none', 'nan', '', '<na>', 'na'] # Ajout de 'na'
        combined_df['doi'] = s_doi.replace(valeurs_a_remplacer_par_na, pd.NA)

        # --- Maintenant, séparer les lignes ---
        with_doi_df = combined_df[combined_df['doi'].notna()].copy()
        without_doi_df = combined_df[combined_df['doi'].isna()].copy()

        
        merged_data_doi = pd.DataFrame()
        if not with_doi_df.empty:
            merged_data_doi = with_doi_df.groupby('doi', as_index=False).apply(merge_rows_with_sources)
            if 'doi' not in merged_data_doi.columns and merged_data_doi.index.name == 'doi':
                merged_data_doi.reset_index(inplace=True)
            if isinstance(merged_data_doi.columns, pd.MultiIndex):
                 merged_data_doi.columns = merged_data_doi.columns.droplevel(0)
        
        
        merged_data_no_doi = pd.DataFrame()
        if not without_doi_df.empty:
            merged_data_no_doi = without_doi_df.copy() 
        
      
        merged_data = pd.concat([merged_data_doi, merged_data_no_doi], ignore_index=True)

        st.success(f"{len(merged_data)} publications uniques après fusion.")
        progress_bar.progress(50)

        # --- Étape 6 : Comparaison avec HAL ---
        coll_df = pd.DataFrame() 
        if collection_a_chercher: 
            with st.spinner(f"Import de la collection HAL '{collection_a_chercher}'..."):
                progress_text_area.info(f"Étape 6a/9 : Import de la collection HAL '{collection_a_chercher}'...")
                coll_importer = HalCollImporter(collection_a_chercher, start_year, end_year)
                coll_df = coll_importer.import_data() 
                if coll_df.empty:
                    st.warning(f"La collection HAL '{collection_a_chercher}' est vide ou n'a pas pu être chargée pour les années {start_year}-{end_year}.")
                else:
                    st.success(f"{len(coll_df)} notices trouvées dans la collection HAL '{collection_a_chercher}'.")
        else: 
            st.info("Aucun code de collection HAL fourni. La comparaison se fera avec l'ensemble de HAL (peut être long et moins précis).")
        
        progress_text_area.info("Étape 6b/9 : Comparaison avec les données HAL...")
        final_df = check_df(merged_data.copy(), coll_df, progress_bar_st=progress_bar, progress_text_st=progress_text_area) 
        st.success("Comparaison avec HAL terminée.")
        # progress_bar est géré par check_df, donc pas besoin de le mettre à jour ici explicitement à 60%

        # --- Étape 7 : Enrichissement Unpaywall ---
        with st.spinner("Enrichissement Unpaywall..."):
            progress_text_area.info("Étape 7/9 : Enrichissement avec Unpaywall...")
            final_df = enrich_w_upw_parallel(final_df.copy()) 
            st.success("Enrichissement Unpaywall terminé.")
        progress_bar.progress(70)

        # --- Étape 8 : Ajout des permissions de dépôt (OA.Works) ---
        with st.spinner("Récupération des permissions de dépôt (OA.Works)..."):
            progress_text_area.info("Étape 8/9 : Récupération des permissions de dépôt...")
            final_df = add_permissions_parallel(final_df.copy()) 
            st.success("Récupération des permissions terminée.")
        progress_bar.progress(80)

        # --- Étape 9 : Déduction des actions et récupération des auteurs (si cochée) ---
        progress_text_area.info("Étape 9/9 : Déduction des actions et traitement des auteurs...")
        if 'Action' not in final_df.columns: 
            final_df['Action'] = pd.NA
        final_df['Action'] = final_df.apply(deduce_todo, axis=1)
        
        if fetch_authors:
            with st.spinner("Récupération des auteurs via Crossref..."):
                if 'doi' in final_df.columns:
                    # Utiliser ThreadPoolExecutor directement ici pour get_authors_from_crossref
                    from concurrent.futures import ThreadPoolExecutor
                    from tqdm import tqdm

                    dois_for_authors = final_df['doi'].fillna("").tolist()
                    authors_results = []
                    with ThreadPoolExecutor(max_workers=10) as executor:
                        authors_results = list(tqdm(executor.map(get_authors_from_crossref, dois_for_authors), total=len(dois_for_authors), desc="Récupération auteurs Crossref"))
                    
                    final_df['Auteurs_Crossref'] = ['; '.join(author_list) if isinstance(author_list, list) and not any("Erreur" in str(a) or "Timeout" in str(a) for a in author_list) else (author_list[0] if isinstance(author_list, list) and author_list else '') for author_list in authors_results]
                    st.success("Récupération des auteurs terminée.")
                else:
                    st.warning("Colonne 'doi' non trouvée, impossible de récupérer les auteurs.")
                    final_df['Auteurs_Crossref'] = ''

            if compare_authors and uploaded_authors_file and collection_a_chercher: 
                with st.spinner("Comparaison des auteurs avec le fichier fourni..."):
                    try:
                        user_authors_df = pd.read_csv(uploaded_authors_file)
                        if not ({'collection', user_authors_df.columns[1]} <= set(user_authors_df.columns)): 
                            st.error("Le fichier CSV des chercheurs doit contenir une colonne 'collection' et une deuxième colonne avec 'prénom nom'.")
                        else:
                            author_name_col = user_authors_df.columns[1] 
                            noms_ref_list = user_authors_df[user_authors_df["collection"].astype(str).str.lower() == str(collection_a_chercher).lower()][author_name_col].dropna().unique().tolist()
                            
                            if not noms_ref_list:
                                st.warning(f"Aucun chercheur trouvé pour la collection '{collection_a_chercher}' dans le fichier fourni.")
                            else:
                                chercheur_map_norm = {normalize_name(n): n for n in noms_ref_list} 
                                initial_map_norm = {get_initial_form(normalize_name(n)): n for n in noms_ref_list} 
                                
                                from difflib import get_close_matches # S'assurer de l'import si pas global à utils

                                def detect_known_authors_optimized(authors_crossref_str):
                                    if pd.isna(authors_crossref_str) or not str(authors_crossref_str).strip() or "Erreur" in authors_crossref_str or "Timeout" in authors_crossref_str :
                                        return ""
                                    
                                    authors_from_pub = [a.strip() for a in str(authors_crossref_str).split(';') if a.strip()]
                                    noms_detectes_originaux = set() 

                                    for author_pub_orig in authors_from_pub:
                                        author_pub_norm = normalize_name(author_pub_orig) 
                                        author_pub_initial_norm = get_initial_form(author_pub_norm) 

                                        match_complet = get_close_matches(author_pub_norm, chercheur_map_norm.keys(), n=1, cutoff=0.85) 
                                        if match_complet:
                                            noms_detectes_originaux.add(chercheur_map_norm[match_complet[0]])
                                            continue 

                                        match_initial = get_close_matches(author_pub_initial_norm, initial_map_norm.keys(), n=1, cutoff=0.9) 
                                        if match_initial:
                                            noms_detectes_originaux.add(initial_map_norm[match_initial[0]])
                                            
                                    return "; ".join(sorted(list(noms_detectes_originaux))) if noms_detectes_originaux else ""

                                final_df['Auteurs_Laboratoire_Détectés'] = final_df['Auteurs_Crossref'].apply(detect_known_authors_optimized)
                                st.success("Comparaison des auteurs avec le fichier terminée.")

                    except Exception as e_author_file:
                        st.error(f"Erreur lors du traitement du fichier des auteurs : {e_author_file}")
            elif compare_authors and not uploaded_authors_file:
                st.warning("Veuillez téléverser un fichier CSV de chercheurs pour la comparaison des auteurs.")
            elif compare_authors and not collection_a_chercher:
                 st.warning("Veuillez spécifier un code de collection HAL pour la comparaison des auteurs.")

        progress_bar.progress(90) # Avant affichage et DL
        st.success("Déduction des actions et traitement des auteurs terminés.")
        
        st.dataframe(final_df)
        

       
        if not final_df.empty:
            csv_export = final_df.to_csv(index=False, encoding='utf-8-sig')
            filename_coll_part = str(collection_a_chercher).replace(" ", "_") if collection_a_chercher else "HAL_global"
            output_filename = f"c2LabHAL_resultats_{filename_coll_part}_{start_year}-{end_year}.csv"

            st.download_button(
                label="📥 Télécharger les résultats en CSV",
                data=csv_export,
                file_name=output_filename,
                mime="text/csv"
            )
            st.info("""
            **Si vous utilisez Excel, en cas de problème de lecture du fichier .csv :**
            Utilisez l'assistant d'importation de texte d'Excel.
            - **Ouvrez Excel** et créez une nouvelle feuille de calcul vierge.
            - Allez dans l'onglet **Données** (Data).
            - Dans le groupe **Obtenir et transformer des données** (Get & Transform Data), cliquez sur **À partir d'un fichier texte/CSV** (From Text/CSV).
            - Sélectionnez le fichier CSV que vous avez téléchargé et cliquez sur **Importer**.
            
            Une fenêtre d'aperçu s'ouvrira. Assurez-vous que les paramètres sont corrects :
            - **Origine du fichier** (File Origin) ou **Encodage** : Sélectionnez **`65001 : Unicode (UTF-8)`**.
            - **Séparateur** (Delimiter) : Assurez-vous que **`Virgule`** (Comma) est sélectionné.
            - Cliquez sur **Charger** (Load). Excel importera les données correctement, avec les colonnes bien séparées et les caractères spéciaux affichés sans problème.
            """)
            
           # --- Bloc de Visualisations ---
        st.header("📊 Visualisations des données")
        
        # S'assurer que les données nécessaires existent
        if 'Action' in final_df.columns and 'oa_status' in final_df.columns and 'Date' in final_df.columns:
            
            # --- Graphique 1 : Évolution du nombre de publications par année ---
            st.subheader("Nombre de publications par année")
            
            # Convertir la colonne 'Date' en type datetime et extraire l'année
            # Utiliser une copie pour éviter le SettingWithCopyWarning
            df_for_viz = final_df.copy()
            df_for_viz['Year'] = pd.to_datetime(df_for_viz['Date'], errors='coerce').dt.year
            
            # Filtrer les années valides et compter
            publications_by_year = df_for_viz.dropna(subset=['Year']).groupby('Year').size().reset_index(name='Count')
            
            # Créer le graphique avec Altair pour l'interactivité
            if not publications_by_year.empty:
                import altair as alt
                
                chart_year = alt.Chart(publications_by_year).mark_bar().encode(
                    x=alt.X('Year:O', title='Année'),
                    y=alt.Y('Count:Q', title='Nombre de publications'),
                    tooltip=[
                        alt.Tooltip('Year:O', title='Année'),
                        alt.Tooltip('Count:Q', title='Nombre de publications')
                    ]
                ).properties(
                    title='Évolution du nombre de publications par année'
                )
                
                st.altair_chart(chart_year, use_container_width=True)
            else:
                st.warning("Pas de données de publication par année pour la visualisation.")

            # --- Graphique 2 : Répartition des publications par statut OA (Unpaywall) ---
            st.subheader("Répartition par statut Open Access")
            
            # Compter la répartition des statuts OA
            oa_status_counts = final_df['oa_status'].value_counts().reset_index()
            oa_status_counts.columns = ['Statut OA', 'Count']

            if not oa_status_counts.empty:
                # Définir le mapping des couleurs
                color_map = {
                    'closed': 'black',
                    'green': 'green',
                    'bronze': 'saddlebrown', # Une teinte de marron
                    'gold': 'gold',
                    'hybrid': 'gray'
                }
                
                # Créer une liste de couleurs dans le bon ordre en fonction des étiquettes
                colors = [color_map.get(status.lower(), 'lightgray') for status in oa_status_counts['Statut OA']]

                # Créer un camembert avec Plotly en utilisant les couleurs personnalisées
                import plotly.graph_objects as go
                fig_pie = go.Figure(data=[go.Pie(
                    labels=oa_status_counts['Statut OA'], 
                    values=oa_status_counts['Count'], 
                    marker_colors=colors, # Utilisation de la liste de couleurs
                    hole=.3
                )])
                fig_pie.update_layout(title_text='Répartition par statut Open Access')
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.warning("Pas de données de statut Open Access pour la visualisation.")
                
# --- Graphique 3 : Répartition par Statut_HAL ---
            st.subheader("Répartition par Statut HAL")
            
            # Vérifier si la colonne existe pour éviter une KeyError
            if 'Statut_HAL' in final_df.columns:
                
                # Créer une copie du DataFrame pour le graphique
                df_statut_hal = final_df.copy()

                # Gérer les valeurs manquantes si nécessaire (s'il y en a)
                df_statut_hal['Statut_HAL'] = df_statut_hal['Statut_HAL'].fillna('Statut Inconnu')

                # Compter la répartition des statuts
                statut_hal_counts = df_statut_hal['Statut_HAL'].value_counts().reset_index()
                statut_hal_counts.columns = ['Statut HAL', 'Count']
                
                if not statut_hal_counts.empty:
                    # Créer un camembert avec Plotly
                    import plotly.graph_objects as go
                    fig_pie_statut_hal = go.Figure(data=[go.Pie(
                        labels=statut_hal_counts['Statut HAL'],
                        values=statut_hal_counts['Count'],
                        hole=.3
                    )])
                    fig_pie_statut_hal.update_layout(title_text='Répartition par Statut HAL')
                    st.plotly_chart(fig_pie_statut_hal, use_container_width=True)
                else:
                    st.warning("Pas de données pour la visualisation du statut HAL.")
            else:
                st.warning("La colonne 'Statut_HAL' n'est pas trouvée dans les données.")        

            
           # --- Graphique 4 : Répartition par type de dépôt HAL ---
            st.subheader("Répartition par type de dépôt HAL")

            # Créer une copie du DataFrame
            df_hal_depot = final_df.copy()

            # S'assurer que les chaînes vides sont traitées comme des valeurs manquantes
            df_hal_depot['type_dépôt_si_trouvé'].replace('', pd.NA, inplace=True)

            # Définir le mapping des noms
            nom_map = {
                'file': 'Fichier',
                'notice': 'Notice',
                'annex': 'Annexe',
            }

            # Remplacer les valeurs existantes de la colonne en utilisant le dictionnaire
            df_hal_depot['type_dépôt_si_trouvé'] = df_hal_depot['type_dépôt_si_trouvé'].replace(nom_map)
            
            # Compter la répartition des types de dépôt, y compris les NaN
            depot_status_counts = df_hal_depot['type_dépôt_si_trouvé'].value_counts(dropna=False).reset_index()
            depot_status_counts.columns = ['Statut HAL', 'Count']
            
            # Remplacer l'étiquette pour les valeurs NaN par 'Absent de HAL'
            depot_status_counts['Statut HAL'] = depot_status_counts['Statut HAL'].fillna('Absent de HAL')

            if not depot_status_counts.empty:
                # Créer un camembert avec Plotly
                import plotly.graph_objects as go
                fig_pie_hal_depot = go.Figure(data=[go.Pie(
                    labels=depot_status_counts['Statut HAL'],
                    values=depot_status_counts['Count'],
                    hole=.3
                )])
                fig_pie_hal_depot.update_layout(title_text='Statut des dépôts dans HAL')
                st.plotly_chart(fig_pie_hal_depot, use_container_width=True)
            else:
                st.warning("Pas de données sur le statut de dépôt HAL pour la visualisation.")
           
            
        else:
            st.warning("Les colonnes nécessaires pour les visualisations (Action, oa_status, Date) sont manquantes.")                
            
        progress_bar.progress(100)
        progress_text_area.success("🎉 Traitement terminé avec succès !")

if __name__ == "__main__":
    main()
