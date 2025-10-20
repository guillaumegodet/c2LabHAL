import streamlit as st
import pandas as pd
import io

# Importer les fonctions nécessaires depuis utils.py
from utils import (
    check_df,
    enrich_w_upw_parallel,
    add_permissions_parallel,
    deduce_todo,
    # normalise, # Utilisé indirectement via HalCollImporter et check_df
    HalCollImporter
)

# Fonction pour ajouter le menu de navigation dans la barre latérale (spécifique à cette app)
def add_sidebar_menu():
    st.sidebar.header("À Propos")
    st.sidebar.info(
        """
        **c2LabHAL - Version CSV** :
        Cet outil permet de comparer une liste de publications (fournie via un fichier CSV contenant au minimum les colonnes 'doi' et 'Title')
        avec une collection HAL spécifique. Il enrichit également les données avec Unpaywall et les permissions de dépôt.
        """
    )
    st.sidebar.markdown("---")
   
    st.sidebar.header("Autres applications c2LabHAL")
    st.sidebar.markdown("📖 [c2LabHAL - Application Principale](https://c2labhal.streamlit.app/)")
    st.sidebar.markdown("🏛️ [c2LabHAL version Nantes Université](https://c2labhal-nantes.streamlit.app/)")
    st.sidebar.markdown("🔗 [Alignez une liste de chercheurs avec IdRef et HAL](https://c2labhal-auteurs.streamlit.app/)")


    st.sidebar.markdown("---")
   
    st.sidebar.markdown("Présentation du projet :")
    st.sidebar.markdown("[📊 Voir les diapositives](https://slides.com/guillaumegodet/deck-d5bc03#/2)")
    st.sidebar.markdown("Code source :")
    st.sidebar.markdown("[🐙 Voir sur GitHub](https://github.com/GuillaumeGodet/c2labhal)")


def process_csv(uploaded_file_data, collection_hal_code, start_year_hal, end_year_hal, progress_bar_st, progress_text_area_st):
    """
    Traite le fichier CSV uploadé, effectue les vérifications HAL et Unpaywall,
    et retourne le DataFrame enrichi.
    """
    try:
        df_input = pd.read_csv(uploaded_file_data)
    except Exception as e:
        st.error(f"Erreur lors de la lecture du fichier CSV : {e}")
        return None

    if 'doi' not in df_input.columns and 'Title' not in df_input.columns:
        st.error("Le fichier CSV doit contenir au moins une colonne 'doi' ou une colonne 'Title'.")
        return None
    
    if 'doi' not in df_input.columns:
        df_input['doi'] = pd.NA
    if 'Title' not in df_input.columns:
        df_input['Title'] = "" # Les titres peuvent être des chaînes vides

    if 'doi' in df_input.columns: # Nettoyer les DOI
        df_input['doi'] = df_input['doi'].astype(str).str.lower().str.strip().replace(['nan', ''], pd.NA)

    progress_text_area_st.info("Étape 1/5 : Importation de la collection HAL...")
    progress_bar_st.progress(10)
    
    coll_importer_obj = HalCollImporter(collection_hal_code, start_year_hal, end_year_hal)
    coll_df_hal = coll_importer_obj.import_data() 
    if coll_df_hal.empty:
        st.warning(f"La collection HAL '{collection_hal_code}' est vide ou n'a pas pu être chargée pour {start_year_hal}-{end_year_hal}.")
    else:
        st.success(f"{len(coll_df_hal)} notices trouvées dans la collection HAL '{collection_hal_code}'.")
    # La barre de progression est mise à jour par HalCollImporter si st.progress y est passé, sinon manuellement ici.
    # Pour l'instant, on gère la progression globale ici.
    progress_bar_st.progress(25)


    progress_text_area_st.info("Étape 2/5 : Comparaison avec les données HAL...")
    df_checked_hal = check_df(df_input.copy(), coll_df_hal, progress_bar_st=progress_bar_st, progress_text_st=progress_text_area_st) 
    st.success("Comparaison HAL terminée.")
    # check_df gère sa propre progression jusqu'à la fin de son étape

    progress_text_area_st.info("Étape 3/5 : Enrichissement avec Unpaywall...")
    progress_bar_st.progress(50) # Marquer le début de l'étape Unpaywall
    df_enriched_upw = enrich_w_upw_parallel(df_checked_hal.copy())
    st.success("Enrichissement Unpaywall terminé.")
    progress_bar_st.progress(70)

    progress_text_area_st.info("Étape 4/5 : Récupération des permissions de dépôt...")
    df_enriched_perms = add_permissions_parallel(df_enriched_upw.copy())
    st.success("Récupération des permissions OA.works terminée.")
    progress_bar_st.progress(85)

    progress_text_area_st.info("Étape 5/5 : Déduction des actions...")
    if 'Action' not in df_enriched_perms.columns:
        df_enriched_perms['Action'] = pd.NA
    df_enriched_perms['Action'] = df_enriched_perms.apply(deduce_todo, axis=1)
    st.success("Déduction des actions terminée.")
    progress_bar_st.progress(100)

    return df_enriched_perms


def main():
    st.set_page_config(page_title="c2LabHAL - CSV", layout="wide")
    add_sidebar_menu() 

    st.title("🥎 c2LabHAL - Version import CSV")
    st.subheader("Comparez les publications d'un fichier CSV avec une collection HAL et enrichissez les données.")

    uploaded_file = st.file_uploader(
        "📤 Téléversez un fichier CSV", 
        type="csv",
        help="Votre fichier CSV doit contenir au minimum une colonne 'doi' ou une colonne 'Title'. Les deux sont recommandées."
    )
    collection_a_chercher_csv = st.text_input(
        "Code de la collection HAL à comparer (ex: MIP)", 
        "",
        help="Laissez vide pour comparer avec tout HAL (non recommandé, peut être très long et moins précis)."
    )
    
    st.markdown("##### Période pour l'extraction de la collection HAL :")
    col1_date_csv, col2_date_csv = st.columns(2)
    with col1_date_csv:
        start_year_coll_csv = st.number_input("Année de début (collection HAL)", min_value=1900, max_value=2100, value=2018, key="csv_start_year")
    with col2_date_csv:
        end_year_coll_csv = st.number_input("Année de fin (collection HAL)", min_value=1900, max_value=2100, value=pd.Timestamp.now().year, key="csv_end_year")

    progress_bar_main_csv = st.progress(0)
    progress_text_area_main_csv = st.empty()

    if st.button("🚀 Lancer le traitement du CSV"):
        if uploaded_file and collection_a_chercher_csv:
            progress_text_area_main_csv.info("Traitement du fichier CSV en cours...")
            processed_df_csv = process_csv(uploaded_file, collection_a_chercher_csv, start_year_coll_csv, end_year_coll_csv, progress_bar_main_csv, progress_text_area_main_csv)
            
            if processed_df_csv is not None and not processed_df_csv.empty:
                st.dataframe(processed_df_csv)
                csv_export_data = processed_df_csv.to_csv(index=False, encoding='utf-8-sig')
                
                filename_coll_part_csv = str(collection_a_chercher_csv).replace(" ", "_")
                output_filename_csv = f"c2LabHAL_resultats_CSV_{filename_coll_part_csv}.csv"
                
                st.download_button(
                    label="📥 Télécharger le CSV enrichi",
                    data=csv_export_data,
                    file_name=output_filename_csv,
                    mime="text/csv"
                )
                progress_text_area_main_csv.success("🎉 Traitement terminé avec succès !")
            elif processed_df_csv is not None and processed_df_csv.empty:
                st.warning("Le traitement n'a produit aucun résultat. Vérifiez le contenu de votre fichier CSV et les paramètres.")
                progress_text_area_main_csv.warning("Aucun résultat à afficher ou télécharger.")
            # else: # Cas où processed_df_csv is None (erreur déjà gérée dans process_csv)
                # progress_text_area_main_csv.error("Le traitement a échoué. Veuillez vérifier les messages d'erreur.")

        elif not uploaded_file:
            st.error("Veuillez téléverser un fichier CSV.")
            progress_text_area_main_csv.empty()
            progress_bar_main_csv.progress(0)
        elif not collection_a_chercher_csv: # collection_a_chercher_csv est requis ici
            st.error("Veuillez spécifier un code de collection HAL à comparer.")
            progress_text_area_main_csv.empty()
            progress_bar_main_csv.progress(0)

if __name__ == "__main__":
    main()
