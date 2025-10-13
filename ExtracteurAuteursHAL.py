# Suite du fichier hal_author_extractor.py, ou dans un app.py séparé
import streamlit as st
import pandas as pd
from hal_author_extractor import (
    get_authors_data, convert_df
)

# --- Fonctions utilitaires pour Streamlit ---
@st.cache_data
def convert_df(df):
    """Convertit un DataFrame en CSV pour le téléchargement."""
    # Utilisation de la même configuration que dans le script (point-virgule comme séparateur)
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

    # Options des champs (similaire à `champsAuteurs` du script JS)
    all_available_fields = [
        'halId_s', 'orcidId_s', 'firstName_s', 'lastName_s', 
        'email_s', 'structHasAuthId_fs', 'birthDateY_i', 'validity_s'
    ]
    
    with st.sidebar:
        st.header("Paramètres de l'Extraction")
        collection_code = st.text_input("Code Collection HAL (ex: CRAO, TEL)", value="CRAO")
        years = st.text_input("Année(s) (ex: 2023, 2020-2023)", value="")
        
        st.subheader("Champs d'Auteur à Récupérer")
        # Sélection des champs. On exclut 'docid', 'fullName_s', 'valid_s' de la sélection car ils sont inclus par défaut.
        fields_selected = st.multiselect(
            "Sélectionnez les champs additionnels", 
            options=all_available_fields,
            default=['halId_s', 'orcidId_s', 'firstName_s', 'lastName_s']
        )
        
        # Bouton d'extraction
        if st.button("Lancer l'Extraction"):
            if not collection_code:
                st.sidebar.error("Veuillez entrer un code de collection HAL.")
                return
            
            # Préparer les champs pour l'API
            api_fields = build_fields_list(fields_selected)
            
            with st.spinner(f"Récupération des publications et auteurs pour la collection **{collection_code}**..."):
                # Exécution de la logique d'extraction
                df_authors = get_authors_data(collection_code, years, api_fields)
            
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
        
        st.header(f"Résultats pour Collection : {collection_code} (Année(s) : {years if years else 'Toutes'})")
        st.info(f"Nombre de formes-auteurs uniques trouvées : **{len(df_authors)}**")
        
        # Afficher le DataFrame
        st.dataframe(df_authors)
        
        # Préparer et afficher le bouton de téléchargement CSV
        csv_data = convert_df(df_authors)
        filename = f'auteurs_uniques_{collection_code}_{years if years else "all"}.csv'
        
        st.download_button(
            label="Télécharger la liste des auteurs (CSV)",
            data=csv_data,
            file_name=filename,
            mime='text/csv',
            key='download_button'
        )

if __name__ == '__main__':
    # Si le script est exécuté directement, lance l'application Streamlit
    if 'streamlit' in globals(): # Vérifie si Streamlit est en cours d'exécution
        main()
    else:
        # Permet d'exécuter la partie de test standalone si on n'est pas dans Streamlit
        # (Laissez le bloc `if __name__ == '__main__':` de la section 1 en haut)
        # S'assurer que les fonctions sont définies avant cet appel ou importées.
        pass
