import streamlit as st
import pandas as pd
from io import StringIO
import datetime
from pydref import Pydref

st.set_page_config(
    page_title="Recherche d'identifiants IdRef",
    layout="wide"
)

@st.cache_resource
def get_pydref_instance():
    return Pydref()

try:
    pydref_api = get_pydref_instance()
except Exception as e:
    st.error(f"Erreur lors de l'initialisation de Pydref. Vérifiez les dépendances de 'pydref.py': {e}")
    st.stop()


def search_idref_for_person(full_name, min_birth_year, min_death_year):
    try:
        results = pydref_api.get_idref(
            query=full_name,
            min_birth_year=min_birth_year,
            min_death_year=min_death_year,
            is_scientific=True,           # toujours filtrer les non-scientifiques
            exact_fullname=True            # toujours exiger correspondance exacte (forme retenue ou rejetée)
        )
        return results
    except Exception as e:
        st.warning(f"Erreur lors de la recherche pour '{full_name}': {e}") 
        return []

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

    st.sidebar.markdown("---")
    
    st.sidebar.markdown("Présentation du projet :")
    st.sidebar.markdown("[📊 Voir les diapositives](https://slides.com/guillaumegodet/deck-d5bc03#/2)")
    st.sidebar.markdown("Code source :")
    st.sidebar.markdown("[🐙 Voir sur GitHub](https://github.com/GuillaumeGodet/c2labhal)")

# Fonction principale
def main():
    st.title("🔗 Alignez une liste de chercheurs avec IdRef")
    st.markdown("Téléversez un fichier CSV ou Excel contenant une liste de personnes pour récupérer leurs identifiants IdRef. Ce fichier doit contenir a minima une colonne Nom et une colonne Prénom.")
    
    uploaded_file = st.file_uploader(
        "Téléverser votre fichier (.csv, .xlsx)",
        type=["csv", "xlsx"]
    )
    
    if uploaded_file is not None:
        try:
            if uploaded_file.name.endswith('.csv'):
                data = pd.read_csv(uploaded_file) 
            else:
                data = pd.read_excel(uploaded_file)
    
            st.success(f"Fichier **{uploaded_file.name}** chargé avec succès. {len(data)} lignes trouvées.")
            st.subheader("Aperçu des données")
            st.dataframe(data.head())
    
            st.subheader("Configuration et Paramètres de recherche")
            col_name, col_firstname = st.columns(2)
            cols = data.columns.tolist()
    
            def find_default_index(candidates, cols):
                for i, col in enumerate(cols):
                    if col.lower() in candidates:
                        return i
                return 0 if cols else None
    
            name_default_idx = find_default_index(['nom', 'last_name', 'surname'], cols)
            firstname_default_idx = find_default_index(['prénom', 'prenom', 'first_name'], cols)
    
            name_column = col_name.selectbox(
                "Colonne contenant le **Nom** :",
                options=cols,
                index=name_default_idx
            )
            
            firstname_column = col_firstname.selectbox(
                "Colonne contenant le **Prénom** :",
                options=cols,
                index=firstname_default_idx
            )
            
            st.markdown("---")
            st.markdown("**Filtres additionnels**")
            
            col_date1, col_date2 = st.columns(2)
            
            current_year = datetime.datetime.now().year
            min_birth_year = col_date1.number_input("Année de naissance min. (YYYY)", value=1920, min_value=1000, max_value=current_year, step=1)
            min_death_year = col_date2.number_input("Année de décès min. (YYYY)", value=2005, min_value=1000, max_value=current_year + 5, step=1)
    
            if st.button("Lancer la recherche IdRef", type="primary"):
                if not name_column or not firstname_column:
                    st.error("Veuillez sélectionner les colonnes Nom et Prénom.")
                else:
                    st.info("Recherche en cours... Veuillez ne pas fermer l'onglet.")
                    
                    all_results = []
                    progress_bar = st.progress(0, text="Progression de la recherche...")
                    
                    for index, row in data.iterrows():
                        name = str(row[name_column]) if pd.notna(row[name_column]) else ""
                        first_name = str(row[firstname_column]) if pd.notna(row[firstname_column]) else ""
                        full_name = f"{first_name} {name}".strip()
    
                        if not full_name:
                            matches = []
                        else:
                            matches = search_idref_for_person(
                                full_name=full_name,
                                min_birth_year=min_birth_year,
                                min_death_year=min_death_year
                            )
                        
                        original_data = row.to_dict()
                        result_row = {
                            **original_data,
                            'query_name': full_name,
                            'idref_status': 'not_found',
                            'nb_matches': len(matches),
                            'idref_ppn': None,
                            'match_info': None,
                            'alt_names': None,
                            'orcid': None
                        }
    
                        if matches:
                            all_ppns = []
                            all_match_info = []
                            alt_names_all = []
                            orcid_list = []
    
                            for match in matches:
                                ppn = match.get('idref', '').replace('idref', '')
                                all_ppns.append(ppn)
    
                                birth_year = match.get('birth_date', '????')[:4]
                                death_year = match.get('death_date', '????')[:4]
                                match_details = (
                                    f"{match.get('last_name')} {match.get('first_name')} "
                                    f"({birth_year}-{death_year})"
                                )
                                all_match_info.append(match_details)
    
                                if 'alt_names' in match:
                                    alt_names_all.extend(match['alt_names'])
    
                                for identifier in match.get('identifiers', []):
                                    if 'orcid' in identifier:
                                        orcid_list.append(identifier['orcid'])
    
                            result_row['idref_ppn'] = " | ".join(all_ppns)
                            result_row['match_info'] = " | ".join(all_match_info)
                            result_row['alt_names'] = " | ".join(set(alt_names_all)) if alt_names_all else None
                            result_row['orcid'] = " | ".join(set(orcid_list)) if orcid_list else None
                            
                            if len(matches) == 1:
                                result_row['idref_status'] = 'found'
                                match_unique = matches[0]
                                result_row['last_name_match'] = match_unique.get('last_name')
                                result_row['first_name_match'] = match_unique.get('first_name')
                                result_row['birth_date_match'] = match_unique.get('birth_date', 'N/A')
                                result_row['death_date_match'] = match_unique.get('death_date', 'N/A')
                                result_row['gender_match'] = match_unique.get('gender', 'N/A')
                                result_row['description_match'] = '; '.join(match_unique.get('description', []))
                            elif len(matches) > 1:
                                result_row['idref_status'] = 'ambiguous'
    
                        all_results.append(result_row)
                        progress_bar.progress((index + 1) / len(data))
    
                    results_df = pd.DataFrame(all_results)
                    
                    st.subheader("Résultats de la recherche")
                    st.dataframe(results_df)
    
                    csv_output = results_df.to_csv(index=False, encoding='utf-8').encode('utf-8')
                    st.download_button(
                        label="💾 Télécharger les résultats en CSV",
                        data=csv_output,
                        file_name=f"idref_results_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                    )
                    st.success("Recherche terminée ! Prête à télécharger.")
    
        except ImportError as ie:
            st.error(f"Erreur d'importation : {ie}. Avez-vous mis à jour votre requirements.txt et redéployé l'application ?")
    
        except Exception as e:
            st.exception(e)
            st.error(f"Une erreur est survenue lors du traitement du fichier : {e}")
            st.info("Vérifiez que le format du fichier et les colonnes sélectionnées sont corrects.")
