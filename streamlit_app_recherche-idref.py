import streamlit as st
import pandas as pd
from io import StringIO
from pydref import Pydref # Importez votre classe Pydref
import datetime

# --- Configuration de la page Streamlit ---
st.set_page_config(
    page_title="Recherche d'identifiants IdRef",
    layout="wide"
)

# --- Initialisation de l'objet Pydref ---
@st.cache_resource
def get_pydref_instance():
    """Crée et met en cache l'instance de Pydref."""
    return Pydref()

pydref_api = get_pydref_instance()

# --- Fonction principale de recherche IdRef ---
def search_idref_for_person(full_name, min_birth_year, min_death_year, is_scientific, exact_fullname):
    """
    Lance la recherche IdRef pour un nom donné en utilisant la méthode get_idref,
    qui permet d'obtenir plusieurs correspondances.
    """
    try:
        results = pydref_api.get_idref(
            query=full_name,
            min_birth_year=min_birth_year,
            min_death_year=min_death_year,
            is_scientific=is_scientific,
            exact_fullname=exact_fullname
        )
        return results
    except Exception as e:
        st.error(f"Erreur lors de la recherche pour {full_name}: {e}")
        return []


# --- Interface utilisateur Streamlit ---
st.title("🔗 Outil d'identification IdRef à partir d'un fichier")
st.markdown("Téléversez un fichier CSV/Excel contenant une liste de personnes pour récupérer leurs identifiants IdRef.")

# --- 1. Téléversement du fichier ---
uploaded_file = st.file_uploader(
    "Téléverser votre fichier (.csv, .xlsx)",
    type=["csv", "xlsx"]
)

if uploaded_file is not None:
    # Lecture du fichier
    try:
        if uploaded_file.name.endswith('.csv'):
            data = pd.read_csv(uploaded_file)
        else: # .xlsx
            data = pd.read_excel(uploaded_file)

        st.success(f"Fichier **{uploaded_file.name}** chargé avec succès. {len(data)} lignes trouvées.")
        
        # Affichage des premières lignes pour vérification
        st.subheader("Aperçu des données")
        st.dataframe(data.head())
        
        # --- 2. Configuration des colonnes et des paramètres ---
        st.subheader("Configuration et Paramètres de recherche")
        
        col_name, col_firstname = st.columns(2)
        
        # Sélection des colonnes Nom et Prénom
        name_column = col_name.selectbox(
            "Colonne contenant le **Nom** :",
            options=data.columns,
            index=0 if 'nom' in data.columns.str.lower() else (0 if data.columns.size > 0 else None)
        )
        
        firstname_column = col_firstname.selectbox(
            "Colonne contenant le **Prénom** :",
            options=data.columns,
            index=1 if 'prénom' in data.columns.str.lower() or 'prenom' in data.columns.str.lower() else (1 if data.columns.size > 1 else None)
        )
        
        # Paramètres de filtrage (issus de la méthode identify/get_idref)
        st.markdown("---")
        st.markdown("**Filtres additionnels**")
        
        col_date1, col_date2, col_scientific, col_exact = st.columns(4)
        
        min_birth_year = col_date1.number_input("Année de naissance min. (YYYY)", value=1920, min_value=1000, max_value=datetime.datetime.now().year, step=1)
        min_death_year = col_date2.number_input("Année de décès min. (YYYY)", value=2005, min_value=1000, max_value=datetime.datetime.now().year, step=1)
        is_scientific = col_scientific.checkbox("Filtrer les non-scientifiques (selon NOT_SCIENTIST_TOKEN)", value=True)
        exact_fullname = col_exact.checkbox("Exiger une correspondance exacte du nom complet", value=True)

        
        # --- 3. Démarrage de la recherche ---
        if st.button("Lancer la recherche IdRef", type="primary"):
            if not name_column or not firstname_column:
                st.error("Veuillez sélectionner les colonnes Nom et Prénom.")
            else:
                st.info("Recherche en cours... Cela peut prendre un moment en fonction du nombre de lignes.")
                
                # Liste pour stocker les résultats enrichis
                all_results = []
                progress_bar = st.progress(0)
                
                # Itération sur chaque ligne du DataFrame
                for index, row in data.iterrows():
                    full_name = f"{row[firstname_column]} {row[name_column]}".strip()
                    
                    # Logique de recherche
                    matches = search_idref_for_person(
                        full_name=full_name,
                        min_birth_year=min_birth_year,
                        min_death_year=min_death_year,
                        is_scientific=is_scientific,
                        exact_fullname=exact_fullname
                    )
                    
                    # Traitement des résultats pour le DataFrame final
                    original_data = row.to_dict()
                    
                    if matches:
                        # Cas avec des correspondances
                        for i, match in enumerate(matches):
                            result_row = {
                                **original_data,
                                'query_name': full_name,
                                'idref_status': 'found' if len(matches) == 1 else 'ambiguous',
                                'match_rank': i + 1,
                                'idref_ppn': match.get('idref', '').replace('idref', ''),
                                'last_name_match': match.get('last_name'),
                                'first_name_match': match.get('first_name'),
                                'birth_date_match': match.get('birth_date', 'N/A'),
                                'death_date_match': match.get('death_date', 'N/A'),
                                'gender_match': match.get('gender', 'N/A'),
                                'identifiers_match': ', '.join([f"{k}:{v}" for d in match.get('identifiers', []) for k, v in d.items()]),
                                'description_match': '; '.join(match.get('description', [])),
                            }
                            all_results.append(result_row)
                    else:
                        # Cas sans correspondance
                        all_results.append({
                            **original_data,
                            'query_name': full_name,
                            'idref_status': 'not_found',
                            'match_rank': 1,
                            'idref_ppn': None,
                            'last_name_match': None,
                            'first_name_match': None,
                            'birth_date_match': None,
                            'death_date_match': None,
                            'gender_match': None,
                            'identifiers_match': None,
                            'description_match': None,
                        })

                    # Mise à jour de la barre de progression
                    progress_bar.progress((index + 1) / len(data))

                # --- 4. Affichage et Téléchargement des résultats ---
                results_df = pd.DataFrame(all_results)
                
                st.subheader("Résultats de la recherche")
                st.dataframe(results_df)

                # Bouton de téléchargement
                csv_output = results_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="💾 Télécharger les résultats en CSV",
                    data=csv_output,
                    file_name=f"idref_results_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                )
                st.success("Recherche terminée !")

    except Exception as e:
        st.error(f"Une erreur est survenue lors du traitement du fichier : {e}")
        st.info("Vérifiez que vous avez bien sélectionné les colonnes Nom et Prénom appropriées.")
