import streamlit as st
import pandas as pd
from io import StringIO
import datetime # Ajout de l'import manquant
from pydref import Pydref # Importez votre classe Pydref

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

try:
    pydref_api = get_pydref_instance()
except Exception as e:
    st.error(f"Erreur lors de l'initialisation de Pydref. Vérifiez les dépendances de 'pydref.py': {e}")
    st.stop()


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
        # Afficher l'erreur mais ne pas stopper l'application
        st.warning(f"Erreur lors de la recherche pour '{full_name}': {e}") 
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
            # Enlever l'argument StringIO non nécessaire pour st.file_uploader
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
        
        # Logique de sélection des colonnes
        cols = data.columns.tolist()
        
        def find_default_index(candidates, cols):
            for i, col in enumerate(cols):
                if col.lower() in candidates:
                    return i
            return 0 if cols else None

        name_default_idx = find_default_index(['nom', 'last_name', 'surname'], cols)
        firstname_default_idx = find_default_index(['prénom', 'prenom', 'first_name'], cols)


        # Sélection des colonnes Nom et Prénom
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
        
        # Paramètres de filtrage
        st.markdown("---")
        st.markdown("**Filtres additionnels**")
        
        col_date1, col_date2, col_scientific, col_exact = st.columns(4)
        
        current_year = datetime.datetime.now().year
        min_birth_year = col_date1.number_input("Année de naissance min. (YYYY)", value=1920, min_value=1000, max_value=current_year, step=1)
        min_death_year = col_date2.number_input("Année de décès min. (YYYY)", value=2005, min_value=1000, max_value=current_year + 5, step=1)
        is_scientific = col_scientific.checkbox("Filtrer les non-scientifiques", value=True)
        exact_fullname = col_exact.checkbox("Exiger une correspondance exacte du nom complet", value=True)

        
        # --- 3. Démarrage de la recherche ---
        if st.button("Lancer la recherche IdRef", type="primary"):
            if not name_column or not firstname_column:
                st.error("Veuillez sélectionner les colonnes Nom et Prénom.")
            else:
                st.info("Recherche en cours... Veuillez ne pas fermer l'onglet.")
                
                # Liste pour stocker les résultats enrichis
                all_results = []
                progress_bar = st.progress(0, text="Progression de la recherche...")
                
                # Itération sur chaque ligne du DataFrame
                for index, row in data.iterrows():
                    
                    # Correction: S'assurer que les valeurs ne sont pas NaN ou None avant de les convertir en string
                    name = str(row[name_column]) if pd.notna(row[name_column]) else ""
                    first_name = str(row[firstname_column]) if pd.notna(row[firstname_column]) else ""
                    full_name = f"{first_name} {name}".strip()

                    # Lancer la recherche uniquement si le nom n'est pas vide
                    if not full_name:
                        matches = []
                    else:
                        matches = search_idref_for_person(
                            full_name=full_name,
                            min_birth_year=min_birth_year,
                            min_death_year=min_death_year,
                            is_scientific=is_scientific,
                            exact_fullname=exact_fullname
                        )
                    
                    # --- LOGIQUE DE CONSOLIDATION SUR UNE SEULE LIGNE ---
                    
                    original_data = row.to_dict()
                    result_row = {
                        **original_data,
                        'query_name': full_name,
                        'idref_status': 'not_found', # Statut par défaut
                        'nb_matches': len(matches), # Nombre de correspondances
                        'idref_ppn': None, # PPN(s) concaténé(s)
                        'match_info': None # Informations sur les correspondances
                    }

                    if matches:
                        all_ppns = []
                        all_match_info = []
                        
                        # Récupérer tous les PPNs et les informations de base
                        for match in matches:
                            ppn = match.get('idref', '').replace('idref', '')
                            all_ppns.append(ppn)
                            
                            # Construire une chaîne d'information concise
                            birth_year = match.get('birth_date', '????')[:4]
                            death_year = match.get('death_date', '????')[:4]
                            match_details = (
                                f"{match.get('last_name')} {match.get('first_name')} "
                                f"({birth_year}-{death_year})"
                            )
                            all_match_info.append(match_details)


                        # Mise à jour des champs pour la ligne unique
                        result_row['idref_ppn'] = " | ".join(all_ppns)
                        result_row['match_info'] = " | ".join(all_match_info)
                        
                        if len(matches) == 1:
                            result_row['idref_status'] = 'found'
                            
                            # Pour le cas unique, on détaille les colonnes séparément
                            match_unique = matches[0]
                            result_row['last_name_match'] = match_unique.get('last_name')
                            result_row['first_name_match'] = match_unique.get('first_name')
                            result_row['birth_date_match'] = match_unique.get('birth_date', 'N/A')
                            result_row['death_date_match'] = match_unique.get('death_date', 'N/A')
                            result_row['gender_match'] = match_unique.get('gender', 'N/A')
                            result_row['description_match'] = '; '.join(match_unique.get('description', []))


                        elif len(matches) > 1:
                            result_row['idref_status'] = 'ambiguous'
                        
                        
                    # Ajouter la ligne unique à la liste
                    all_results.append(result_row)

                    # Mise à jour de la barre de progression
                    progress_bar.progress((index + 1) / len(data))

                # --- 4. Affichage et Téléchargement des résultats ---
                results_df = pd.DataFrame(all_results)
                
                st.subheader("Résultats de la recherche")
                st.dataframe(results_df)

                # Bouton de téléchargement
                # Utiliser to_csv avec encoding utf-8 pour supporter les caractères spéciaux
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
        # Erreur générale de traitement de fichier (e.g. format incorrect)
        st.exception(e)
        st.error(f"Une erreur est survenue lors du traitement du fichier : {e}")
        st.info("Vérifiez que le format du fichier et les colonnes sélectionnées sont corrects.")
