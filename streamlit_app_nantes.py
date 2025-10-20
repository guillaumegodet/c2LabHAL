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
        "collection": "CAPHI", "scopus_id": "60105490", "openalex_id": "I4387152714",
        "pubmed_query": "(CAPHI[Affiliation]) OR (\"CENTRE ATLANTIQUE DE PHILOSOPHIE\"[Affiliation]) OR (\"EA 7463\" [Affiliation]) OR (EA7463[Affiliation]) OR (UR7463[Affiliation]) OR (\"UR 7463\"[Affiliation])"
    },
    {
        "collection": "CFV", "scopus_id": "60105524", "openalex_id": "I4387153064",
        "pubmed_query": "(CFV[Affiliation]) OR (\"EA 1161\"[Affiliation]) OR (Viete[Affiliation])"
    },
    {"collection": "CREAAH", "scopus_id": "60105602", "openalex_id": "I4387153012", "pubmed_query": ""},
    {
        "collection": "CREN", "scopus_id": "60105539", "openalex_id": "I4387152322",
        "pubmed_query": "(CREN[Affiliation]) OR (2661[Affiliation]) OR (\"Ctr Rech Educ\"[Affiliation])"
    },
    {"collection": "CRHIA", "scopus_id": "60105526", "openalex_id": "I4399598365", "pubmed_query": ""},
    {"collection": "CRINI", "scopus_id": "60105525", "openalex_id": "I4387153799", "pubmed_query": ""},
    {
        "collection": "ESO", "scopus_id": "60105581", "openalex_id": "I4387153532",
        "pubmed_query": "(\"UMR 6590\"[Affiliation]) OR (\"Espaces et Sociétés\"[Affiliation]) OR (ESO Nantes[Affiliation]) OR (ESO-Angers[Affiliation]) NOT (\"UFR Sciences Espaces et Sociétés\"[Affiliation])"
    },
    {"collection": "LAMO", "scopus_id": "60105566", "openalex_id": "I4387152722", "pubmed_query": ""},
    {
        "collection": "LETG", "scopus_id": "60105608", "openalex_id": "I4387153176",
        "pubmed_query": "(LETG[Affiliation]) OR (UMR6554[Affiliation]) OR (UMR 6554[Affiliation]) OR (UMR CNRS 6554[Affiliation]) OR (Geolittomer[Affiliation])"
    },
    {
        "collection": "LLING", "scopus_id": "60105540", "openalex_id": "I4387152679",
        "pubmed_query": "(\"Laboratoire de linguistique de Nantes\"[Affiliation])"
    },
    {
        "collection": "LPPL", "scopus_id": "60105621", "openalex_id": "I4210089331",
        "pubmed_query": "(LPPL[Affiliation]) OR (Laboratoire de Psychologie des Pays de la Loire[Affiliation]) OR (EA 4638[Affiliation]) OR (EA4638[Affiliation]) OR (EA 3259[Affiliation]) OR (EA3259[Affiliation]) OR (EA 2646[Affiliation])"
    },
    {
        "collection": "CEISAM", "scopus_id": "60105571", "openalex_id": "I4210138474",
        "pubmed_query": "(CEISAM[Affiliation]) OR (UMR6230[Affiliation]) OR (UMR 6230[Affiliation]) OR (LAIEM[Affiliation]) OR (Laboratory of Isotopic and Electrochemical Analysis of Metabolism[Affiliation]) OR (Laboratoire d'Analyse Isotopique et Electrochimique des Métabolismes[Affiliation]) OR (Chimie et Interdisciplinarite Synthese Analyse Modelisation[Affiliation]) OR (CNRS 6230[Affiliation]) OR (CNRS6230[Affiliation]) OR (Interdisciplinary Chemistry Synthesis Analysis Modelling[Affiliation]) OR (CNRS 6513[Affiliation]) OR ((Laboratoire de Synthèse Organique[Affiliation]) AND (nantes[Affiliation])) OR (EA 1149[Affiliation]) OR ((Laboratoire de Spectrochimie[Affiliation]) AND (nantes[Affiliation]) NOT (villeneuve[Affiliation])) OR (UMR 6513[Affiliation]) OR (UMR6006[Affiliation]) OR (UMR 6006[Affiliation])"
    },
    {
        "collection": "GEM", "scopus_id": "60105606", "openalex_id": "I4210137520",
        "pubmed_query": "((GEM[Affiliation]) AND ((nazaire[Affiliation]) OR (nantes[Affiliation])) NOT (chicago[Affiliation])) OR (UMR 6183[Affiliation]) OR (UMR6183[Affiliation]) OR (CNRS 6183[Affiliation]) OR ((Laboratoire de Mécanique et Matériaux [Affiliation]) AND (nantes[Affiliation])) OR (Institute for Research in Civil and Mechanical Engineering[Affiliation]) OR (Research Institute in Civil Engineering and Mechanics[Affiliation]) OR (Institut de Recherche en Génie Civil[Affiliation]) OR (Research Institute in Civil and Mechanical Engineering[Affiliation])"
    },
    {
        "collection": "GEPEA", "scopus_id": "60105518", "openalex_id": "I4210148006",
        "pubmed_query": "((GEPEA[Affiliation]) AND ((nantes[Affiliation]) OR (nazaire[Affiliation]) OR (angers[ad) OR (Nntes Université[Affiliation]))) OR (UMR6144[Affiliation]) OR (UMR 6144[Affiliation]) OR (CNRS 6144[Affiliation]) OR (Génie des Procédés Environnement[Affiliation]) AND ((nantes[Affiliation]) OR (nazaire[Affiliation]))"
    },
    {
        "collection": "IETR", "scopus_id": "60105585", "openalex_id": "I4210100151",
        "pubmed_query": "(IETR[Affiliation]) OR (CNRS 6164[Affiliation]) OR (UMR 6164[Affiliation]) OR (UMR6164[Affiliation]) OR (IETR Polytech[Affiliation]) OR (Institut d'Electronique et des Technologies du numéRique[Affiliation]) OR (Institut d'Électronique et de Télécommunications[Affiliation])"
    },
    {
        "collection": "IMN", "scopus_id": "60020658", "openalex_id": "I4210091049",
        "pubmed_query": "((IMN[Affiliation]) AND (nantes[Affiliation])) OR (CNRS 6502[Affiliation]) OR ((UMR 6502[Affiliation]) AND (nantes[Affiliation])) OR (UMR6502[Affiliation]) OR (Inst. des Mat. Jean Rouxel[Affiliation]) OR (Institut des Matériaux de Nantes Jean Rouxel[Affiliation]) OR (Institut des Matériaux Jean Rouxel[Affiliation])"
    },
    {
        "collection": "IREENA", "scopus_id": "60105577", "openalex_id": "I4392021119",
        "pubmed_query": "(IREENA[Affiliation]) OR (EA 4642[Affiliation]) OR (Institut de Recherche en Energie Electrique de Nantes Atlantique[Affiliation]) OR (Institute of Research in Electric Power of Nantes Atlantique[Affiliation]) OR (Institut de Recherche en Electrotechnique et Electronique de Nantes Atlantique[Affiliation]) OR (Institut de Recherche en Electronique et Electrotechnique de Nantes Atlantique[Affiliation])"
    },
    {
        "collection": "LMJL", "scopus_id": "60105520", "openalex_id": "I4210153365",
        "pubmed_query": "(LMJL[Affiliation]) OR ((Leray[Affiliation]) AND (nantes[Affiliation]) NOT ((Leray[au]) OR (Le Ray[au]))) OR (CNRS 6629[Affiliation]) OR (UMR 6629[Affiliation]) OR (Laboratoire de mathématiques Jean Leray[Affiliation]) OR (Department of Mathematics Jean Leray[Affiliation]) OR (Laboratoire Jean Leray[Affiliation]) OR (Laboratory of Mathematics Jean Leray[Affiliation])"
    },
    {
        "collection": "LPG", "scopus_id": "60105669", "openalex_id": "I4210146808",
        "pubmed_query": "((LPG[Affiliation]) AND (france[Affiliation])) OR (CNRS 6112[Affiliation]) OR (UMR 6112[Affiliation]) OR (UMR6112[Affiliation]) OR (LPGN[Affiliation]) OR (Laboratoire de Planétologie et Géodynamique[Affiliation]) OR (Laboratoire de Planétologie et Géosciences[Affiliation]) OR (Laboratorie du Planétologie et Géosciences[Affiliation])"
    },
    {
        "collection": "LS2N", "scopus_id": "60110511", "openalex_id": "I4210117005",
        "pubmed_query": "(LS2N[Affiliation]) OR (UMR 6004[Affiliation]) OR (UMR6004[Affiliation]) OR ((Cnrs 6004[Affiliation]) AND (nantes[Affiliation])) OR (Laboratoire des Sciences du Numérique[Affiliation]) OR ((Laboratory of Digital Sciences[Affiliation]) NOT (orsay[Affiliation])) OR (IRCCYN[Affiliation]) OR (Cnrs 6597[Affiliation]) OR (Umr 6597[Affiliation]) OR (UMR_C 6597[Affiliation]) OR (Institut de Recherche en Communications et Cybernétique de Nantes[Affiliation]) OR (Research Institute in Communications and Cybernetics of Nantes[Affiliation]) OR (UMR 6241[Affiliation]) OR (UMR6241[Affiliation]) OR (CNRS 6241[Affiliation]) OR (Computer Science Institute of Nantes-Atlantic[Affiliation]) OR (Computer Science Laboratory of Nantes Atlantique[Affiliation]) OR (Laboratoire d'Informatique de Nantes-Atlantique[Affiliation])"
    },
    {
        "collection": "LTEN", "scopus_id": "60105570", "openalex_id": "I4210109587",
        "pubmed_query": "((LTEN[Affiliation]) NOT (Louisville[Affiliation])) OR ((LTN[Affiliation]) AND (nantes[Affiliation])) OR (UMR 6607[Affiliation]) OR (CNRS 6607[Affiliation]) OR (Laboratoire de Thermocinétique[Affiliation]) OR (Laboratoire de Thermique et Energie de Nantes[Affiliation]) OR (Laboratoire Thermique et Energie[Affiliation])"
    },
    {
        "collection": "SUBATECH", "scopus_id": "60008689", "openalex_id": "I4210109007",
        "pubmed_query": "(SUBATECH[Affiliation]) OR (UMR 6457[Affiliation]) OR (UMR6457[Affiliation]) OR (CNRS 6457[Affiliation]) OR (laboratoire de physique subatomique et des technologies associées[Affiliation])"
    },
    {
        "collection": "US2B", "scopus_id": "60276652", "openalex_id": "I4387154840",
        "pubmed_query": "((US2B[Affiliation]) NOT (bordeaux[Affiliation])) OR (UMR6286[Affiliation]) OR (UMR 6286[Affiliation]) OR (CNRS 6286[Affiliation]) OR ((UFIP[Affiliation]) NOT ((spain[Affiliation]) OR (España[Affiliation]))) OR (Biological Sciences and Biotechnologies unit[Affiliation]) OR (Unité en Sciences Biologiques et Biotechnologies[Affiliation]) OR (Fonctionnalité et Ingénierie des Protéines[Affiliation]) OR (Unit Function & Protein Engineering[Affiliation]) OR (Protein Engineering and Functionality Unit[Affiliation]) OR (Laboratoire de Biologie et Pathologie Végétales[Affiliation]) OR ((LBPV[Affiliation]) AND (nantes[Affiliation])) OR (Laboratory of Plant Biology and Pathology[Affiliation]) OR (EA 1157[Affiliation]) OR (EA1157[Affiliation])"
    },
    {
        "collection": "CR2TI", "scopus_id": "60105579", "openalex_id": "I4392021198",
        "pubmed_query": "((CRTI[Affiliation]) AND (Nantes[Affiliation])) OR (CRT2I[Affiliation]) OR (CR2TI[Affiliation]) OR ((UMR 1064[Affiliation]) AND (nantes[Affiliation])) OR ((UMR1064[Affiliation]) NOT (Sophia Antipolis[Affiliation])) OR (Unité Mixte de Recherche 1064[Affiliation]) OR ((ITUN[Affiliation]) AND (nantes[Affiliation])) OR (Institut de Transplantation Urologie Néphrologie[Affiliation]) OR (U 643[Affiliation]) OR (U643[Affiliation]) OR ((Department of Nephrology and Immunology[Affiliation]) AND (nantes[Affiliation])) OR (Centre de Recherche en Transplantation et Immunologie[Affiliation]) OR (Center for Research in Transplantation and Immunology[Affiliation]) OR (Center for Research in Transplantation and Translational Immunology[Affiliation]) OR (Institut de Transplantation et de Recherche en Transplantation Urologie Néphrologie[Affiliation]) OR (U1064[Affiliation]) AND (nantes[Affiliation]) OR (Centre de recherche translationnelle en transplantation et immunologie[Affiliation]) OR (INSERM CR1064[Affiliation]) OR (Institut National de la Santé et de la Recherche Médicale 1064[Affiliation]) OR (INSERM Unité Mixte de Recherche 1064[Affiliation]) OR (Inserm 1064[Affiliation])"
    },
    {
        "collection": "CRCI2NA", "scopus_id": "60117278", "openalex_id": "I4210092509",
        "pubmed_query": "(CRCI2NA[Affiliation]) OR (CRC2INA[Affiliation]) OR ((CRCINA[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR (UMR 1307[Affiliation]) OR (UMR1307[Affiliation]) OR (U1307[Affiliation]) OR (UMR 6075[Affiliation]) OR (UMR6075[Affiliation]) OR (ERL6075[Affiliation]) OR ((ERL6001[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR ((ERL 6001[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR ((UMR 1232[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR ((UMR1232[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR ((Inserm 1232[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR ((Nantes-Angers Cancer Research Center[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR ((CRCNA[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR (Centre de Recherche en Cancérologie et Immunologie Intégrée[Affiliation]) OR ((Centre de Recherche en Cancérologie et Immunologie[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR ((Center for Research in Cancerology and Immunology[Affiliation]) AND ((2022[dp]) OR (2023[dp])))"
    },
    {
        "collection": "IICIMED", "scopus_id": "60105522", "openalex_id": "I4387930219",
        "pubmed_query": "((IICIMED[Affiliation]) AND (nantes[Affiliation])) OR (UR 1155[Affiliation]) OR (UR1155[Affiliation]) OR (EA 1155[Affiliation]) OR (EA1155[Affiliation]) OR (Cibles et Médicaments des Infections et du Cancer[Affiliation]) OR (Cibles et médicaments des infections et de l'immunité[Affiliation]) OR (Cibles et Médicaments des Infections de l'Immunité et du Cancer[Affiliation]) OR (cibles et medicaments des infections et du l immunite[Affiliation])"
    },
    {
        "collection": "INCIT", "scopus_id": "60276656", "openalex_id": "I4392021193",
        "pubmed_query": "((INCIT[Affiliation]) AND ((nantes[Affiliation]) OR (angers[Affiliation]))) OR ((UMR 1302[Affiliation]) AND (nantes[Affiliation])) OR ((UMR1302[Affiliation]) AND (nantes[Affiliation])) OR (EMR6001[Affiliation]) OR (Immunology and New Concepts in ImmunoTherapy[Affiliation])"
    },
    {
        "collection": "ISOMER", "scopus_id": "60105535", "openalex_id": "I4392021232",
        "pubmed_query": "((ISOMER[Affiliation]) AND (nantes[Affiliation])) OR ((MMS[Affiliation]) AND ((nantes[Affiliation]) OR (st nazaire[Affiliation]) OR (angers[Affiliation]) OR (le mans[Affiliation]))) OR (UR 2160[Affiliation]) OR (UR2160[Affiliation]) OR (EA 2160[Affiliation]) OR (EA2160[Affiliation]) OR (MMS EA 2160[Affiliation]) OR ((MicroMar[Affiliation]) AND (france[Affiliation])) OR (Institut des Substances et Organismes de la Mer[Affiliation]) OR (Mer Molécules Santé[Affiliation]) OR (Sea Molecules Health[Affiliation])"
    },
    {
        "collection": "MIP", "scopus_id": "60105638", "openalex_id": "I4392021216",
        "pubmed_query": "((MIP[Affiliation]) AND ((mans[Affiliation]) OR (nantes[Affiliation]))) OR (EA 4334[Affiliation]) OR (EA4334[Affiliation]) OR (UR 4334[Affiliation]) OR (UR4334[Affiliation]) OR (Movement Interactions Performance[Affiliation]) OR (Motricité Interactions Performance[Affiliation]) OR (mouvement interactions performance[Affiliation])"
    },
    {
        "collection": "PHAN", "scopus_id": "60105574", "openalex_id": "I4210162532",
        "pubmed_query": "((PHAN[Affiliation]) AND (nantes[Affiliation]) AND (1280[Affiliation])) OR (UMR 1280[Affiliation]) OR (UMR1280[Affiliation]) OR (Physiologie des Adaptations Nutritionnelles[Affiliation]) OR (Unité Mixte de Recherche 1280[Affiliation]) OR (Physiology of Nutritional Adaptations[Affiliation]) OR (Physiopathologie des Adaptations Nutritionnelles[Affiliation]) OR (Physiopathology of Nutritional Adaptations[Affiliation])"
    },
    {
        "collection": "RMES", "scopus_id": "60117279", "openalex_id": "I4387152865",
        "pubmed_query": "((RMES[Affiliation]) AND (nantes[Affiliation])) OR (UMRS 1229[Affiliation]) OR (UMR S 1229[Affiliation]) OR ((UMR 1229[Affiliation]) AND (nantes[Affiliation])) OR ((UMR1229[Affiliation]) AND (nantes[Affiliation])) OR (U 1229[Affiliation]) OR ((U1229[Affiliation]) AND (nantes[Affiliation])) OR (LIOAD[Affiliation]) OR (UMRS791[Affiliation]) OR (UMRS 791[Affiliation]) OR (UMR S 791[Affiliation]) OR (U 791[Affiliation]) OR (U791[Affiliation]) OR ((UMR 791[Affiliation]) AND (nantes[Affiliation])) OR ((UMR791[Affiliation]) AND (nantes[Affiliation])) OR (Regenerative Medicine and Skeleton[Affiliation]) OR (Osteoarticular and Dental Tissue Engineering[Affiliation]) OR (Laboratoire d'Ingénierie Ostéo-Articulaire et Dentaire[Affiliation]) OR (Ingénierie des Tissus Ostéo-Articulaires et Dentaires[Affiliation]) OR (osteo-articular and dental tissue engineering[Affiliation])"
    },
    {
        "collection": "SPHERE", "scopus_id": "60117638", "openalex_id": "I4392021239",
        "pubmed_query": "((SPHERE[Affiliation]) AND (1246[Affiliation])) OR (U 1246[Affiliation]) OR (U1246[Affiliation]) OR (UMR 1246[Affiliation]) OR (UMR1246[Affiliation]) OR (UMR S 1246[Affiliation]) OR (INSERM 1246[Affiliation]) OR (MethodS in Patients-centered outcomes and HEalth Research[Affiliation])"
    },
    {
        "collection": "TARGET", "scopus_id": "60105668", "openalex_id": "I4392021141",
        "pubmed_query": "((TARGET[Affiliation]) AND ((nantes université[Affiliation]) OR (nantes university[Affiliation]))) OR ((U1089[Affiliation]) AND (nantes[Affiliation])) OR (UMR S 1089[Affiliation]) OR ((UMR 1089[Affiliation]) AND (nantes[Affiliation])) OR ((UMR1089[Affiliation]) AND (nantes[Affiliation])) OR ((Laboratoire de Thérapie Génique[Affiliation]) AND (nantes[Affiliation])) OR (thérapie génique translationnelle des maladies génétiques[Affiliation]) OR (Translational Gene Therapy for Genetic Diseases[Affiliation]) OR (Gene Therapy Laboratory[Affiliation])"
    },
    {
        "collection": "TENS", "scopus_id": "60105652", "openalex_id": "I4210108033",
        "pubmed_query": "((TENS[Affiliation]) AND (nantes[Affiliation])) OR ((UMR 1235[Affiliation]) AND (nantes[Affiliation])) OR ((UMR1235[Affiliation]) AND (nantes[Affiliation])) OR ((U 1235[Affiliation]) AND (nantes[Affiliation])) OR ((U1235[Affiliation]) AND (nantes[Affiliation])) OR (U 913[Affiliation]) OR (U913[Affiliation]) OR (UMR 913[Affiliation]) OR (UMR913[Affiliation]) OR (UMR S 913[Affiliation]) OR (The Enteric Nervous System in Gut and Brain Diseases[Affiliation]) OR (neuropathies du système nerveux entérique[Affiliation])"
    },
    {
        "collection": "THORAX-UMR", "scopus_id": "60105651", "openalex_id": "I4210144168",
        "pubmed_query": "(Umr1087[Affiliation]) OR (Umr 1087[Affiliation]) OR (UMR S 1087[Affiliation]) OR (UMR 6291[Affiliation]) OR (UMRS1087[Affiliation]) OR (UMR6291[Affiliation]) OR ((Inst Thorax[Affiliation])) AND (nantes[Affiliation]) OR (l'institut du thorax[Affiliation]) OR (Institut du Thorax[Affiliation])"
    },
    {"collection": "CDMO", "scopus_id": "60105527", "openalex_id": "I4392021194", "pubmed_query": ""},
    {"collection": "CENS", "scopus_id": "60105489", "openalex_id": "I4210153136", "pubmed_query": ""},
    {"collection": "DCS", "scopus_id": "60105572", "openalex_id": "I4210100746", "pubmed_query": ""},
    {"collection": "IRDP", "scopus_id": "60105528", "openalex_id": "I4392021099", "pubmed_query": ""},
    {
        "collection": "LEMNA", "scopus_id": "60105575", "openalex_id": "I4390039323",
        "pubmed_query": "(LEMNA[Affiliation]) and (nantes[Affiliation])"
    },
    {
        "collection": "LHEEA", "scopus_id": "60105605", "openalex_id": "I4210153154",
        "pubmed_query": "(LHEEA[Affiliation]) OR (UMR 6598[Affiliation]) OR (UMR6598[Affiliation]) OR (CNRS 6598[Affiliation]) OR (Research Laboratory in Hydrodynamics, Energetics & Atmospheric Environment [Affiliation]) OR (Laboratoire de recherche en hydrodynamique[Affiliation])"
    },
    {"collection": "AAU", "scopus_id": "60110513", "openalex_id": "I4210162214", "pubmed_query": ""}
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
    st.sidebar.markdown("🔗 [Alignez une liste de chercheurs avec IdRef et HAL](https://c2labhal-auteurs.streamlit.app/)")


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
