
# c2LabHAL

## Description

[c2LabHAL](https://c2labhal.streamlit.app/) est une application conçue pour comparer les publications d'un laboratoire dans différentes bases de données (Scopus, OpenAlex, PubMed) avec sa collection HAL. Cette application permet aux utilisateurs de vérifier la présence des publications scientifiques dans plusieurs sources et de faciliter le repérage de celles qui n'ont pas encore été déposées dans la collection HAL du labo. 

[Voir une présentation de ce projet](https://slides.com/guillaumegodet/deck-d5bc03)

## Fonctionnalités

- **Récupération des données** : Import des principales métadonnées des publications depuis Scopus, OpenAlex et PubMed.
- **Fusion des lignes en double** : les publications en double sont dédoublonnées via leur DOI.
- **Comparaison avec HAL** : Comparez les publications récupérées avec la collection HAL du laboratoire. Réutilise le code du [hal_collection_checker](https://gitlab.com/hbretel/hal_collection_checker) d'Henri Bretel.
- **Récupération de métadonnées sur Unpaywall** et l'API permissions d'OA.works pour savoir quelle version de la publication peut être déposée.
- **Export des résultats** : Téléchargez les résultats sous la forme d'un fichier CSV.

## Installation

Pour exécuter cette application localement, suivez les étapes ci-dessous :

1. **Cloner le dépôt** :
   ```
   git clone https://github.com/votre-utilisateur/c2LabHAL.git
   cd c2LabHAL 
   ```
2. **Créer un environnement virtuel (optionnel mais recommandé)** :
   ```
   python -m venv venv
   source venv/bin/activate  # Sur Windows, utilisez `venv\Scripts\activate` 
   ```
3. **Installer les dépendances** :
```
pip install -r requirements.txt
```
4. **Exécuter l'application** :
 ```
 streamlit run app.py
 ```

## Configuration

Avant d'utiliser l'application, assurez-vous de disposer des informations suivantes :
 1. le nom de la **collection HAL** du laboratoire
 2. **Identifiant OpenAlex** du laboratoire : L'identifiant de l'institution sur OpenAlex
 3. **Requête PubMed** : Une requête PubMed qui rassemble au mieux les publications du laboratoire sans faire trop de bruit. 
Et si votre établissement est abonné à Scopus :
 4. **Clé API Scopus** () : Obtenez une clé API sur le [site d'Elsevier](https://dev.elsevier.com/)
 5. **Identifiant Scopus**  du laboratoire: L'identifiant du laboratoire sur Scopus (AF-ID).
    

## Utilisation

 - Saisir les paramètres : Entrez les informations nécessaires dans les champs de saisie.
 - Lancer la recherche : Cliquez sur le bouton "Rechercher" pour démarrer le processus de récupération et de comparaison des données.
 - Télécharger les résultats : Une fois le traitement terminé, téléchargez le fichier CSV contenant les résultats.

## Contributions

Les contributions sont les bienvenues ! Si vous souhaitez contribuer à ce projet, veuillez suivre les étapes suivantes :

1.  Forkez le dépôt.
2.  Créez une branche pour votre fonctionnalité (`git checkout -b feature/ma-fonctionnalite`).
3.  Commitez vos modifications (`git commit -m 'Ajout de ma fonctionnalité'`).
4.  Poussez vers la branche (`git push origin feature/ma-fonctionnalite`).
5.  Ouvrez une Pull Request.

## Licence

Ce projet est sous licence MIT. Voir le fichier [LICENSE](LICENSE.md) pour plus de détails



