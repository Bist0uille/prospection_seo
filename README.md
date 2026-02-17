# BotParser NAUTISME

## Vue d'ensemble

**BotParser NAUTISME** est un outil de prospection commerciale automatisé ciblant l'industrie nautique française. Le système identifie, analyse et évalue les entreprises du secteur nautique ayant un potentiel commercial élevé pour des services de développement ou d'optimisation web.

## Objectif principal

L'outil génère automatiquement une liste de prospects commerciaux qualifiés en :

1. **Identifiant les entreprises nautiques** via les bases de données officielles (codes APE/NAF spécifiques)
2. **Découvrant leurs sites web** par recherche automatisée (Selenium + DuckDuckGo)
3. **Auditant la qualité des sites** avec Google Lighthouse (performance, SEO, accessibilité)
4. **Calculant un score de prospect** : plus le site est de mauvaise qualité, plus l'entreprise est un prospect intéressant
5. **Extrayant les emails de contact** pour faciliter la prise de contact

**Principe clé** : Une entreprise avec un site web mal optimisé = un client potentiel pour des services web.

## Technologies utilisées

- **Python 3** - Langage principal
- **Selenium 4** - Automatisation de navigateur web
- **BeautifulSoup4** - Extraction de données HTML
- **Pandas** - Manipulation de données
- **Google Lighthouse** - Audit de qualité web
- **DuckDuckGo** - Moteur de recherche pour la découverte de sites

## Structure du projet

```
botparser_NAUTISME/
├── DataBase/                         # Données sources
│   ├── StockUniteLegale_utf8.csv    # Base SIREN nationale (INSEE)
│   └── annuaire-des-entreprises-*.csv
│
├── Results/                          # Résultats de pipeline
│   ├── filtered_companies.csv
│   ├── filtered_companies_websites.csv
│   └── final_prospect_report.csv
│
├── Reports/Lighthouse/               # Rapports d'audit individuels
│   └── {SIREN}_report.json
│
└── Scripts/                          # Scripts de traitement
    ├── botparser.py                 # Orchestrateur principal
    ├── filter_companies.py          # Filtrage par code NAF
    ├── find_websites.py             # Découverte de sites web
    ├── analyze_websites.py          # Audit Lighthouse
    ├── prospect_analyzer.py         # Module de scoring
    └── find_emails.py               # Extraction d'emails
```

## Codes NAF ciblés (secteur nautique)

Les codes APE/NAF visés sont définis dans `code_ape_nautisme.txt` :

- **3012Z** - Construction de bateaux de plaisance
- **3011Z** - Construction de navires et de structures flottantes
- **3315Z** - Réparation et maintenance navale

## Pipeline de traitement

### Étape 1 : Filtrage des entreprises
```bash
python Scripts/filter_companies.py
```
- Lit la base SIREN nationale
- Filtre par codes NAF nautiques
- Exclut les entreprises inactives
- Filtre par taille (minimum 10+ employés)
- **Sortie** : `Results/filtered_companies.csv`

### Étape 2 : Découverte des sites web
```bash
python Scripts/find_websites.py
```
- Recherche DuckDuckGo automatisée via Selenium
- Validation du domaine par correspondance avec le nom d'entreprise
- Exclusion des annuaires (pappers.fr, societe.com, etc.)
- Support de reprise après interruption
- **Sortie** : `Results/filtered_companies_websites.csv`

### Étape 3 : Analyse et scoring
```bash
python Scripts/analyze_websites.py
```
- Vérification de propriété du site web
- Audit Google Lighthouse de chaque site
- Calcul du score de prospect (1-10)
- Tri par score décroissant
- **Sortie** : `Results/final_prospect_report.csv`

### Étape optionnelle : Extraction d'emails
```bash
python Scripts/find_emails.py
```
- Scraping HTML des pages de contact
- Priorisation des emails (domaine de l'entreprise > emails génériques)
- **Sortie** : `resultats_avec_emails.csv`

## Formule de scoring

Le score de prospect est calculé selon la formule :

```python
score = ((1 - seo) * 1.5 + (1 - perf) * 1.2 + (1 - acc) * 0.8) / 3.5 * 10
```

**Résultat** : Score de 1 à 10 où :
- **Score élevé** (8-10) = Site de mauvaise qualité = Meilleur prospect
- **Score faible** (1-3) = Site bien optimisé = Prospect moins intéressant

## Exécution rapide

### Pipeline complet
```bash
python Scripts/botparser.py --naf 301 --csv DataBase/StockUniteLegale_utf8.csv --employees 10
```

ou

```bash
python run_full_pipeline.py
```

### Ré-analyse des données existantes
```bash
python run_full_reanalysis.py
```

### Scoring uniquement
```bash
python run_scoring.py
```

## Configuration requise

1. Python 3.x avec les dépendances dans `.venv/`
2. Node.js (pour `npx lighthouse`)
3. ChromeDriver (fourni dans `chromedriver_win32/`)
4. Connexion Internet pour les recherches et audits

## Installation

```bash
# Activer l'environnement virtuel (Windows)
.venv\Scripts\activate

# Installer les dépendances
pip install selenium beautifulsoup4 pandas requests tqdm webdriver-manager python-dotenv

# Installer Lighthouse
npm install -g lighthouse
```

## Fichiers de données

- **`DataBase/StockUniteLegale_utf8.csv`** : Base SIREN nationale (~940 Mo décompressé)
- **`code_ape_nautisme.txt`** : Liste des codes NAF ciblés
- **`methodo.txt`** : Méthodologie détaillée en français

## Notes techniques

- Le script `find_websites.py` sauvegarde la progression après chaque entreprise
- Les rapports Lighthouse sont stockés individuellement dans `Reports/Lighthouse/`
- Le projet fonctionne sur Windows/WSL avec des chemins compatibles
- Certains scripts legacy (`get_all_data_multi_page.py`, `botparser_log.py`) proviennent d'une version antérieure ciblant les architectes

## Résultats

Le fichier final `Results/final_prospect_report.csv` contient :
- Informations entreprise (SIREN, nom, adresse)
- URL du site web
- Scores Lighthouse (SEO, Performance, Accessibilité, Best Practices)
- **Score de prospect** (trié par ordre décroissant)
- Statut de vérification du site

Les meilleurs prospects sont en haut de la liste.

## Considérations éthiques et légales

La collecte automatisée de données, en particulier d'informations de contact, doit être effectuée dans le respect des directives légales et éthiques (RGPD, conditions d'utilisation des sites web). Cet outil est fourni à des fins d'analyse et de collecte d'informations, et son utilisation doit être conforme à toutes les réglementations applicables.

## Auteur

Projet de prospection commerciale pour le secteur nautique français.
