# Prospection SEO Nautisme

Outil de prospection commerciale automatisé ciblant le secteur nautique français. Le pipeline identifie les entreprises du secteur, trouve leurs sites web, audite leur qualité via Google Lighthouse, et génère un rapport de scoring : plus le site est mal optimisé, plus l'entreprise est un prospect intéressant pour des services web.

## Pipeline

Le pipeline complet s'exécute en **5 étapes** via un seul script :

```bash
python Scripts/run_full_pipeline.py
```

| Étape | Description | Détail |
|-------|-------------|--------|
| 1 | **Filtrage** | Filtre la base INSEE par codes NAF nautisme (10 codes), tranche d'effectifs (10+ salariés), statut actif, et déduplique par SIREN |
| 2 | **Recherche de sites** | Recherche DuckDuckGo automatisée via Selenium headless, validation par correspondance nom/domaine, exclusion des annuaires |
| 3 | **Vérification** | Vérifie chaque site trouvé par matching domaine/nom d'entreprise, avec blocklist de faux positifs |
| 4 | **Audit Lighthouse** | Exécute `npx lighthouse` sur chaque site vérifié, génère un rapport JSON par entreprise |
| 5 | **Scoring** | Calcule un score de prospect (1-10) à partir des scores Lighthouse, trie par potentiel décroissant |

### Options CLI

```
--no-fresh            Ne pas nettoyer les anciens résultats avant exécution
--limit N             Limiter le nombre d'entreprises (pour les tests)
--skip-lighthouse     Passer l'étape Lighthouse
--keep-intermediates  Conserver les fichiers intermédiaires
```

## Codes NAF ciblés

| Code | Activité |
|------|----------|
| 3012Z | Construction de bateaux de plaisance |
| 3011Z | Construction de navires et structures flottantes |
| 3315Z | Réparation et maintenance navale |
| 5010Z | Transports maritimes et côtiers de passagers |
| 5020Z | Transports maritimes et côtiers de fret |
| 5222Z | Services auxiliaires des transports par eau |
| 7734Z | Location de matériels de transport par eau |
| 7721Z | Location d'articles de loisirs (bateaux plaisance) |
| 4764Z | Commerce de détail d'articles de sport (accastillage) |
| 9329Z | Activités récréatives (marinas) |

Les codes et tranches d'effectifs sont configurables en tête de `Scripts/run_full_pipeline.py`.

## Formule de scoring

```
prospect_score = ((1 - seo) * 1.5 + (1 - perf) * 1.2 + (1 - acc) * 0.8) / 3.5 * 10
```

- **Score 8-10** : site de mauvaise qualité = excellent prospect
- **Score 4-7** : prospect modéré, améliorations possibles
- **Score 1-3** : site bien optimisé = prospect faible

## Structure du projet

```
├── Scripts/
│   ├── run_full_pipeline.py      # Pipeline principal (point d'entrée)
│   ├── prospect_analyzer.py      # Filtrage, vérification, Lighthouse, scoring
│   └── find_websites.py          # Recherche de sites via Selenium/DuckDuckGo
│
├── DataBase/
│   └── annuaire-des-entreprises-nouvelle_aquitaine.csv  # Base source
│
├── Results/
│   └── final_prospect_report.csv  # Rapport final (seul fichier conservé)
│
├── Reports/Lighthouse/
│   └── {SIREN}_report.json        # Rapports Lighthouse individuels
│
├── code_ape_nautisme.txt          # Référence des codes NAF nautisme
├── requirements.txt
└── .gitignore
```

Les fichiers intermédiaires (`filtered_companies.csv`, `*_websites.csv`, `verified_websites.csv`, `lighthouse_reports.csv`) sont créés pendant l'exécution et supprimés automatiquement à la fin.

## Installation

```bash
# Cloner le repo
git clone https://github.com/Bist0uille/prospection_seo.git
cd prospection_seo

# Créer un environnement virtuel et installer les dépendances
python -m venv .venv
source .venv/bin/activate  # Linux/WSL
# ou .venv\Scripts\activate  # Windows
pip install -r requirements.txt

# Installer Lighthouse (nécessite Node.js)
npm install -g lighthouse
```

Chrome ou Chromium doit être installé. Sur WSL sans Chrome Linux, le script détecte et utilise automatiquement [Chrome for Testing](https://googlechromelabs.github.io/chrome-for-testing/) s'il est présent dans `~/.chrome-for-testing/`.

## Sortie

Le rapport final `Results/final_prospect_report.csv` contient :

| Colonne | Description |
|---------|-------------|
| `siren` | Identifiant SIREN |
| `denominationUniteLegale` | Nom de l'entreprise |
| `trancheEffectifsUniteLegale` | Code tranche d'effectifs |
| `site_web` | URL du site trouvé |
| `prospect_score` | Score de 1 à 10 (plus = meilleur prospect) |
| `performance` | Score Lighthouse performance (0-100) |
| `seo` | Score Lighthouse SEO (0-100) |
| `accessibilite` | Score Lighthouse accessibilité (0-100) |
| `bonnes_pratiques` | Score Lighthouse bonnes pratiques (0-100) |
| `prospect_summary` | Résumé textuel du potentiel |

## Considérations légales

La collecte automatisée de données doit être effectuée dans le respect du RGPD et des conditions d'utilisation des sites web. Cet outil est fourni à des fins d'analyse commerciale.
