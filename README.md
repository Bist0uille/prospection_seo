# Prospection SEO — Outil multi-secteur

Outil de prospection commerciale automatisé pour **agences web françaises**. Le pipeline identifie les entreprises d'un secteur donné, trouve leurs sites web, audite leur qualité par crawl léger, et génère un rapport de scoring : plus le site est mal optimisé / abandonné, plus l'entreprise est un prospect prioritaire.

## Pipeline

```bash
# Par fichier secteur
python Scripts/run_full_pipeline.py --sector Sectors/nautisme.txt

# Par codes NAF directs
python Scripts/run_full_pipeline.py --codes 3012Z,3011Z,3315Z --name nautisme

# Avec limites
python Scripts/run_full_pipeline.py --sector Sectors/restaurants.txt --limit 50 --min-employees 1
```

| Étape | Script | Description |
|-------|--------|-------------|
| 1 | `prospect_analyzer.py` | Filtre la base INSEE par codes NAF, tranche d'effectifs, statut actif, déduplique par SIREN |
| 2 | `find_websites.py` | Recherche DuckDuckGo via API `ddgs` (sans navigateur), normalisation domaine, exclusion annuaires, fallback "nautisme" |
| 3 | `prospect_analyzer.py` | Vérifie chaque site (matching domaine, blocklist, filtre sites non-français) |
| 4 | `seo_auditor.py` | Crawl BFS léger (max 30 pages), extraction signaux SEO business |
| 5 | `prospect_analyzer.py` | Scoring d'opportunité business (1–10), rapport CSV final |

Les résultats sont isolés par secteur dans `Results/{secteur}/`.

### Options CLI

```
--sector FILE         Fichier .txt de codes APE (voir Sectors/)
--codes A,B,C         Codes NAF directs (ex. 3012Z,3011Z)
--name NOM            Nom du secteur pour le dossier de résultats
--limit N             Limiter le nombre d'entreprises (tests)
--min-employees N     Effectif minimum (défaut : 10)
```

## Secteurs disponibles

Les secteurs sont définis dans `Sectors/` — un fichier `.txt` par secteur, un code APE par ligne :

```
3012Z - Construction de bateaux de plaisance
3011Z - Construction de navires et structures flottantes
# les commentaires sont ignorés
```

Secteurs inclus : `nautisme`, `architectes`, `immobilier`, `restaurants`.
Copier `Sectors/template.txt` pour créer un nouveau secteur.

## Validation des sites (étape 2)

La recherche DDG retourne l'URL racine du domaine (chemins ignorés) et applique les filtres suivants :
- **Blocklist d'annuaires** : societe.com, pappers.fr, pagesjaunes.fr, linkedin.com, etc. automatiquement exclus
- **TLD `.ca`** rejeté (domaines canadiens)
- **Matching keyword** : le domaine doit contenir au moins un mot-clé significatif (≥ 4 caractères) issu du nom de l'entreprise
- **Préférence `.fr`** : en cas de plusieurs candidats valides, les domaines `.fr` sont prioritaires
- **Fallback "nautisme"** : si aucun résultat pertinent, la recherche est relancée avec le mot-clé secteur

## Audit SEO (seo_auditor.py)

Crawl BFS léger — rapide et stable, sans navigateur. Signaux extraits par site :

| Signal | Description |
|--------|-------------|
| `nb_pages` | Nombre de pages crawlées |
| `has_sitemap` | Présence de sitemap.xml |
| `has_blog` | Blog détecté (URL patterns → liens → nav vérifiée) |
| `blog_status` | `actif` / `semi-actif` / `abandonné` / `présent` / `absent` |
| `derniere_maj_blog` | Date du dernier article détecté |
| `frequence_publication` | `hebdomadaire` / `mensuelle` / `trimestrielle` / `rare` |
| `activite_status` | Basé sur les dates blog (fiable) ou all_dates (plafonné à semi-actif) |
| `cms_detecte` | WordPress, Wix, Shopify, Squarespace, Webflow, Joomla, Drupal |
| `mots_moyen_par_page` | Densité de contenu |
| `ratio_texte_html` | Ratio texte visible / HTML brut |
| `titles_dupliques` | Ratio 0.0–1.0 de titles en doublon |
| `pages_sans_meta_desc` | Nombre de pages sans meta description |
| `pages_sans_h1` | Nombre de pages sans H1 |
| `pages_vides` | Pages < 50 mots (hors /contact, /cgv, /mentions-legales…) |

> Un site sans blog ne peut jamais être déclaré "actif" — les dates globales (footers, CGU) ne sont pas fiables pour juger l'activité réelle.

## Scoring d'opportunité business

Score de 1 à 10 mesurant la **probabilité de deal**, pas la qualité SEO académique.

### Signaux positifs

| Signal | Points |
|--------|--------|
| Blog abandonné | +5 |
| Blog semi-actif | +2 |
| Pas de blog (site vitrine souvent obsolète) | +1 |
| nb_pages < 5 | +3 |
| nb_pages 5–9 | +1 |
| mots_moyen_par_page < 150 | +2 |
| ratio_texte_html < 0.15 | +2 |
| CMS non détecté (site bricolé) | +2 |
| CMS Wix ou Squarespace | +1 |
| Pas de sitemap | +1 |
| Pages sans meta desc (par tranche de 20 %) | +0.5 |
| Pages sans H1 (par tranche de 20 %) | +0.5 |
| Titles dupliqués > 30 % | +0.5 |
| Pages vides (par tranche de 20 %) | +0.5 |

### Signaux négatifs

| Signal | Points |
|--------|--------|
| Blog actif + publication hebdo/mensuelle | −4 |
| nb_pages > 50 | −3 |
| mots_moyen_par_page > 400 | −2 |

## Structure du projet

```
├── Scripts/
│   ├── run_full_pipeline.py      # Point d'entrée — pipeline multi-secteur
│   ├── find_websites.py          # Recherche sites web via API ddgs / DuckDuckGo
│   ├── seo_auditor.py            # Audit SEO par crawl BFS léger
│   ├── prospect_analyzer.py      # Filtrage, vérification, scoring
│   └── core/                     # Infrastructure partagée (logging, modèles Pydantic)
│
├── Sectors/
│   ├── nautisme.txt              # Codes APE secteur nautisme
│   ├── architectes.txt
│   ├── immobilier.txt
│   ├── restaurants.txt
│   └── template.txt              # Modèle pour un nouveau secteur
│
├── DataBase/
│   ├── annuaire-des-entreprises-nautisme.csv
│   └── annuaire-des-entreprises-etablissements-juridique.csv
│
├── Results/
│   └── {secteur}/
│       └── final_prospect_report.csv   # Rapport final par secteur
│
├── requirements.txt
└── .gitignore
```

## Installation

```bash
git clone https://github.com/Bist0uille/prospection_seo.git
cd prospection_seo
make install        # crée le venv WSL et installe les dépendances
```

Ou manuellement :

```bash
python3 -m venv .venv_wsl
source .venv_wsl/bin/activate
pip install -r requirements.txt
```

> Aucun navigateur requis — la recherche de sites (étape 2) utilise l'API `ddgs` directement.

## Rapport final

`Results/{secteur}/final_prospect_report.csv` :

| Colonne | Description |
|---------|-------------|
| `entreprise` | Nom de l'entreprise |
| `site_web` | URL du site |
| `score` | Score d'opportunité 1–10 |
| `cms` | CMS détecté |
| `nb_pages` | Pages crawlées |
| `blog` | Présence blog |
| `blog_url` | URL du blog |
| `activite` | Statut d'activité |
| `derniere_maj_site` | Dernière date détectée |
| `sitemap` | Présence sitemap |
| `pages_sans_meta_desc` | Pages sans meta description |
| `pages_sans_h1` | Pages sans H1 |
| `mots_moy_page` | Mots moyens par page |
| `resume` | Résumé textuel des opportunités |

## Considérations légales

La collecte automatisée de données doit être effectuée dans le respect du RGPD et des conditions d'utilisation des sites web. Cet outil est fourni à des fins d'analyse commerciale.
