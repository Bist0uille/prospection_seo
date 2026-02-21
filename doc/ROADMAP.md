# ROADMAP — Refactoring pipeline botparser

## Vision générale

Refactoriser le pipeline de prospection SEO pour le rendre production-ready :
- CLI moderne via **click** (argparse → click)
- Validation et sanitisation des inputs via **Pydantic v2**
- **Logging** structuré, multi-niveau, dual output (stdout + fichier)
- **Tests unitaires** complets avec pytest
- Architecture claire : `Scripts/core/` pour l'infrastructure partagée

---

## Étapes

### ✅ Étape 1 — Infrastructure core
**Branche :** `refactor/click-pydantic-logging`
**Status :** Complété

- [x] Créer `Scripts/core/__init__.py`
- [x] Créer `Scripts/core/logging_config.py`
  - `get_logger(name)` — logger enfant sous `botparser.*`
  - `setup_pipeline_logging(log_dir, sector_name, level)` — config root logger
  - `reset_logging()` — nettoyage pour les tests
- [x] Créer `Scripts/core/models.py`
  - `ApeCodeList` — liste de codes APE validée et dédupliquée
  - `PipelineConfig` — config pipeline complète (sector/codes, min_employees, etc.)
  - `FindWebsitesConfig` — config pour find_websites.py
  - `SeoAuditConfig` — config pour seo_auditor.py standalone
- [x] Mettre à jour `requirements.txt` : `click>=8.1`, `pydantic>=2.0`, `pytest>=7.0`, `pytest-mock>=3.0`

---

### ✅ Étape 2 — Refactoring `run_full_pipeline.py`
**Status :** Complété

- [x] Remplacer `argparse` par `click`
- [x] Valider tous les inputs via `PipelineConfig`
- [x] Remplacer tous les `print()` par `logger.info/debug/warning/error`
- [x] Initialiser le logging via `setup_pipeline_logging()` après résolution du secteur
- [x] Conserver exactement la logique pipeline (5 étapes)
- [x] Mettre à jour l'appel subprocess : `--output-dir` (tiret, convention click)

---

### ✅ Étape 3 — Refactoring `find_websites.py`
**Status :** Complété

- [x] Remplacer `argparse` par `click`
- [x] Supprimer le logging module-level (setuplogging dans `main()`)
- [x] Valider inputs via `FindWebsitesConfig`
- [x] Extraire la logique métier dans `process_companies(config)`
- [x] Remplacer `logging.warning/info/error` par le logger centralisé

---

### ✅ Étape 4 — Refactoring `prospect_analyzer.py`
**Status :** Complété

- [x] Ajouter `logger = get_logger(__name__)`
- [x] Remplacer tous les `print()` par `logger.info/debug/warning/error`
- [x] Ajouter logging DEBUG pour les entrées/sorties de fonctions
- [x] Documenter avec des docstrings complètes

---

### ✅ Étape 5 — Refactoring `seo_auditor.py`
**Status :** Complété

- [x] Remplacer le bloc `argparse` par `click` (CLI standalone)
- [x] Ajouter `logger = get_logger(__name__)`
- [x] Remplacer tous les `print()` par logger
- [x] Ajouter logging par page crawlée (DEBUG), par site (INFO)
- [x] Valider inputs standalone via `SeoAuditConfig`

---

### ✅ Étape 6 — Tests unitaires pytest
**Status :** Complété

- [x] `conftest.py` (racine) — sys.path
- [x] `tests/conftest.py` — fixtures partagées
  - `sector_file`, `empty_sector_file`, `dummy_db_csv`
  - `websites_df`, `verified_df`
  - `_reset_logging` (autouse)
- [x] `tests/test_models.py` — Pydantic validation
  - ApeCodeList : format, déduplication, CSV parsing, erreurs
  - PipelineConfig : defaults, sanitisation nom, existence fichiers, mutual exclusion
  - FindWebsitesConfig, SeoAuditConfig
- [x] `tests/test_pipeline_helpers.py` — helpers run_full_pipeline
  - `get_employee_codes` : seuils INSEE
  - `load_ape_codes` : parsing, commentaires, erreurs
- [x] `tests/test_prospect_analyzer.py` — logique métier
  - `normalize_name`, `get_domain`, `extract_keywords`
  - `filter_companies_by_employees` : filtres actifs/effectifs/NAF/SIREN
  - `verify_websites_by_domain` : blocklist, /en/, .ca, matching
  - `create_prospect_scoring_v2` : scores, clamp 1–10, tri, colonnes
- [x] `tests/test_seo_auditor.py` — helpers SEO (sans réseau)
  - `_detect_cms` : toutes les signatures
  - `_parse_date`, `_compute_publication_frequency`
  - `_extract_dates` : time tags, schema.org, URL
  - `_extract_text_words`, `_get_internal_links`, `_detect_rss`

---

### ✅ Étape 7 — Makefile + doc
**Status :** Complété

- [x] Ajouter `make test` — lancer pytest
- [x] Ajouter `make test-verbose` — pytest -v
- [x] Ajouter `make test-cov` — pytest + coverage
- [x] Ajouter `make install-dev` — installe les dépendances de dev (pytest, pytest-mock)
- [x] Créer `doc/ROADMAP.md`

---

### ✅ Étape 8 — PostgreSQL + Docker Compose
**Status :** Complété

**Objectif :** Permettre d'utiliser une base PostgreSQL locale à la place du CSV INSEE pour l'étape 1 (filtrage).

- [x] `docker-compose.yml` — postgres:16-alpine, port 5432, healthcheck, volume nommé
- [x] `db/schema.sql` — table `unites_legales` (61 colonnes TEXT + 4 index) montée automatiquement au démarrage
- [x] `db/connection.py` — `get_dsn()`, `get_engine()`, `check_connection()` (SQLAlchemy 2.0)
- [x] `db/importer.py` — `import_csv()` streaming par chunks, normalisation NAF, gestion doublons SIREN ; CLI click
- [x] `Scripts/core/models.py` — champ `pg_dsn: Optional[str]` dans `PipelineConfig`, validé (doit commencer par `postgresql://`)
- [x] `Scripts/prospect_analyzer.py` — `filter_companies_by_employees_pg(engine, output_path, naf_codes, employee_codes)` — requête SQL paramétrée, résultat → CSV
- [x] `Scripts/run_full_pipeline.py` — option `--pg-dsn` (ou `$BOTPARSER_PG_DSN`) ; step [1/5] branche sur PG si fourni, CSV sinon
- [x] `Makefile` — targets `docker-up`, `docker-down`, `import-db`, `pg-shell`, `run-pg`
- [x] `requirements.txt` — `sqlalchemy>=2.0`, `psycopg2-binary>=2.9`

**Usage rapide :**
```bash
make docker-up          # démarre postgres
make import-db          # importe le CSV (par défaut: DataBase/annuaire-des-entreprises-nautisme.csv)
make run-pg SECTOR=nautisme  # pipeline via PG
```

---

### ✅ Étape 9 — Migration Selenium → ddgs + corrections pipeline
**Status :** Complété

**Objectif :** Supprimer la dépendance au navigateur headless pour l'étape 2, corriger l'auto-détection de la base de données, et améliorer la qualité des URLs trouvées.

- [x] `Scripts/find_websites.py` — remplace Selenium par `ddgs` (API DuckDuckGo, sans navigateur)
  - `get_website_with_ddgs()` : recherche `{nom} fr`, fallback `{nom} nautisme` si aucun match
  - `_strip_to_root()` : normalise toute URL vers sa racine (`scheme://domain/`) — les chemins `/en/` ne sont plus rejetés, on prend la racine
  - `_is_canadian()` : seul filtre d'origine conservé (`.ca`)
  - `_pick_best_candidate()` : matching keyword sur le domaine racine
- [x] `Scripts/run_full_pipeline.py` — `find_default_database(sector_name)` : priorité 1 = CSV dont le nom contient le secteur (ex. `nautisme`), évite de prendre un CSV générique ne contenant pas les bons codes NAF
- [x] `Scripts/botparser_log.py` — supprimé (legacy Selenium + chemin Windows hardcodé)
- [x] `requirements.txt` — remplace `selenium>=4.10` + `webdriver-manager>=4.0` par `ddgs>=9.0`
- [x] `README.md` — mise à jour complète (installation, étape 2, structure)

**Bugs corrigés :**
- Auto-détection prenait `etablissements-juridique.csv` (0 entreprises NAF nautisme) au lieu de `nautisme.csv`
- Import Selenium `from selenium.webdriver.by import By` → `selenium.webdriver.common.by`
- URLs avec `/en/` dans le chemin rejetées entièrement au lieu d'être normalisées à la racine

---

## Architecture finale

```
botparser_NAUTISME/
├── Scripts/
│   ├── core/
│   │   ├── __init__.py
│   │   ├── logging_config.py   # get_logger, setup_pipeline_logging, reset_logging
│   │   └── models.py           # PipelineConfig, ApeCodeList, FindWebsitesConfig, SeoAuditConfig
│   ├── run_full_pipeline.py    # click CLI, orchestrateur 5 étapes
│   ├── find_websites.py        # click CLI, ddgs / DuckDuckGo API
│   ├── prospect_analyzer.py    # library : filter (CSV+PG), verify, scoring
│   └── seo_auditor.py          # click CLI + library : BFS crawl
├── db/
│   ├── __init__.py
│   ├── connection.py           # get_engine, get_dsn, check_connection
│   ├── importer.py             # import_csv() + click CLI
│   └── schema.sql              # CREATE TABLE unites_legales (61 cols TEXT)
├── tests/
│   ├── conftest.py             # fixtures partagées
│   ├── test_models.py
│   ├── test_pipeline_helpers.py
│   ├── test_prospect_analyzer.py
│   └── test_seo_auditor.py
├── Sectors/                    # .txt files (1 par secteur)
├── DataBase/                   # CSV INSEE source
├── Results/                    # sorties par secteur
├── Logs/                       # logs horodatés par run
├── doc/
│   └── ROADMAP.md
├── conftest.py                 # sys.path racine
├── docker-compose.yml          # postgres:16-alpine
├── Makefile
└── requirements.txt
```

## Décisions techniques

| Décision | Justification |
|---|---|
| click au lieu de argparse | API déclarative, meilleur help auto, tests plus simples (CliRunner) |
| Pydantic v2 | Validation rapide, messages d'erreur clairs, coercition auto des types |
| Logger hiérarchique `botparser.*` | Héritage des handlers depuis la racine, isolation par module |
| `setup_pipeline_logging()` dans `main()` seulement | Évite le logging au niveau module (mauvaise pratique Python) |
| `reset_logging()` dans les tests | Évite l'accumulation de handlers entre tests |
| Subprocess `--output-dir` (tiret) | Convention click standard ; argparse acceptait l'underscore |
| Tests sans réseau | Mock ddgs et requests.get pour des tests rapides et déterministes |
| PostgreSQL optionnel (step 1 seulement) | Les étapes 2–5 restent en DataFrames mémoire ; PG remplace uniquement la lecture CSV initiale |
| `filter_companies_by_employees_pg` dans `prospect_analyzer.py` | Symétrique avec la version CSV ; même signature de sortie (CSV) pour le reste du pipeline |
| Lazy import sqlalchemy dans `filter_companies_by_employees_pg` | Le module fonctionne sans sqlalchemy si la path CSV est utilisée |

## Commandes de validation manuelle

Avant chaque commit, valider manuellement :

```bash
# 1. Tests unitaires
make test

# 2. Pipeline dry-run (limit=5, skip-audit, CSV)
make dry-run SECTOR=nautisme

# 3. Vérifier le help click
python Scripts/run_full_pipeline.py --help
python Scripts/find_websites.py --help
python Scripts/seo_auditor.py --help

# 4. Test PostgreSQL (optionnel — nécessite Docker)
make docker-up
make import-db
make run-pg SECTOR=nautisme
make docker-down
```
