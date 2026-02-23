# CLAUDE.md — botparser_NAUTISME

Outil de prospection commerciale pour agences web. Identifie les entreprises d'un secteur avec un site web faible/absent et les classe par opportunité business.

## Commandes utiles

```bash
source .venv_wsl/bin/activate   # activer le venv (WSL)

# Pipeline complet
python Scripts/run_full_pipeline.py --sector Sectors/nautisme.txt

# Health checker standalone
python Scripts/site_health_checker.py \
  --departements 17,33,16,40,47,64 \
  --output Results/nautisme/site_health

# Codes NAF directs
python Scripts/run_full_pipeline.py --codes 3012Z,3011Z --name nautisme --limit 50
```

## Structure des scripts

| Script | Rôle |
|--------|------|
| `run_full_pipeline.py` | Point d'entrée — orchestre les étapes 1-5 |
| `find_websites.py` | Recherche sites via API ddgs (3 passes : fr, site officiel, nautisme) |
| `seo_auditor.py` | Crawl BFS léger, extraction signaux SEO |
| `prospect_analyzer.py` | Filtrage INSEE, matching domaine, scoring 1-10 |
| `contact_scraper.py` | Extraction emails + téléphones |
| `site_health_checker.py` | Vérif santé sites (standalone, ne pas modifier le pipeline) |

## Fichiers résultats nautisme

```
Results/nautisme/
├── filtered_companies.csv              # base SIRENE filtrée (étape 1)
├── filtered_companies_websites.csv     # + sites trouvés (étape 2)
├── final_prospect_report.csv           # rapport scoré final
├── site_health.csv                     # output health checker
└── site_health.html                    # rapport HTML filtrable
```

## site_health_checker.py — logique

Priorités de classification :
1. `pas_de_site` — aucun site trouvé (signal #1, plus fort)
2. `down` — site inaccessible (erreur HTTP, DNS, timeout)
3. `lent` — réponse > 3s
4. `site_ancien` — copyright > 2 ans
5. `sans_blog` — site up mais pas de blog
6. `ok` — pas d'opportunité évidente

Score flottant : `priorite_base + 0.5` si agence déjà en place (descend dans la catégorie).

Signaux additionnels : agence_detectee, annee_copyright, reseaux_sociaux (dict platform→URL).

`_AGENCY_FALSE_POSITIVES` : complianz, wordpress, woocommerce, cookiebot, axeptio, gdpr, divi, elementor, etc.

## find_websites.py — passes de recherche

1. `{denomination}` (lang=fr, max_results=10)
2. `{denomination} site officiel`
3. `{denomination} nautisme`

Matching : keyword ≥ 4 chars du nom d'entreprise doit apparaître dans le domaine. Préférence `.fr`. Blocklist annuaires (societe.com, pappers.fr, linkedin.com…).

## Conventions

- **Mettre à jour README.md avant chaque commit** si des scripts, options CLI, signaux ou comportements ont changé
- Ne pas modifier les scripts du pipeline principal pour ajouter des fonctionnalités standalone
- Résultats toujours dans `Results/{secteur}/`
- Logs dans `Logs/`
- Venv WSL : `.venv_wsl/` (ignoré git)
- Dépôt GitHub : `github.com/Bist0uille/prospection_seo`
