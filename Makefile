# ==============================================================================
# Makefile — Pipeline de prospection SEO multi-secteur
# ==============================================================================

# ---- Python / venv -----------------------------------------------------------
PYTHON_LINUX  := .venv_wsl/bin/python
PYTHON_WIN    := .venv/bin/python
PYTHON        := $(shell \
  if [ -f $(PYTHON_LINUX) ]; then echo $(PYTHON_LINUX); \
  elif [ -f $(PYTHON_WIN) ]; then echo $(PYTHON_WIN); \
  else echo python3; fi)

PIP           := $(PYTHON) -m pip

# ---- Dossiers ----------------------------------------------------------------
SCRIPTS_DIR   := Scripts
SECTORS_DIR   := Sectors
RESULTS_DIR   := Results
DB_DIR        := DataBase

# ---- Secteur par défaut (override: make run SECTOR=architectes) --------------
SECTOR        := nautisme

# ---- PostgreSQL ---------------------------------------------------------------
PG_DSN        ?= postgresql://botparser:botparser@localhost:5432/botparser
DB_CSV        ?= DataBase/annuaire-des-entreprises-nautisme.csv

# ---- Options pipeline --------------------------------------------------------
LIMIT         ?=
MIN_EMPLOYEES ?= 10
PIPELINE_OPTS :=
ifdef LIMIT
  PIPELINE_OPTS += --limit $(LIMIT)
endif
PIPELINE_OPTS += --min-employees $(MIN_EMPLOYEES)

# ---- Couleurs ----------------------------------------------------------------
BOLD  := \033[1m
GREEN := \033[32m
CYAN  := \033[36m
RESET := \033[0m

# ==============================================================================
# Cibles par défaut
# ==============================================================================

.DEFAULT_GOAL := help

.PHONY: help install install-dev run run-fresh dry-run run-pg \
        nautisme architectes immobilier restaurants \
        sectors results clean clean-results clean-all \
        lint check-db venv-info \
        test test-verbose test-cov \
        docker-up docker-down import-db pg-shell

# ==============================================================================
# Aide
# ==============================================================================

help:
	@printf "$(BOLD)Pipeline de prospection SEO$(RESET)\n\n"
	@printf "$(CYAN)Configuration$(RESET)\n"
	@printf "  SECTOR=$(SECTOR)   MIN_EMPLOYEES=$(MIN_EMPLOYEES)   LIMIT=$(LIMIT)\n"
	@printf "  Python : $(PYTHON)\n\n"
	@printf "$(CYAN)Commandes principales$(RESET)\n"
	@printf "  $(BOLD)make install$(RESET)                    Crée le venv et installe les dépendances\n"
	@printf "  $(BOLD)make run$(RESET)                        Lance le pipeline (SECTOR=$(SECTOR) par défaut)\n"
	@printf "  $(BOLD)make run SECTOR=architectes$(RESET)     Lance pour un autre secteur\n"
	@printf "  $(BOLD)make run-fresh$(RESET)                  Lance en effaçant les anciens résultats\n"
	@printf "  $(BOLD)make dry-run$(RESET)                    Test rapide (LIMIT=5, skip-audit)\n\n"
	@printf "$(CYAN)Raccourcis secteurs$(RESET)\n"
	@printf "  $(BOLD)make nautisme$(RESET)     make architectes     make immobilier     make restaurants\n\n"
	@printf "$(CYAN)PostgreSQL / Docker$(RESET)\n"
	@printf "  $(BOLD)make docker-up$(RESET)                  Démarre PostgreSQL (Docker Compose)\n"
	@printf "  $(BOLD)make docker-down$(RESET)                Arrête le conteneur PostgreSQL\n"
	@printf "  $(BOLD)make import-db$(RESET)                  Importe le CSV INSEE dans PostgreSQL\n"
	@printf "  $(BOLD)make import-db DB_CSV=path/to/file.csv$(RESET)\n"
	@printf "  $(BOLD)make pg-shell$(RESET)                   Shell psql interactif\n"
	@printf "  $(BOLD)make run-pg$(RESET)                     Pipeline via PostgreSQL (secteur courant)\n\n"
	@printf "$(CYAN)Utilitaires$(RESET)\n"
	@printf "  $(BOLD)make sectors$(RESET)                    Liste les secteurs disponibles\n"
	@printf "  $(BOLD)make results$(RESET)                    Affiche les résultats existants\n"
	@printf "  $(BOLD)make check-db$(RESET)                   Vérifie la base de données INSEE\n"
	@printf "  $(BOLD)make test$(RESET)                       Lance les tests unitaires\n"
	@printf "  $(BOLD)make test-verbose$(RESET)               Tests avec détail\n"
	@printf "  $(BOLD)make test-cov$(RESET)                   Tests + rapport de couverture\n"
	@printf "  $(BOLD)make lint$(RESET)                       Analyse le code (ruff / flake8)\n"
	@printf "  $(BOLD)make clean$(RESET)                      Supprime les résultats du secteur courant\n"
	@printf "  $(BOLD)make clean-results$(RESET)              Supprime TOUS les résultats\n"
	@printf "  $(BOLD)make clean-all$(RESET)                  Supprime résultats + caches Python\n"

# ==============================================================================
# Installation
# ==============================================================================

install: _check-python
	@printf "$(GREEN)→ Création du venv WSL$(RESET)\n"
	python3 -m venv .venv_wsl
	$(PYTHON_LINUX) -m pip install --upgrade pip --quiet
	$(PYTHON_LINUX) -m pip install -r requirements.txt --quiet
	@printf "$(GREEN)✓ Installation terminée$(RESET)\n"

install-dev: install
	@printf "$(GREEN)→ Installation des dépendances de dev$(RESET)\n"
	$(PYTHON_LINUX) -m pip install pytest pytest-mock --quiet
	@printf "$(GREEN)✓ Dev dependencies installées$(RESET)\n"

_check-python:
	@command -v python3 >/dev/null 2>&1 || { \
	  echo "Erreur : python3 introuvable. Installez Python 3.10+."; exit 1; }

venv-info:
	@printf "Interpréteur : $(PYTHON)\n"
	@$(PYTHON) --version

# ==============================================================================
# Pipeline
# ==============================================================================

_sector-file = $(SECTORS_DIR)/$(1).txt

_check-sector:
	@test -f $(call _sector-file,$(SECTOR)) || { \
	  echo "Erreur : secteur '$(SECTOR)' introuvable → $(call _sector-file,$(SECTOR))"; \
	  echo "Secteurs disponibles :"; \
	  ls $(SECTORS_DIR)/*.txt | xargs -n1 basename | sed 's/\.txt//'; \
	  exit 1; }

_check-venv:
	@test -f $(PYTHON) || { \
	  echo "Erreur : venv absent. Lancez : make install"; exit 1; }

run: _check-venv _check-sector
	@printf "$(GREEN)→ Pipeline : secteur=$(SECTOR) min-employees=$(MIN_EMPLOYEES)$(RESET)\n"
	$(PYTHON) $(SCRIPTS_DIR)/run_full_pipeline.py \
	  --sector $(call _sector-file,$(SECTOR)) \
	  --no-fresh \
	  $(PIPELINE_OPTS)

run-fresh: _check-venv _check-sector
	@printf "$(GREEN)→ Pipeline (fresh) : secteur=$(SECTOR)$(RESET)\n"
	$(PYTHON) $(SCRIPTS_DIR)/run_full_pipeline.py \
	  --sector $(call _sector-file,$(SECTOR)) \
	  $(PIPELINE_OPTS)

dry-run: _check-venv _check-sector
	@printf "$(GREEN)→ Dry-run : secteur=$(SECTOR) limit=5 skip-audit$(RESET)\n"
	$(PYTHON) $(SCRIPTS_DIR)/run_full_pipeline.py \
	  --sector $(call _sector-file,$(SECTOR)) \
	  --no-fresh \
	  --limit 5 \
	  --skip-audit

run-pg: _check-venv _check-sector
	@printf "$(GREEN)→ Pipeline (PostgreSQL) : secteur=$(SECTOR) min-employees=$(MIN_EMPLOYEES)$(RESET)\n"
	$(PYTHON) $(SCRIPTS_DIR)/run_full_pipeline.py \
	  --sector $(call _sector-file,$(SECTOR)) \
	  --no-fresh \
	  --pg-dsn $(PG_DSN) \
	  $(PIPELINE_OPTS)

# ---- Raccourcis secteurs -----------------------------------------------------

nautisme:
	$(MAKE) run SECTOR=nautisme

architectes:
	$(MAKE) run SECTOR=architectes

immobilier:
	$(MAKE) run SECTOR=immobilier

restaurants:
	$(MAKE) run SECTOR=restaurants

# ==============================================================================
# Utilitaires
# ==============================================================================

sectors:
	@printf "$(CYAN)Secteurs disponibles$(RESET)\n"
	@for f in $(SECTORS_DIR)/*.txt; do \
	  name=$$(basename $$f .txt); \
	  codes=$$(grep -v '^#' $$f | grep -v '^$$' | wc -l); \
	  printf "  %-20s %s codes APE\n" "$$name" "$$codes"; \
	done

results:
	@printf "$(CYAN)Résultats disponibles$(RESET)\n"
	@for d in $(RESULTS_DIR)/*/; do \
	  sector=$$(basename $$d); \
	  count=$$(ls "$$d"*.csv 2>/dev/null | wc -l); \
	  if [ $$count -gt 0 ]; then \
	    latest=$$(ls -t "$$d"*.csv 2>/dev/null | head -1); \
	    lines=$$(tail -n +2 "$$latest" 2>/dev/null | wc -l); \
	    printf "  %-20s %s fichiers CSV — %s entrées (dernier: %s)\n" \
	      "$$sector" "$$count" "$$lines" "$$(basename $$latest)"; \
	  else \
	    printf "  %-20s (vide)\n" "$$sector"; \
	  fi; \
	done

check-db:
	@printf "$(CYAN)Base de données INSEE$(RESET)\n"
	@for f in $(DB_DIR)/*.csv; do \
	  [ -f "$$f" ] || { echo "  Aucun CSV dans $(DB_DIR)/"; break; }; \
	  size=$$(du -h "$$f" | cut -f1); \
	  lines=$$(wc -l < "$$f"); \
	  printf "  %-60s %6s  %s lignes\n" "$$(basename $$f)" "$$size" "$$lines"; \
	done

# ==============================================================================
# Tests
# ==============================================================================

test: _check-venv
	@printf "$(GREEN)→ Tests unitaires$(RESET)\n"
	$(PYTHON) -m pytest tests/ -q

test-verbose: _check-venv
	@printf "$(GREEN)→ Tests unitaires (verbose)$(RESET)\n"
	$(PYTHON) -m pytest tests/ -v

test-cov: _check-venv
	@printf "$(GREEN)→ Tests + couverture$(RESET)\n"
	$(PYTHON) -m pytest tests/ --cov=Scripts --cov-report=term-missing -q

# ==============================================================================
# PostgreSQL / Docker
# ==============================================================================

docker-up:
	@printf "$(GREEN)→ Démarrage PostgreSQL (Docker Compose)$(RESET)\n"
	docker compose up -d --wait
	@printf "$(GREEN)✓ PostgreSQL prêt sur localhost:5432$(RESET)\n"

docker-down:
	@printf "$(GREEN)→ Arrêt PostgreSQL$(RESET)\n"
	docker compose down
	@printf "$(GREEN)✓ Conteneur arrêté$(RESET)\n"

import-db: _check-venv docker-up
	@printf "$(GREEN)→ Import CSV INSEE : $(DB_CSV)$(RESET)\n"
	$(PYTHON) db/importer.py $(DB_CSV) --drop
	@printf "$(GREEN)✓ Import terminé$(RESET)\n"

pg-shell:
	@printf "$(GREEN)→ Shell PostgreSQL (Ctrl+D pour quitter)$(RESET)\n"
	docker exec -it botparser_pg psql -U botparser -d botparser

# ==============================================================================
# Qualité du code
# ==============================================================================

lint: _check-venv
	@if $(PYTHON) -m ruff --version >/dev/null 2>&1; then \
	  $(PYTHON) -m ruff check $(SCRIPTS_DIR)/; \
	elif $(PYTHON) -m flake8 --version >/dev/null 2>&1; then \
	  $(PYTHON) -m flake8 $(SCRIPTS_DIR)/ --max-line-length=120; \
	else \
	  echo "Avertissement : ni ruff ni flake8 disponibles. Installez-les dans le venv."; \
	fi

# ==============================================================================
# Nettoyage
# ==============================================================================

clean: _check-sector
	@printf "$(GREEN)→ Nettoyage : $(RESULTS_DIR)/$(SECTOR)/$(RESET)\n"
	rm -rf $(RESULTS_DIR)/$(SECTOR)/
	@printf "$(GREEN)✓ Fait$(RESET)\n"

clean-results:
	@printf "$(GREEN)→ Suppression de tous les résultats$(RESET)\n"
	rm -rf $(RESULTS_DIR)/*/
	@printf "$(GREEN)✓ Fait$(RESET)\n"

clean-all: clean-results
	@printf "$(GREEN)→ Nettoyage des caches Python$(RESET)\n"
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -f website_finder.log pipeline_*.log
	@printf "$(GREEN)✓ Fait$(RESET)\n"
