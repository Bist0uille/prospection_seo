#!/usr/bin/env python3
"""
Pipeline de prospection SEO nautisme.

Exécute le pipeline complet :
  1. Filtrage par codes NAF nautisme + tranche d'effectifs
  2. Recherche de sites web (Selenium / DuckDuckGo)
  3. Vérification des sites par domaine
  4. Audits Lighthouse
  5. Scoring de prospection

Usage:
  python run_full_pipeline.py [--no-fresh] [--limit N] [--skip-lighthouse]
"""

import os
import sys
import glob
import argparse
import subprocess
import platform

from Scripts.prospect_analyzer import (
    filter_companies_by_employees,
    verify_websites_by_domain,
    run_lighthouse_reports,
    create_prospect_scoring,
)

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_CSV = 'DataBase/annuaire-des-entreprises-nouvelle_aquitaine.csv'

NAF_CODES_NAUTISME = [
    '3012Z',  # Construction de bateaux de plaisance
    '3011Z',  # Construction de navires et structures flottantes
    '3315Z',  # Réparation et maintenance navale
    '5010Z',  # Transports maritimes et côtiers de passagers
    '5020Z',  # Transports maritimes et côtiers de fret
    '5222Z',  # Services auxiliaires des transports par eau
    '7734Z',  # Location de matériels de transport par eau
    '7721Z',  # Location d'articles de loisirs (bateaux plaisance)
    '4764Z',  # Commerce de détail d'articles de sport (accastillage)
    '9329Z',  # Activités récréatives (marinas)
]

EMPLOYEE_CODES = ['11', '12', '21', '22', '31', '32', '41', '42', '51', '52', '53']

# Fichiers intermédiaires (nettoyés en fin de pipeline)
FILTERED_CSV = 'Results/filtered_companies.csv'
WEBSITES_CSV = 'Results/filtered_companies_websites.csv'
VERIFIED_CSV = 'Results/verified_websites.csv'
LIGHTHOUSE_CSV = 'Results/lighthouse_reports.csv'

# Fichier final (conservé)
FINAL_REPORT_CSV = 'Results/final_prospect_report.csv'
LIGHTHOUSE_DIR = 'Reports/Lighthouse'

INTERMEDIATE_FILES = [FILTERED_CSV, WEBSITES_CSV, VERIFIED_CSV, LIGHTHOUSE_CSV]

# ============================================================================
# HELPERS
# ============================================================================

def _python_cmd():
    """Renvoie le chemin vers l'interpréteur Python adapté à la plateforme."""
    if platform.system() == 'Windows':
        candidates = ['.venv/Scripts/python.exe']
    else:
        candidates = ['.venv_linux/bin/python', '.venv/bin/python']
    for venv in candidates:
        if os.path.exists(venv):
            # Vérifier que le venv a pandas (proxy pour "environnement fonctionnel")
            check = subprocess.run(
                [venv, '-c', 'import pandas'],
                capture_output=True, timeout=10,
            )
            if check.returncode == 0:
                return venv
    return sys.executable


def clean_results(clean_lighthouse=False):
    """Supprime les fichiers intermédiaires et, optionnellement, les anciens rapports."""
    for f in INTERMEDIATE_FILES:
        if os.path.exists(f):
            os.remove(f)
            print(f"  Supprimé : {f}")
    if os.path.exists(FINAL_REPORT_CSV):
        os.remove(FINAL_REPORT_CSV)
        print(f"  Supprimé : {FINAL_REPORT_CSV}")
    if clean_lighthouse:
        for f in glob.glob(os.path.join(LIGHTHOUSE_DIR, '*.json')):
            os.remove(f)
        print(f"  Nettoyé : {LIGHTHOUSE_DIR}/*.json")


def cleanup_intermediates():
    """Supprime les fichiers intermédiaires en fin de pipeline."""
    for f in INTERMEDIATE_FILES:
        if os.path.exists(f):
            os.remove(f)
    print("Fichiers intermédiaires supprimés.")

# ============================================================================
# PIPELINE
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Pipeline de prospection SEO nautisme")
    parser.add_argument('--no-fresh', action='store_true',
                        help="Ne pas nettoyer les anciens résultats avant exécution")
    parser.add_argument('--limit', type=int, default=None,
                        help="Limiter le nombre d'entreprises traitées (pour les tests)")
    parser.add_argument('--skip-lighthouse', action='store_true',
                        help="Passer l'étape Lighthouse (utile pour les tests)")
    parser.add_argument('--keep-intermediates', action='store_true',
                        help="Conserver les fichiers intermédiaires")
    args = parser.parse_args()

    print("=" * 60)
    print("  PIPELINE DE PROSPECTION SEO — NAUTISME")
    print("=" * 60)

    # --- Vérification du fichier source ---
    if not os.path.exists(BASE_CSV):
        print(f"\nErreur : fichier source introuvable → {BASE_CSV}")
        sys.exit(1)

    # --- Création des répertoires ---
    os.makedirs('Results', exist_ok=True)
    os.makedirs(LIGHTHOUSE_DIR, exist_ok=True)

    # --- Nettoyage (fresh start) ---
    if not args.no_fresh:
        print("\n[0] Nettoyage des anciens résultats...")
        clean_results(clean_lighthouse=False)

    # ------------------------------------------------------------------
    # Étape 1 : Filtrage NAF + effectifs
    # ------------------------------------------------------------------
    print("\n[1/5] Filtrage des entreprises (NAF nautisme + effectifs)...")
    filter_companies_by_employees(
        BASE_CSV,
        FILTERED_CSV,
        naf_codes=NAF_CODES_NAUTISME,
        employee_codes=EMPLOYEE_CODES,
    )

    # ------------------------------------------------------------------
    # Étape 2 : Recherche de sites web (Selenium / DuckDuckGo)
    # ------------------------------------------------------------------
    print("\n[2/5] Recherche de sites web...")

    # Supprimer le fichier output pour éviter le mode resume
    if os.path.exists(WEBSITES_CSV):
        os.remove(WEBSITES_CSV)

    python_cmd = _python_cmd()
    find_cmd = [python_cmd, 'Scripts/find_websites.py', FILTERED_CSV, '--output_dir', 'Results']
    if args.limit:
        find_cmd += ['--limit', str(args.limit)]

    print(f"  Commande : {' '.join(find_cmd)}")
    result = subprocess.run(find_cmd, text=True)
    if result.returncode != 0:
        print("Erreur lors de la recherche de sites web.")
        sys.exit(1)

    if not os.path.exists(WEBSITES_CSV):
        print(f"Erreur : fichier attendu introuvable → {WEBSITES_CSV}")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Étape 3 : Vérification des sites par domaine
    # ------------------------------------------------------------------
    print("\n[3/5] Vérification des sites par domaine...")
    verify_websites_by_domain(WEBSITES_CSV, VERIFIED_CSV)

    # ------------------------------------------------------------------
    # Étape 4 : Audits Lighthouse
    # ------------------------------------------------------------------
    if not args.skip_lighthouse:
        print("\n[4/5] Audits Lighthouse...")
        run_lighthouse_reports(VERIFIED_CSV, LIGHTHOUSE_CSV, reports_dir=LIGHTHOUSE_DIR)
    else:
        print("\n[4/5] Audits Lighthouse — IGNORÉ (--skip-lighthouse)")
        # Copier verified → lighthouse pour que l'étape 5 fonctionne
        import shutil
        shutil.copy2(VERIFIED_CSV, LIGHTHOUSE_CSV)

    # ------------------------------------------------------------------
    # Étape 5 : Scoring de prospection
    # ------------------------------------------------------------------
    print("\n[5/5] Scoring de prospection...")
    create_prospect_scoring(LIGHTHOUSE_CSV, FINAL_REPORT_CSV)

    # --- Nettoyage des fichiers intermédiaires ---
    if not args.keep_intermediates:
        print("\nNettoyage des fichiers intermédiaires...")
        cleanup_intermediates()

    # --- Résumé ---
    print("\n" + "=" * 60)
    print("  PIPELINE TERMINÉ")
    print("=" * 60)
    if os.path.exists(FINAL_REPORT_CSV):
        import pandas as pd
        df = pd.read_csv(FINAL_REPORT_CSV)
        print(f"  Rapport final : {FINAL_REPORT_CSV}")
        print(f"  Nombre de prospects : {len(df)}")
        scored = df[df['prospect_score'] > 0]
        if not scored.empty:
            print(f"  Prospects avec score : {len(scored)}")
            print(f"  Score moyen : {scored['prospect_score'].mean():.1f}/10")
    print(f"  Rapports Lighthouse : {LIGHTHOUSE_DIR}/")
    print()


if __name__ == '__main__':
    main()
