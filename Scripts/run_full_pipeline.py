#!/usr/bin/env python3
"""
Pipeline de prospection SEO – outil universel multi-secteur.

Usage :
  python Scripts/run_full_pipeline.py --sector Sectors/nautisme.txt
  python Scripts/run_full_pipeline.py --codes 3012Z,3011Z,3315Z --name nautisme
  python Scripts/run_full_pipeline.py --sector Sectors/architectes.txt --limit 50
  python Scripts/run_full_pipeline.py --sector Sectors/restaurants.txt --min-employees 1

Les résultats sont isolés par secteur dans Results/{nom_secteur}/.
"""

import os
import sys
import glob
import argparse
import subprocess
import platform

# S'assurer que la racine du projet est dans le sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)
sys.path.insert(0, PROJECT_ROOT)

from Scripts.prospect_analyzer import (
    filter_companies_by_employees,
    verify_websites_by_domain,
    create_prospect_scoring_v2,
)
from Scripts.seo_auditor import run_seo_audit

# ============================================================================
# TRANCHES D'EFFECTIFS INSEE
# ============================================================================

# Codes INSEE tranche d'effectifs et leur borne inférieure
_EMPLOYEE_THRESHOLDS = [
    (0,     'NN'), (0,     '00'), (1,   '01'), (3,   '02'), (6,   '03'),
    (10,    '11'), (20,    '12'), (50,  '21'), (100, '22'), (200, '31'),
    (250,   '32'), (500,   '41'), (1000,'42'), (2000,'51'), (5000,'52'),
    (10000, '53'),
]


def get_employee_codes(min_employees: int) -> list:
    """Retourne les codes INSEE tranche d'effectifs pour un minimum donné.

    Exemple :
        get_employee_codes(10)  → ['11', '12', '21', '22', '31', '32', '41', '42', '51', '52', '53']
        get_employee_codes(0)   → tous les codes (pas de filtre)
    """
    return [code for lower_bound, code in _EMPLOYEE_THRESHOLDS if lower_bound >= min_employees]


# ============================================================================
# HELPERS
# ============================================================================

def load_ape_codes(sector_file: str) -> list:
    """Charge les codes APE depuis un fichier texte.

    Format accepté (une entrée par ligne) :
        3012Z - Construction de bateaux de plaisance
        3011Z
        # commentaires et lignes vides ignorés

    Retourne une liste de codes APE (ex. ['3012Z', '3011Z']).
    """
    codes = []
    with open(sector_file, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            # Extraire le code : tout ce qui précède le premier tiret ou espace
            code = line.split('-')[0].strip().split()[0]
            if code:
                codes.append(code)
    if not codes:
        raise ValueError(f"Aucun code APE trouvé dans {sector_file}")
    return codes


def find_default_database() -> str | None:
    """Cherche automatiquement le fichier CSV source dans DataBase/."""
    candidates = [
        'DataBase/annuaire-des-entreprises-etablissements-juridique.csv',
        'DataBase/StockUniteLegale_utf8.csv',
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    # Fallback : le plus gros CSV présent dans DataBase/
    csvs = glob.glob('DataBase/*.csv')
    if csvs:
        return max(csvs, key=os.path.getsize)
    return None


def _python_cmd() -> str:
    """Retourne le chemin vers l'interpréteur Python du venv actif."""
    if platform.system() == 'Windows':
        candidates = ['.venv/Scripts/python.exe']
    else:
        candidates = ['.venv_linux/bin/python', '.venv/bin/python']
    for venv in candidates:
        if os.path.exists(venv):
            check = subprocess.run(
                [venv, '-c', 'import pandas'],
                capture_output=True, timeout=10,
            )
            if check.returncode == 0:
                return venv
    return sys.executable


# ============================================================================
# PIPELINE
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline de prospection SEO – multi-secteur universel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python Scripts/run_full_pipeline.py --sector Sectors/nautisme.txt
  python Scripts/run_full_pipeline.py --sector Sectors/architectes.txt --limit 100
  python Scripts/run_full_pipeline.py --codes 3012Z,3011Z --name nautisme
  python Scripts/run_full_pipeline.py --sector Sectors/restaurants.txt --min-employees 1
  python Scripts/run_full_pipeline.py --sector Sectors/nautisme.txt --no-fresh --skip-audit
        """,
    )

    # --- Source des codes APE (obligatoire, l'un ou l'autre) ---
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--sector', type=str, metavar='FICHIER',
        help="Fichier .txt avec les codes APE (ex: Sectors/nautisme.txt)",
    )
    group.add_argument(
        '--codes', type=str, metavar='CODE1,CODE2,...',
        help="Codes APE séparés par virgule (ex: 3012Z,3011Z,3315Z)",
    )

    # --- Configuration ---
    parser.add_argument(
        '--name', type=str, default=None,
        help="Nom du secteur pour le dossier résultats (défaut: nom du fichier sector)",
    )
    parser.add_argument(
        '--db', type=str, default=None, metavar='FICHIER',
        help="Chemin vers le CSV source INSEE (auto-détecté dans DataBase/ si absent)",
    )
    parser.add_argument(
        '--min-employees', type=int, default=10, metavar='N',
        help="Nombre minimum de salariés à cibler (défaut: 10)",
    )

    # --- Options pipeline ---
    parser.add_argument(
        '--no-fresh', action='store_true',
        help="Ne pas supprimer les anciens résultats avant l'exécution",
    )
    parser.add_argument(
        '--limit', type=int, default=None,
        help="Limiter le nombre d'entreprises traitées (pour les tests)",
    )
    parser.add_argument(
        '--skip-audit', action='store_true',
        help="Passer l'étape Audit SEO (étape 4)",
    )
    parser.add_argument(
        '--keep-intermediates', action='store_true',
        help="Conserver les fichiers intermédiaires après le pipeline",
    )

    args = parser.parse_args()

    # --- Résoudre les codes APE ---
    if args.sector:
        if not os.path.exists(args.sector):
            print(f"Erreur : fichier secteur introuvable → {args.sector}")
            sys.exit(1)
        try:
            naf_codes = load_ape_codes(args.sector)
        except ValueError as e:
            print(f"Erreur : {e}")
            sys.exit(1)
        sector_name = args.name or os.path.splitext(os.path.basename(args.sector))[0]
    else:
        naf_codes = [c.strip() for c in args.codes.split(',') if c.strip()]
        if not naf_codes:
            print("Erreur : aucun code APE valide dans --codes.")
            sys.exit(1)
        sector_name = args.name or 'secteur'

    # --- Résoudre la base de données ---
    base_csv = args.db or find_default_database()
    if not base_csv or not os.path.exists(base_csv):
        print("Erreur : base de données INSEE introuvable.")
        print("  Placez le fichier dans DataBase/ ou utilisez --db pour le spécifier.")
        sys.exit(1)

    # --- Codes tranche d'effectifs ---
    employee_codes = get_employee_codes(args.min_employees)
    if not employee_codes:
        print(f"Erreur : aucun code d'effectifs pour min-employees={args.min_employees}.")
        sys.exit(1)

    # --- Chemins de sortie (isolés par secteur) ---
    output_dir = f'Results/{sector_name}'
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs('Reports/Lighthouse', exist_ok=True)

    filtered_csv     = f'{output_dir}/filtered_companies.csv'
    websites_csv     = f'{output_dir}/filtered_companies_websites.csv'
    verified_csv     = f'{output_dir}/verified_websites.csv'
    seo_audit_csv    = f'{output_dir}/seo_audit.csv'
    final_report_csv = f'{output_dir}/final_prospect_report.csv'

    intermediate_files = [filtered_csv, websites_csv, verified_csv, seo_audit_csv]

    # --- Affichage de la configuration ---
    print("=" * 60)
    print(f"  PIPELINE DE PROSPECTION SEO — {sector_name.upper()}")
    print("=" * 60)
    print(f"  Base de données  : {base_csv}")
    print(f"  Codes APE ({len(naf_codes)})    : {', '.join(naf_codes)}")
    print(f"  Min. salariés    : {args.min_employees}+")
    print(f"  Dossier résultats: {output_dir}/")
    print("=" * 60)

    # --- Nettoyage (fresh start) ---
    if not args.no_fresh:
        print("\n[0] Nettoyage des anciens résultats...")
        for f in intermediate_files + [final_report_csv]:
            if os.path.exists(f):
                os.remove(f)
                print(f"  Supprimé : {f}")

    # ------------------------------------------------------------------
    # Étape 1 : Filtrage NAF + effectifs
    # ------------------------------------------------------------------
    print(f"\n[1/5] Filtrage des entreprises ({len(naf_codes)} codes APE, {args.min_employees}+ salariés)...")
    filter_companies_by_employees(
        base_csv,
        filtered_csv,
        naf_codes=naf_codes,
        employee_codes=employee_codes,
    )

    # ------------------------------------------------------------------
    # Étape 2 : Recherche de sites web (Selenium / DuckDuckGo)
    # ------------------------------------------------------------------
    print("\n[2/5] Recherche de sites web (Selenium / DuckDuckGo)...")

    if os.path.exists(websites_csv):
        os.remove(websites_csv)

    python_cmd = _python_cmd()
    find_cmd = [
        python_cmd, 'Scripts/find_websites.py',
        filtered_csv, '--output_dir', output_dir,
    ]
    if args.limit:
        find_cmd += ['--limit', str(args.limit)]

    print(f"  Commande : {' '.join(find_cmd)}")
    result = subprocess.run(find_cmd, text=True)
    if result.returncode != 0:
        print("Erreur lors de la recherche de sites web.")
        sys.exit(1)

    if not os.path.exists(websites_csv):
        print(f"Erreur : fichier attendu introuvable → {websites_csv}")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Étape 3 : Vérification des sites par domaine
    # ------------------------------------------------------------------
    print("\n[3/5] Vérification des sites par domaine...")
    verify_websites_by_domain(websites_csv, verified_csv)

    # ------------------------------------------------------------------
    # Étape 4 : Audit SEO (crawl léger)
    # ------------------------------------------------------------------
    if not args.skip_audit:
        print("\n[4/5] Audit SEO (crawl léger)...")
        run_seo_audit(verified_csv, seo_audit_csv, max_pages=30)
    else:
        print("\n[4/5] Audit SEO — IGNORÉ (--skip-audit)")
        import shutil
        shutil.copy2(verified_csv, seo_audit_csv)

    # ------------------------------------------------------------------
    # Étape 5 : Scoring de prospection v2
    # ------------------------------------------------------------------
    print("\n[5/5] Scoring de prospection v2...")
    create_prospect_scoring_v2(seo_audit_csv, final_report_csv)

    # --- Nettoyage des fichiers intermédiaires ---
    if not args.keep_intermediates:
        print("\nNettoyage des fichiers intermédiaires...")
        for f in intermediate_files:
            if os.path.exists(f):
                os.remove(f)
        print("Fichiers intermédiaires supprimés.")

    # --- Résumé final ---
    print("\n" + "=" * 60)
    print("  PIPELINE TERMINÉ")
    print("=" * 60)
    if os.path.exists(final_report_csv):
        import pandas as pd
        df = pd.read_csv(final_report_csv)
        print(f"  Rapport final    : {final_report_csv}")
        print(f"  Prospects totaux : {len(df)}")
        scored = df[df['score'] > 0]
        if not scored.empty:
            print(f"  Avec score       : {len(scored)}")
            print(f"  Score moyen      : {scored['score'].mean():.1f}/10")
    print()


if __name__ == '__main__':
    main()
