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

from __future__ import annotations

import glob
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

import click
from pydantic import ValidationError

# ── Project root on sys.path ──────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT))

from Scripts.core.logging_config import get_logger, setup_pipeline_logging
from Scripts.core.models import PipelineConfig
from Scripts.contact_scraper import generate_html_report, run_contact_extraction
from Scripts.prospect_analyzer import (
    create_prospect_scoring_v2,
    filter_companies_by_employees,
    filter_companies_by_employees_pg,
    verify_websites_by_domain,
)
from Scripts.seo_auditor import run_seo_audit

logger = get_logger(__name__)


# ============================================================================
# TRANCHES D'EFFECTIFS INSEE
# ============================================================================

_EMPLOYEE_THRESHOLDS: list[tuple[int, str]] = [
    (0,     "NN"), (0,     "00"), (1,   "01"), (3,   "02"), (6,   "03"),
    (10,    "11"), (20,    "12"), (50,  "21"), (100, "22"), (200, "31"),
    (250,   "32"), (500,   "41"), (1000,"42"), (2000,"51"), (5000,"52"),
    (10000, "53"),
]


def get_employee_codes(min_employees: int) -> list[str]:
    """Return INSEE employee-band codes for a given minimum headcount.

    Example:
        get_employee_codes(10)  →  ['11', '12', '21', …, '53']
        get_employee_codes(0)   →  all codes (no filter)

    Args:
        min_employees: Minimum number of employees.

    Returns:
        List of INSEE tranche codes whose lower bound >= min_employees.
    """
    logger.debug("get_employee_codes(min_employees=%d)", min_employees)
    codes = [
        code
        for lower_bound, code in _EMPLOYEE_THRESHOLDS
        if lower_bound >= min_employees
    ]
    logger.debug("→ %d employee codes: %s", len(codes), codes)
    return codes


# ============================================================================
# HELPERS
# ============================================================================

def load_ape_codes(sector_file: Path) -> list[str]:
    """Load APE codes from a sector text file.

    Accepted format (one entry per line):
        3012Z - Construction de bateaux de plaisance
        3011Z
        # comments and blank lines are ignored

    Args:
        sector_file: Path to the ``.txt`` sector file.

    Returns:
        List of APE codes (e.g. ``['3012Z', '3011Z']``).

    Raises:
        ValueError: If no codes are found in the file.
    """
    logger.debug("load_ape_codes('%s')", sector_file)
    codes: list[str] = []
    with open(sector_file, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            code = line.split("-")[0].strip().split()[0]
            if code:
                codes.append(code)
    if not codes:
        raise ValueError(f"Aucun code APE trouvé dans {sector_file}")
    logger.info(
        "Loaded %d APE codes from '%s': %s",
        len(codes), sector_file, ", ".join(codes),
    )
    return codes


def find_default_database(sector_name: str | None = None) -> str | None:
    """Auto-detect the INSEE CSV source file inside DataBase/.

    Priority order:
        1. CSV whose filename contains the sector name (e.g. 'nautisme')
        2. annuaire-des-entreprises-etablissements-juridique.csv
        3. StockUniteLegale_utf8.csv
        4. Largest CSV in DataBase/ (fallback)

    Args:
        sector_name: Optional sector name used to find a sector-specific CSV first.

    Returns:
        Absolute path string, or None if DataBase/ contains no CSV.
    """
    logger.debug("find_default_database(sector_name=%r)", sector_name)

    # 1. Sector-specific match
    if sector_name:
        matches = glob.glob(f"DataBase/*{sector_name}*.csv")
        if matches:
            best = max(matches, key=os.path.getsize)
            logger.info("Auto-detected database (sector match): '%s'", best)
            return best

    # 2. Known generic files
    candidates = [
        "DataBase/annuaire-des-entreprises-etablissements-juridique.csv",
        "DataBase/StockUniteLegale_utf8.csv",
    ]
    for path in candidates:
        if os.path.exists(path):
            logger.info("Auto-detected database: '%s'", path)
            return path

    # 3. Largest CSV fallback
    csvs = glob.glob("DataBase/*.csv")
    if csvs:
        best = max(csvs, key=os.path.getsize)
        logger.info("Auto-detected database (largest CSV): '%s'", best)
        return best
    logger.warning("No database CSV found in DataBase/")
    return None


def _python_cmd() -> str:
    """Return the Python interpreter path from the active virtual environment.

    Checks candidate venv paths in preference order, verifying that pandas
    is importable (a proxy for a properly installed project venv).

    Returns:
        Absolute path to a Python executable, falling back to sys.executable.
    """
    logger.debug("_python_cmd()")
    if platform.system() == "Windows":
        candidates = [".venv/Scripts/python.exe"]
    else:
        candidates = [
            ".venv_wsl/bin/python",
            ".venv_linux/bin/python",
            ".venv/bin/python",
        ]
    for venv in candidates:
        if os.path.exists(venv):
            check = subprocess.run(
                [venv, "-c", "import pandas"],
                capture_output=True,
                timeout=10,
            )
            if check.returncode == 0:
                logger.debug("Using venv Python: %s", venv)
                return venv
    logger.debug("Falling back to sys.executable: %s", sys.executable)
    return sys.executable


# ============================================================================
# CLI
# ============================================================================

@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--sector",
    type=click.Path(dir_okay=False),
    default=None,
    help="Fichier .txt avec les codes APE (ex: Sectors/nautisme.txt)",
)
@click.option(
    "--codes",
    type=str,
    default=None,
    help="Codes APE séparés par virgule (ex: 3012Z,3011Z,3315Z)",
)
@click.option(
    "--name",
    type=str,
    default=None,
    help="Nom du secteur pour le dossier résultats (défaut: nom du fichier sector)",
)
@click.option(
    "--db",
    type=click.Path(dir_okay=False),
    default=None,
    help="Chemin vers le CSV source INSEE (auto-détecté dans DataBase/ si absent)",
)
@click.option(
    "--pg-dsn",
    type=str,
    default=None,
    envvar="BOTPARSER_PG_DSN",
    show_default=True,
    help="DSN PostgreSQL (ex: postgresql://user:pass@host/db). "
         "Remplace --db pour l'étape 1 si fourni.",
)
@click.option(
    "--min-employees",
    type=int,
    default=10,
    show_default=True,
    help="Nombre minimum de salariés à cibler",
)
@click.option(
    "--no-fresh",
    is_flag=True,
    default=False,
    help="Ne pas supprimer les anciens résultats avant l'exécution",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Limiter le nombre d'entreprises traitées (pour les tests)",
)
@click.option(
    "--skip-audit",
    is_flag=True,
    default=False,
    help="Passer l'étape Audit SEO (étape 4)",
)
@click.option(
    "--keep-intermediates",
    is_flag=True,
    default=False,
    help="Conserver les fichiers intermédiaires après le pipeline",
)
def main(
    sector: str | None,
    codes: str | None,
    name: str | None,
    db: str | None,
    pg_dsn: str | None,
    min_employees: int,
    no_fresh: bool,
    limit: int | None,
    skip_audit: bool,
    keep_intermediates: bool,
) -> None:
    """Pipeline de prospection SEO – multi-secteur universel.

    \b
    Exemples :
      python Scripts/run_full_pipeline.py --sector Sectors/nautisme.txt
      python Scripts/run_full_pipeline.py --sector Sectors/architectes.txt --limit 100
      python Scripts/run_full_pipeline.py --codes 3012Z,3011Z --name nautisme
      python Scripts/run_full_pipeline.py --sector Sectors/restaurants.txt --min-employees 1
      python Scripts/run_full_pipeline.py --sector Sectors/nautisme.txt --no-fresh --skip-audit
      python Scripts/run_full_pipeline.py --sector Sectors/nautisme.txt --pg-dsn postgresql://botparser:botparser@localhost:5432/botparser
    """
    # ── Validate all inputs through Pydantic ─────────────────────────────────
    try:
        config = PipelineConfig(
            sector=Path(sector) if sector else None,
            codes=codes,
            name=name,
            db=Path(db) if db else None,
            pg_dsn=pg_dsn,
            min_employees=min_employees,
            fresh=not no_fresh,
            limit=limit,
            skip_audit=skip_audit,
            keep_intermediates=keep_intermediates,
        )
    except ValidationError as exc:
        click.echo(f"Erreur de configuration :\n{exc}", err=True)
        sys.exit(1)

    # ── Resolve APE codes and sector name ────────────────────────────────────
    if config.sector:
        try:
            naf_codes = load_ape_codes(config.sector)
        except ValueError as exc:
            click.echo(f"Erreur : {exc}", err=True)
            sys.exit(1)
        sector_name = config.name or config.sector.stem
    else:
        naf_codes = config.codes  # already validated by Pydantic
        sector_name = config.name or "secteur"

    # ── Setup logging (sector_name now known) ────────────────────────────────
    setup_pipeline_logging(log_dir="Logs", sector_name=sector_name)
    logger.info("Pipeline starting — sector='%s'", sector_name)

    # ── Employee band codes ───────────────────────────────────────────────────
    employee_codes = get_employee_codes(config.min_employees)
    if not employee_codes:
        logger.error(
            "No employee codes resolved for min-employees=%d.", config.min_employees
        )
        sys.exit(1)

    # ── Resolve data source ───────────────────────────────────────────────────
    if config.pg_dsn:
        data_source = f"PostgreSQL ({config.pg_dsn})"
        base_csv = None
    else:
        base_csv = str(config.db) if config.db else find_default_database(sector_name)
        if not base_csv or not os.path.exists(base_csv):
            logger.error(
                "Database not found.  Place CSV in DataBase/ or use --db / --pg-dsn."
            )
            sys.exit(1)
        data_source = base_csv

    # ── Output paths ─────────────────────────────────────────────────────────
    output_dir      = f"Results/{sector_name}"
    filtered_csv     = f"{output_dir}/filtered_companies.csv"
    websites_csv     = f"{output_dir}/filtered_companies_websites.csv"
    verified_csv     = f"{output_dir}/verified_websites.csv"
    seo_audit_csv    = f"{output_dir}/seo_audit.csv"
    final_report_csv = f"{output_dir}/final_prospect_report.csv"
    intermediate_files = [filtered_csv, websites_csv, verified_csv, seo_audit_csv]

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs("Reports/Lighthouse", exist_ok=True)

    # ── Header ────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("  PIPELINE DE PROSPECTION SEO — %s", sector_name.upper())
    logger.info("=" * 60)
    logger.info("  Source de données: %s", data_source)
    logger.info("  Codes APE (%d)    : %s", len(naf_codes), ", ".join(naf_codes))
    logger.info("  Min. salariés    : %d+", config.min_employees)
    logger.info("  Dossier résultats: %s/", output_dir)
    logger.info("=" * 60)

    # ── [0] Fresh start ───────────────────────────────────────────────────────
    if config.fresh:
        logger.info("[0] Nettoyage des anciens résultats...")
        for f in intermediate_files + [final_report_csv]:
            if os.path.exists(f):
                os.remove(f)
                logger.debug("  Supprimé : %s", f)

    # ── [1/6] Filter by NAF codes + employee band ─────────────────────────────
    logger.info(
        "[1/6] Filtrage des entreprises (%d codes APE, %d+ salariés)...",
        len(naf_codes), config.min_employees,
    )
    if config.pg_dsn:
        from db.connection import get_engine
        pg_engine = get_engine(config.pg_dsn)
        filter_companies_by_employees_pg(
            pg_engine,
            filtered_csv,
            naf_codes=naf_codes,
            employee_codes=employee_codes,
        )
    else:
        filter_companies_by_employees(
            base_csv,
            filtered_csv,
            naf_codes=naf_codes,
            employee_codes=employee_codes,
        )

    # ── [2/6] Website search via DDGS / DuckDuckGo ───────────────────────────
    logger.info("[2/6] Recherche de sites web (DDGS / DuckDuckGo)...")
    if os.path.exists(websites_csv):
        os.remove(websites_csv)

    python_cmd = _python_cmd()
    find_cmd = [
        python_cmd, "Scripts/find_websites.py",
        filtered_csv, "--output-dir", output_dir,
    ]
    if config.limit:
        find_cmd += ["--limit", str(config.limit)]

    logger.info("  Commande : %s", " ".join(find_cmd))
    result = subprocess.run(find_cmd, text=True)
    if result.returncode != 0:
        logger.error(
            "Website search failed (exit code %d).", result.returncode
        )
        sys.exit(1)
    if not os.path.exists(websites_csv):
        logger.error("Expected output file missing → %s", websites_csv)
        sys.exit(1)

    # ── [3/6] Domain-based verification ──────────────────────────────────────
    logger.info("[3/6] Vérification des sites par domaine...")
    verify_websites_by_domain(websites_csv, verified_csv)

    # ── [4/6] SEO audit ───────────────────────────────────────────────────────
    if not config.skip_audit:
        logger.info("[4/6] Audit SEO (crawl léger)...")
        run_seo_audit(verified_csv, seo_audit_csv, max_pages=30)
    else:
        logger.info("[4/6] Audit SEO — IGNORÉ (--skip-audit)")
        shutil.copy2(verified_csv, seo_audit_csv)

    # ── [5/6] Prospect scoring v2 ─────────────────────────────────────────────
    logger.info("[5/6] Scoring de prospection v2...")
    create_prospect_scoring_v2(seo_audit_csv, final_report_csv)

    # ── [6/6] Contact extraction + HTML report ────────────────────────────────
    logger.info("[6/6] Extraction des contacts (email + téléphone)...")
    run_contact_extraction(final_report_csv, final_report_csv)
    html_report_path = f"{output_dir}/rapport_prospects.html"
    generate_html_report(final_report_csv, html_report_path, sector_name=sector_name)
    logger.info("  Rapport HTML : %s", html_report_path)

    # ── Cleanup intermediates ─────────────────────────────────────────────────
    if not config.keep_intermediates:
        logger.info("Nettoyage des fichiers intermédiaires...")
        for f in intermediate_files:
            if os.path.exists(f):
                os.remove(f)
        logger.debug("Fichiers intermédiaires supprimés.")

    # ── Final summary ─────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("  PIPELINE TERMINÉ")
    logger.info("=" * 60)
    if os.path.exists(final_report_csv):
        import pandas as pd
        df = pd.read_csv(final_report_csv)
        logger.info("  Rapport final    : %s", final_report_csv)
        logger.info("  Prospects totaux : %d", len(df))
        scored = df[df["score"] > 0]
        if not scored.empty:
            logger.info("  Avec score       : %d", len(scored))
            logger.info("  Score moyen      : %.1f/10", scored["score"].mean())


if __name__ == "__main__":
    main()
