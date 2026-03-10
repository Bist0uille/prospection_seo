#!/usr/bin/env python3
"""
Fetch companies from the official recherche-entreprises API (data.gouv.fr).

Replaces step 1 of the pipeline (INSEE CSV filtering) for cases where no local
CSV is available or when a fresh pull is needed.

Output CSV is compatible with find_websites.py and site_health_checker.py.

Usage:
  python Scripts/fetch_sirene_api.py --sector Sectors/nautisme.txt
  python Scripts/fetch_sirene_api.py --sector Sectors/nautisme.txt --departements 33,64
  python Scripts/fetch_sirene_api.py --sector Sectors/nautisme.txt --output Results/nautisme_na/filtered_companies.csv
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path

import click
import pandas as pd
import requests
from tqdm import tqdm

# ── Constants ────────────────────────────────────────────────────────────────

API_URL = "https://recherche-entreprises.api.gouv.fr/search"
PER_PAGE = 25  # API max
DELAY_S = 0.3  # between requests
RETRY_DELAY_S = 5.0  # on 429
MAX_RETRIES = 3

DEPARTEMENTS_NA = [
    "16", "17", "19", "23", "24",
    "33", "40", "47", "64", "79", "86", "87",
]

# Regex: NAF code without dot (e.g. "3012Z")
_NAF_RE = re.compile(r"^\d{4}[A-Z]$")
# Regex: line in sector file (e.g. "3012Z - Description")
_SECTOR_LINE_RE = re.compile(r"^(\d{4}[A-Z])\s*-")


# ── Helpers ──────────────────────────────────────────────────────────────────

def _naf_to_api(code: str) -> str:
    """Convert '3012Z' → '30.12Z' (format expected by the API)."""
    return f"{code[:2]}.{code[2:]}"


def _naf_from_api(code: str) -> str:
    """Convert '30.12Z' → '3012Z' (pipeline format)."""
    return code.replace(".", "")


def _parse_sector_file(path: Path) -> list[str]:
    """Extract NAF codes from a sector file (e.g. Sectors/nautisme.txt)."""
    codes: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        m = _SECTOR_LINE_RE.match(line.strip())
        if m:
            codes.append(m.group(1))
    if not codes:
        raise click.ClickException(f"No NAF codes found in {path}")
    return codes


def _fetch_page(session: requests.Session, naf_api: str, dept: str, page: int) -> dict:
    """Fetch one page, retry on 429, raise on other errors."""
    params = {
        "activite_principale": naf_api,
        "departement": dept,
        "etat_administratif": "A",
        "page": page,
        "per_page": PER_PAGE,
    }
    for attempt in range(1, MAX_RETRIES + 1):
        resp = session.get(API_URL, params=params, timeout=15)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_S * attempt)
                continue
            raise click.ClickException(
                f"Rate-limited after {MAX_RETRIES} retries for {naf_api} / dept {dept} / page {page}"
            )
        resp.raise_for_status()
    # unreachable
    raise RuntimeError("Unexpected exit from retry loop")


def _result_to_row(r: dict) -> dict:
    """Map one API result entry to a pipeline-compatible CSV row."""
    siege = r.get("siege") or {}
    naf_raw = r.get("activite_principale", "")
    return {
        "siren": r.get("siren", ""),
        "denominationUniteLegale": r.get("nom_complet", ""),
        "activitePrincipaleUniteLegale": _naf_from_api(naf_raw),
        "trancheEffectifsUniteLegale": r.get("tranche_effectif_salarie", ""),
        "etatAdministratifUniteLegale": r.get("etat_administratif", "A"),
        "codePostalEtablissement": siege.get("code_postal", ""),
        "libelleCommuneEtablissement": siege.get("libelle_commune", ""),
        "etablissementSiege": siege.get("est_siege", True),
    }


# ── Main ─────────────────────────────────────────────────────────────────────

@click.command()
@click.option(
    "--sector", "-s",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="Sector file containing NAF codes (e.g. Sectors/nautisme.txt).",
)
@click.option(
    "--departements", "-d",
    default=",".join(DEPARTEMENTS_NA),
    show_default=True,
    help="Comma-separated department numbers.",
)
@click.option(
    "--output", "-o",
    default=None,
    type=click.Path(path_type=Path),
    help="Output CSV path (default: Results/<sector_name>/filtered_companies.csv).",
)
def main(sector: Path, departements: str, output: Path | None) -> None:
    """Fetch active companies from the recherche-entreprises API and write a pipeline-ready CSV."""

    naf_codes = _parse_sector_file(sector)
    depts = [d.strip() for d in departements.split(",") if d.strip()]

    # Default output path mirrors pipeline convention
    if output is None:
        sector_name = sector.stem  # e.g. "nautisme"
        output = Path("Results") / sector_name / "filtered_companies.csv"

    output.parent.mkdir(parents=True, exist_ok=True)

    click.echo(f"NAF codes : {', '.join(naf_codes)}")
    click.echo(f"Départements : {', '.join(depts)}")
    click.echo(f"Output : {output}")
    click.echo()

    rows: list[dict] = []
    seen_sirens: set[str] = set()

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    combos = [(naf, dept) for naf in naf_codes for dept in depts]

    with tqdm(total=len(combos), desc="Fetching", unit="combo") as pbar:
        for naf_code, dept in combos:
            naf_api = _naf_to_api(naf_code)
            pbar.set_postfix(naf=naf_code, dept=dept)

            try:
                first = _fetch_page(session, naf_api, dept, page=1)
            except Exception as exc:
                tqdm.write(f"  [WARN] {naf_code} / dept {dept} — {exc}")
                pbar.update(1)
                continue

            total_pages = first.get("total_pages", 1) or 1
            total_results = first.get("total_results", 0)
            tqdm.write(f"  {naf_code} / dept {dept} → {total_results} résultats ({total_pages} pages)")

            pages_data = [first]
            for page in range(2, total_pages + 1):
                time.sleep(DELAY_S)
                try:
                    pages_data.append(_fetch_page(session, naf_api, dept, page))
                except Exception as exc:
                    tqdm.write(f"  [WARN] page {page} — {exc}")
                    break

            for page_data in pages_data:
                for result in page_data.get("results", []):
                    siren = result.get("siren", "")
                    if siren and siren not in seen_sirens:
                        seen_sirens.add(siren)
                        rows.append(_result_to_row(result))

            time.sleep(DELAY_S)
            pbar.update(1)

    if not rows:
        click.echo("[WARN] Aucune entreprise récupérée.")
        sys.exit(1)

    df = pd.DataFrame(rows)
    df.to_csv(output, index=False, encoding="utf-8-sig")

    click.echo(f"\n{len(df)} entreprises uniques → {output}")


if __name__ == "__main__":
    main()
