#!/usr/bin/env python3
"""
Website discovery via DuckDuckGo search API (ddgs).

For each company in the input CSV, queries the DuckDuckGo API (no browser
needed), validates the result against the company name (keyword matching),
and saves a URL when a match is found.

Usage:
  python Scripts/find_websites.py Results/nautisme/filtered_companies.csv
  python Scripts/find_websites.py Results/nautisme/filtered_companies.csv --output-dir Results/nautisme
  python Scripts/find_websites.py Results/nautisme/filtered_companies.csv --limit 20
"""

from __future__ import annotations

import re
import sys
import time
import random
from pathlib import Path

import click
import pandas as pd
import requests
from pydantic import ValidationError
from tqdm import tqdm
from urllib.parse import urlparse

# ── Project root on sys.path ──────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from Scripts.core.logging_config import get_logger, setup_pipeline_logging
from Scripts.core.models import FindWebsitesConfig

logger = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

DIRECTORY_DOMAINS: set[str] = {
    "societe.com", "pagesjaunes.fr", "pappers.fr",
    "annuaire-entreprises.data.gouv.fr", "verif.com",
    "entreprises.lefigaro.fr", "fr.kompass.com", "facebook.com",
    "linkedin.com", "youtube.com", "wikipedia.org", "doctrine.fr",
    "app.dataprospects.fr", "service-de-reparation-de-bateaux.autour-de-moi.com",
    "entreprises.lagazettefrance.fr", "reseauexcellence.fr", "actunautique.com",
    # annuaires locaux / portails ville (ajoutés suite audit secteur nautisme)
    "autour-de-moi.tel", "autour-de-moi.com",
    "nous-larochelle.fr", "nous-bordeaux.fr",
    "investinbordeaux.fr",
    "portail-nautisme.fr",
    "bateauavendre.fr",
    "seapolelarochelle.com",
}

# Mots trop courants pour valider un match de domaine : ils apparaissent dans
# des dizaines de domaines non liés à l'entreprise cherchée.
# Ils restent dans la requête de recherche DDG, mais n'entrent pas dans le matching domaine.
#
# NE PAS mettre ici les mots spécifiques au secteur qui peuvent être LE nom
# de l'entreprise (ex: "marine" pour RC MARINE, "naval" pour NAVAL GROUP).
_DOMAIN_NOISE_WORDS: set[str] = {
    # Villes / régions
    "bordeaux", "larochelle", "nantes", "brest", "toulon", "marseille",
    "gironde", "charente", "atlantique", "arcachon",
    # Termes sectoriels trop génériques (apparaissent dans annuaires et portails)
    "maritime", "nautique", "nautisme", "yachting", "bateau", "bateaux",
    "port", "mer", "ocean",
    # Termes corporate génériques
    "france", "french", "groupe", "group", "services", "service",
    "industrie", "industries", "invest", "solutions",
}

_STOP_WORDS: set[str] = {"sa", "sas", "sarl", "eurl", "snc", "ste", "et", "de", "la", "les", "des"}


# ============================================================================
# HELPERS
# ============================================================================

def normalize_name(name: str) -> str:
    """Normalise a company name for comparison against a domain.

    Lowercases and removes all non-alphanumeric characters.

    Args:
        name: Raw company name.

    Returns:
        Normalised alphanumeric string.
    """
    name = name.lower()
    return re.sub(r"[^a-z0-9]", "", name)


def _strip_to_root(url: str) -> str:
    """Return the root URL (scheme + domain only), stripping any path.

    Examples:
        https://ap-yachting.fr/en/  →  https://ap-yachting.fr/
        https://lecamus.fr/notre-entreprise/  →  https://lecamus.fr/

    Args:
        url: Any URL string.

    Returns:
        Scheme + netloc with trailing slash.
    """
    try:
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}/"
    except Exception:
        return url


def _is_canadian(url: str) -> bool:
    """Return True if the URL has a .ca TLD (Canadian domain).

    Args:
        url: URL to check.

    Returns:
        True if the domain ends with ``.ca``.
    """
    try:
        domain = urlparse(url).netloc.lower().replace("www.", "")
        return domain.endswith(".ca")
    except Exception:
        return False


def _tld_priority(url: str) -> int:
    """Return a sort key for TLD preference.  Lower is better.

    .fr → 0  (clearly French, highest priority)
    others → 1

    Args:
        url: Candidate URL.

    Returns:
        0 for .fr, 1 otherwise.
    """
    try:
        domain = urlparse(url).netloc.lower().replace("www.", "")
        return 0 if domain.endswith(".fr") else 1
    except Exception:
        return 1


# ============================================================================
# DDGS SEARCH
# ============================================================================

_VERIFY_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; bot/1.0)"}


def _verify_url(url: str, timeout: int = 6) -> bool:
    """Return True if the URL responds with HTTP < 400.

    Args:
        url: Root URL to check.
        timeout: Request timeout in seconds.

    Returns:
        True if accessible, False otherwise.
    """
    try:
        resp = requests.head(
            url, timeout=timeout, allow_redirects=True, headers=_VERIFY_HEADERS
        )
        return resp.status_code < 400
    except Exception:
        return False


def _ddgs_search(query: str, max_results: int = 10) -> list[dict]:
    """Run a DDG text search and return results list."""
    from ddgs import DDGS
    return list(DDGS().text(query, max_results=max_results))


def _pick_best_candidate(
    results: list[dict],
    keywords: list[str],
) -> list[tuple[int, int, str]]:
    """Filter DDG results and return sorted candidates [(tld_priority, rank, root_url)].

    Returns an empty list if no candidate matches.

    Matching rules :
    - ``_DOMAIN_NOISE_WORDS`` (amélioration 2) : mots trop courants exclus du matching
      (bordeaux, maritime, yachting…). Pas de filtre sur la longueur — les acronymes
      spécifiques courts (MSC, AMEL, OCEA) sont des identifiants valides.
    - ``DIRECTORY_DOMAINS`` + pattern ``autour-de-moi`` (amélioration 3) : annuaires
      bloqués avant le matching.
    - Si tous les mots du nom sont du bruit, on refuse plutôt qu'accepter n'importe quoi.
    """
    # Keywords effectifs : tout sauf les noise words (amélioration 2)
    active_kws = [
        kw for kw in keywords
        if normalize_name(kw) not in _DOMAIN_NOISE_WORDS
    ]
    logger.debug("Active matching keywords (non-noise): %s", active_kws)

    candidates: list[tuple[int, int, str]] = []
    for rank, result in enumerate(results, 1):
        raw_url = result.get("href", "")
        if not raw_url:
            continue

        # Normalise to root domain — companies are in France, paths don't matter
        url = _strip_to_root(raw_url)
        domain = urlparse(url).netloc.replace("www.", "")
        cleaned_domain = domain.replace(".", "").replace("-", "")
        logger.debug("Checking rank %d: %s → root: %s", rank, raw_url, url)

        # amélioration 3 — blocklist étendue
        if domain in DIRECTORY_DOMAINS:
            logger.warning("Skipping known directory domain: %s", domain)
            continue
        if "autour-de-moi" in domain:
            logger.warning("Skipping directory domain (pattern): %s", domain)
            continue

        if _is_canadian(url):
            logger.warning("Skipping Canadian domain: %s", domain)
            continue

        if not active_kws:
            # Tous les mots sont du bruit — refuser plutôt qu'accepter n'importe quoi
            logger.debug("No active keywords after noise filter — skipping %s", domain)
            continue

        matched = any(normalize_name(kw) in cleaned_domain for kw in active_kws)
        if matched:
            matched_kws = [kw for kw in active_kws if normalize_name(kw) in cleaned_domain]
            candidates.append((_tld_priority(url), rank, url))
            logger.debug("Keyword match %s → domain '%s'", matched_kws, domain)
        else:
            logger.debug("No keyword match for domain '%s'", domain)

    candidates.sort(key=lambda x: (x[0], x[1]))
    return candidates


def get_website_with_ddgs(denomination: str) -> tuple[str, str | None, int | None]:
    """Search DuckDuckGo for a company website via the ddgs API (no browser).

    Strategy:
    1. Search ``<denomination> fr`` — build ranked candidate list.
    2. If no candidates, retry with ``<denomination> nautisme`` as fallback.
    3. For each candidate (best TLD/rank first), verify it responds (HTTP < 400).
       The first accessible URL wins.

    The URL is always normalised to the root domain (scheme + host) so that
    paths like ``/en/`` are stripped — all targeted companies are in France.

    Args:
        denomination: Company legal name (denominationUniteLegale).

    Returns:
        Tuple ``(status, url, rank)`` where status is one of:
            - ``'TROUVÉ'``     — a matching URL was found and verified
            - ``'NON TROUVÉ'`` — no accessible match after both searches
            - ``'ERREUR'``     — ddgs raised an exception
    """
    logger.info("Searching for: '%s'", denomination)
    try:
        keywords = [
            word for word in re.split(r"[\s-]+", denomination)
            if word.lower() not in _STOP_WORDS and len(word) > 2
        ]

        # ── Pass 1 : "<denomination> fr" ────────────────────────────────────
        results = _ddgs_search(f"{denomination} fr")
        logger.debug("Pass 1 — %d results", len(results))
        candidates = _pick_best_candidate(results, keywords)

        # ── Pass 2 : "site officiel" — remonte le vrai site d'entreprise ─────
        # DDG rankera davantage le site propre qu'un annuaire pour cette requête.
        if not candidates:
            logger.info("No match in pass 1 — retrying with 'site officiel'.")
            results2 = _ddgs_search(f"{denomination} site officiel")
            logger.debug("Pass 2 — %d results", len(results2))
            candidates = _pick_best_candidate(results2, keywords)

        # ── Pass 3 : fallback with "nautisme" ────────────────────────────────
        if not candidates:
            logger.info("No match in pass 2 — retrying with 'nautisme' keyword.")
            results3 = _ddgs_search(f"{denomination} nautisme")
            logger.debug("Pass 3 — %d results", len(results3))
            candidates = _pick_best_candidate(results3, keywords)

        # ── Verify accessibility (first accessible candidate wins) ────────────
        for best_priority, best_rank, best_url in candidates:
            if _verify_url(best_url):
                logger.info(
                    "Best match for '%s': %s (TLD priority=%d, rank=%d)",
                    denomination, best_url, best_priority, best_rank,
                )
                return "TROUVÉ", best_url, best_rank
            logger.warning("URL not accessible, skipping: %s", best_url)

        logger.warning("No match found for '%s' after both search passes.", denomination)
        return "NON TROUVÉ", None, None

    except Exception as exc:
        logger.error("DDGS error for '%s': %s", denomination, exc, exc_info=True)
        return "ERREUR", None, None


# ============================================================================
# MAIN PROCESSING LOOP
# ============================================================================

def process_companies(
    config: FindWebsitesConfig,
) -> None:
    """Find websites for all companies in the input CSV and save results.

    Supports resuming: rows with a non-empty ``statut_recherche`` that is not
    ``'ERREUR'`` are skipped.  Results are written to disk after each row so
    that progress is never lost on interruption.

    Args:
        config: Validated :class:`FindWebsitesConfig` instance.
    """
    output_dir = config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    input_path = config.input_csv
    stem = input_path.stem
    output_path = output_dir / f"{stem}_websites.csv"

    logger.info(
        "Starting website search — input='%s', output='%s', limit=%s",
        input_path, output_path, config.limit,
    )

    try:
        df_input = pd.read_csv(input_path)
    except FileNotFoundError:
        logger.error("Input file not found: '%s'", input_path)
        return

    # ── Resume or start fresh ─────────────────────────────────────────────────
    if output_path.exists():
        logger.info("Resuming from existing results: '%s'", output_path)
        df_output = pd.read_csv(output_path)
        for col in ("site_web", "statut_recherche", "source_site_web"):
            if col not in df_output.columns:
                df_output[col] = ""
    else:
        logger.info("No existing results — starting from scratch.")
        df_output = df_input.copy()
        df_output["site_web"] = ""
        df_output["statut_recherche"] = ""
        df_output["source_site_web"] = ""

    for col in ("site_web", "statut_recherche", "source_site_web"):
        df_output[col] = df_output[col].fillna("")

    # ── Select rows to process ────────────────────────────────────────────────
    rows_to_process = df_output[df_output["statut_recherche"].isin(["", "ERREUR"])].copy()
    if config.limit:
        rows_to_process = rows_to_process.head(config.limit)

    # Always write the output file upfront — even if empty or fully processed.
    # This guarantees the pipeline's post-step existence check always passes.
    df_output.to_csv(output_path, index=False, encoding="utf-8")

    if rows_to_process.empty:
        logger.info("All companies already processed — nothing to do.")
        return

    logger.info("%d companies to process.", len(rows_to_process))

    try:
        for original_index, row in tqdm(
            rows_to_process.iterrows(),
            total=len(rows_to_process),
            desc="Finding websites",
        ):
            denomination = row["denominationUniteLegale"]
            status, website, rank = get_website_with_ddgs(denomination)

            df_output.loc[original_index, "statut_recherche"] = status
            df_output.loc[original_index, "site_web"] = website if status == "TROUVÉ" else ""
            df_output.loc[original_index, "source_site_web"] = (
                f"DDG Rank {rank}" if status == "TROUVÉ" else ""
            )

            # Save after every row to preserve progress
            df_output.to_csv(output_path, index=False, encoding="utf-8")
            logger.debug("Saved progress → '%s'", output_path)

            time.sleep(random.uniform(3, 8))

    except KeyboardInterrupt:
        logger.warning("Interrupted by user — progress saved to '%s'.", output_path)
        sys.exit(0)
    except Exception as exc:
        logger.critical(
            "Fatal error during processing: %s — progress saved.", exc, exc_info=True
        )
        sys.exit(1)

    logger.info("Processing complete — results saved to '%s'.", output_path)


# ============================================================================
# CLI
# ============================================================================

@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("input_csv", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False),
    default="Results",
    show_default=True,
    help="Dossier où sauvegarder le CSV de sortie.",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Limiter le nombre d'entreprises traitées (tests).",
)
def main(input_csv: str, output_dir: str, limit: int | None) -> None:
    """Find company websites via DuckDuckGo API (ddgs, no browser).

    INPUT_CSV is the filtered companies CSV produced by prospect_analyzer.py.
    """
    setup_pipeline_logging(log_dir="Logs", sector_name="find_websites")
    logger.info("find_websites.py started — input='%s'", input_csv)

    try:
        config = FindWebsitesConfig(
            input_csv=Path(input_csv),
            output_dir=Path(output_dir),
            limit=limit,
        )
    except ValidationError as exc:
        click.echo(f"Erreur de configuration :\n{exc}", err=True)
        sys.exit(1)

    process_companies(config)


if __name__ == "__main__":
    main()
