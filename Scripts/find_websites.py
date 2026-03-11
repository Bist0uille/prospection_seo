#!/usr/bin/env python3
"""
Website discovery via DuckDuckGo search API (ddgs).

For each company in the input CSV, queries the DuckDuckGo API (no browser
needed), validates the result with multi-signal confidence scoring, and saves
a URL when a match is found with confirmed sector content.

Strategy (passes in order):
  0 — Direct URL guessing (no search engine)
  1 — "{alias}"       — trade name between parentheses, if present
  2 — "{nom}"         — legal name
  3 — "{nom} {commune}"
  4 — "{nom} {secteur}"

Acceptance criteria:
  - conf >= CONFIDENCE_THRESHOLD (2.5)
  - secteur_ok == True (at least one sector keyword in page title/h1/p)

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
from urllib.parse import urlparse

import click
import pandas as pd
import requests
from pydantic import ValidationError
from tqdm import tqdm

# ── Project root on sys.path ──────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from Scripts.core.logging_config import get_logger, setup_pipeline_logging
from Scripts.core.models import FindWebsitesConfig

logger = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

CONFIDENCE_THRESHOLD = 2.5

DIRECTORY_DOMAINS: set[str] = {
    "societe.com", "pagesjaunes.fr", "pappers.fr",
    "annuaire-entreprises.data.gouv.fr", "verif.com",
    "entreprises.lefigaro.fr", "fr.kompass.com", "facebook.com",
    "linkedin.com", "youtube.com", "wikipedia.org", "doctrine.fr",
    "app.dataprospects.fr", "service-de-reparation-de-bateaux.autour-de-moi.com",
    "entreprises.lagazettefrance.fr", "reseauexcellence.fr", "actunautique.com",
    "autour-de-moi.tel", "autour-de-moi.com",
    "nous-larochelle.fr", "nous-bordeaux.fr",
    "investinbordeaux.fr", "portail-nautisme.fr",
    "bateauavendre.fr", "seapolelarochelle.com",
    "chateau.fr", "chateaux-france.com", "bordeaux.guides.winefolly.com",
    # Annuaires génériques
    "compagnie.com", "annuairefrancais.fr", "annuaire.fr", "123annuaire.com",
    "infonet.fr", "manageo.fr", "societe.ninja", "score3.fr",
    "e-pro.fr", "lemarche.fr", "entreprises.annuairefrancais.fr",
    "journalmarinemarchande.fr", "cbnews.fr",
    # Annuaires financiers / juridiques
    "infogreffe.fr", "bodacc.fr", "kashe.fr", "dirigeant.com", "bilan-gratuit.fr",
}

_DOMAIN_NOISE_WORDS: set[str] = {
    "bordeaux", "larochelle", "nantes", "brest", "toulon", "marseille",
    "gironde", "charente", "atlantique", "arcachon",
    "maritime", "nautique", "nautisme", "yachting", "bateau", "bateaux",
    "port", "mer", "ocean",
    "chateau", "vignobles", "vignoble", "domaine", "domaines", "vigne", "vignes",
    "wine", "vins", "vin",
    "france", "french", "groupe", "group", "services", "service",
    "industrie", "industries", "invest", "solutions",
}

_STOP_WORDS: set[str] = {"sa", "sas", "sarl", "eurl", "snc", "ste", "et", "de", "la", "les", "des"}

# Vocabulaire sectoriel — normalisé (minuscules, sans accents/tirets/espaces).
_SECTEUR_KEYWORDS: set[str] = {
    # Embarcations (FR)
    "bateau", "bateaux", "voilier", "voiliers", "yacht", "yachts", "yachting",
    "catamaran", "trimaran", "deriveur", "horsbord", "jetski", "kayak",
    "paddle", "canoe", "pirogue", "chaloupe", "annexe", "zodiac", "pneumatique",
    # Activité nautique (FR)
    "nautisme", "nautique", "plaisance", "plaisancier", "navigation", "naviguer",
    "croisiere", "regate", "regates", "regatier", "skipper",
    "charter", "plongee", "surf", "kitesurf",
    # Construction / réparation (FR)
    "chantiernaval", "chantier", "carene", "coque", "composite", "refit",
    "sellerie", "greement", "accastillage", "grement", "voilerie",
    # Maritime / marin (FR)
    "marin", "marine", "maritime", "armateur", "armement",
    # Transport / location (FR)
    "fluvial", "fluviale", "location", "loueur", "affreter",
    "transport", "ferry", "traversee",
    # Équipements (FR)
    "moteur", "helice", "gouvernail", "derive", "ancre",
    "inox", "antifouling", "carburant",
    # Port / infrastructure (FR)
    "port", "marina", "capitainerie", "cale", "quai", "ponton",
    # Embarcations (EN)
    "sailing", "sailboat", "powerboat", "motorboat", "dinghy", "outboard",
    "rib", "inflatable", "vessel", "boat", "boats", "ship",
    # Activité nautique (EN)
    "boating", "seafaring", "offshore", "regatta", "racing", "cruising",
    "diving", "watersport", "watersports",
    # Construction / réparation (EN)
    "boatyard", "shipyard", "boatbuilding", "hull", "rigging", "chandlery",
    "repower", "fiberglass", "gelcoat",
    # Maritime (EN)
    "nautical", "seafarer", "mariner",
    # Transport / location (EN)
    "rental", "hire", "bareboat",
    # Équipements (EN)
    "propeller", "rudder", "mast", "keel", "anchor", "winch", "furler",
}

# Keywords additionnels par code NAF
_NAF_EXTRA_KEYWORDS: dict[str, set[str]] = {
    "3315Z": {"reparation", "entretien", "maintenance", "refit", "soudure",
              "repair", "service", "servicing", "overhaul"},
    "3012Z": {"construction", "fabrication", "conception", "chantier", "composite",
              "boatbuilding", "manufacturing", "design"},
    "3011Z": {"construction", "fabrication", "naval", "navire",
              "shipbuilding", "vessel"},
    "5010Z": {"croisiere", "excursion", "traversee", "passagers", "ferry", "promenade",
              "cruise", "crossing", "passenger", "trip"},
    "5020Z": {"fret", "transport", "logistique", "fluvial",
              "freight", "cargo", "shipping"},
    "5222Z": {"manutention", "levage", "port", "capitainerie", "pilotage",
              "handling", "piloting", "harbour", "harbor"},
    "7734Z": {"location", "charter", "loueur", "louer",
              "rental", "hire", "bareboat"},
}

_VERIFY_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# ============================================================================
# HELPERS
# ============================================================================

def normalize_name(name: str) -> str:
    name = name.lower()
    return re.sub(r"[^a-z0-9]", "", name)


def _strip_to_root(url: str) -> str:
    try:
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}/"
    except Exception:
        return url


def _is_canadian(url: str) -> bool:
    try:
        domain = urlparse(url).netloc.lower().replace("www.", "")
        return domain.endswith(".ca")
    except Exception:
        return False


def _tld_priority(url: str) -> int:
    try:
        domain = urlparse(url).netloc.lower().replace("www.", "")
        return 0 if domain.endswith(".fr") else 1
    except Exception:
        return 1


# ============================================================================
# KEYWORD EXTRACTION
# ============================================================================

def _extract_keywords(denomination: str) -> list[str]:
    """Extrait les mots-clés pour le matching domaine.

    Accepte :
    - mots ≥ 4 chars (standard)
    - acronymes tout-majuscules ≥ 2 chars (AP, CIM, MSC…)
    """
    words = re.split(r"[\s\-_]+", denomination)
    keywords: list[str] = []
    for word in words:
        if word.lower() in _STOP_WORDS:
            continue
        if not re.search(r"[a-zA-Z0-9]", word):
            continue
        if re.fullmatch(r"[A-Z0-9]{2,}", word):
            keywords.append(word)
        elif len(word) >= 4:
            keywords.append(word)
    return keywords


def _extract_alias(denomination: str) -> str | None:
    """Extrait le nom commercial entre parenthèses.

    Exemples :
      "GUYMARINE (GUYMARINE)"                  → "GUYMARINE"
      "NAUTITECH CATAMARANS (CIM)"             → None  (≤3 chars)
      "EMILIEN FAURENS (FAURSAIL OU FAURENS)"  → "FAURSAIL"
    """
    m = re.search(r"\(([^)]+)\)", denomination)
    if not m:
        return None
    alias = m.group(1).strip()
    if " OU " in alias.upper():
        alias = re.split(r"\s+OU\s+", alias, flags=re.I)[0].strip()
    if len(alias) <= 3:
        return None
    return alias if alias else None


# ============================================================================
# SECTOR DETECTION
# ============================================================================

def _extract_snippet(html: str) -> str:
    """Extrait title + premier h1 + premier <p> ≥ 30 chars."""
    parts: list[str] = []
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    if m:
        text = re.sub(r"<[^>]+>", "", m.group(1)).strip()
        if text:
            parts.append(text[:120])
    m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.I | re.S)
    if m:
        text = re.sub(r"<[^>]+>", "", m.group(1)).strip()
        if text:
            parts.append(text[:120])
    for m in re.finditer(r"<p[^>]*>(.*?)</p>", html, re.I | re.S):
        text = re.sub(r"<[^>]+>", "", m.group(1)).strip()
        if len(text) >= 30:
            parts.append(text[:200])
            break
    return " | ".join(parts)[:400]


def _is_secteur_ok(snippet: str, naf_code: str = "") -> bool:
    """Retourne True si le snippet contient au moins un terme sectoriel."""
    normalized = normalize_name(snippet)
    keywords = _SECTEUR_KEYWORDS | _NAF_EXTRA_KEYWORDS.get(naf_code, set())
    return any(kw in normalized for kw in keywords)


# ============================================================================
# CONFIDENCE SCORING
# ============================================================================

def _compute_confidence(
    url: str,
    keywords: list[str],
    code_postal: str,
    commune: str,
    naf_code: str = "",
) -> tuple[float, bool, str]:
    """Calcule un score de confiance pour un couple (url, entreprise).

    Signaux :
      +2.0  keyword du nom dans le domaine
      +0.5  TLD .fr
      +1.5  code postal dans la page
      +1.0  commune dans la page

    Returns:
        (score, secteur_ok, snippet)
    """
    score = 0.0
    snippet = ""
    secteur_ok = False

    domain = urlparse(url).netloc.lower().replace("www.", "")

    if domain.endswith(".fr"):
        score += 0.5

    cleaned_domain = domain.replace(".", "").replace("-", "")
    active_kws = [kw for kw in keywords if normalize_name(kw) not in _DOMAIN_NOISE_WORDS]
    if active_kws and any(normalize_name(kw) in cleaned_domain for kw in active_kws):
        score += 2.0

    try:
        resp = requests.get(url, timeout=8, allow_redirects=True, headers=_VERIFY_HEADERS)

        if resp.status_code in {403, 429, 503}:
            logger.debug("Anti-bot (%d) sur %s — score domaine seul : %.1f", resp.status_code, url, score)
            return score, secteur_ok, snippet

        if resp.status_code in {404, 410} or resp.status_code >= 400:
            return 0.0, False, ""

        page_text = resp.text
        page_lower = page_text.lower()

        snippet = _extract_snippet(page_text)
        secteur_ok = _is_secteur_ok(snippet, naf_code)

        if code_postal and code_postal in page_lower:
            score += 1.5
            logger.debug("Code postal %s trouvé dans %s → +1.5", code_postal, url)

        if commune and normalize_name(commune) in normalize_name(page_lower):
            score += 1.0
            logger.debug("Commune '%s' trouvée dans %s → +1", commune, url)

    except requests.exceptions.ConnectionError:
        return 0.0, False, ""
    except Exception:
        pass

    return score, secteur_ok, snippet


# ============================================================================
# DIRECT URL GUESSING (pass 0)
# ============================================================================

_LEGAL_SUFFIXES = {
    "sa", "sas", "sarl", "eurl", "snc", "sca", "sci", "scp",
    "ste", "ets", "coop", "association", "assoc",
}


def _candidate_urls(search_name: str) -> list[str]:
    tokens = [
        t.lower() for t in re.split(r"[\s\-_]+", search_name)
        if t and t.lower() not in _LEGAL_SUFFIXES
        and re.search(r"[a-z0-9]", t.lower())
        and len(t) >= 2
    ]
    if not tokens:
        return []

    slugs: list[str] = []
    slugs.append("".join(tokens))
    if len(tokens) > 1:
        slugs.append("-".join(tokens))
    if len(tokens) > 1:
        slugs.append(tokens[-1])
    if len(tokens) > 2:
        slugs.append("".join(tokens[:2]))
        slugs.append("-".join(tokens[:2]))

    seen: set[str] = set()
    unique_slugs = [s for s in slugs if not (s in seen or seen.add(s))]  # type: ignore[func-returns-value]

    urls = []
    for slug in unique_slugs:
        for tld in (".fr", ".com"):
            urls.append(f"https://www.{slug}{tld}")
            urls.append(f"https://{slug}{tld}")
    return urls


def _verify_url_direct(url: str, keywords: list[str], timeout: int = 8) -> bool:
    """Vérifie qu'une URL directe est accessible et contient les keywords.

    Exige une réponse 200 : les 403/503 (anti-bot) ne sont plus acceptés
    pour éviter de valider un domaine aléatoire qui bloque les robots.
    """
    try:
        resp = requests.get(url, timeout=timeout, allow_redirects=True, headers=_VERIFY_HEADERS)
        if resp.status_code != 200:
            return False
        page_text = normalize_name(resp.text)
        return all(normalize_name(kw) in page_text for kw in keywords)
    except requests.exceptions.ConnectionError:
        return False
    except Exception:
        return False


def _try_direct_urls(search_name: str, keywords: list[str]) -> str | None:
    for url in _candidate_urls(search_name):
        if _verify_url_direct(url, keywords):
            return url
    return None


# ============================================================================
# SEARCH ENGINE — DDG + fallback SearXNG
# ============================================================================

_SEARXNG_INSTANCES = [
    "https://searx.be",
    "https://paulgo.io",
    "https://searxng.site",
    "https://search.mdosch.de",
]


def _searxng_search(query: str, max_results: int = 10) -> list[dict]:
    params = {"q": query, "format": "json", "engines": "google,bing,brave", "language": "fr-FR"}
    for instance in _SEARXNG_INSTANCES:
        try:
            resp = requests.get(f"{instance}/search", params=params, timeout=8, headers=_VERIFY_HEADERS)
            if resp.status_code != 200:
                continue
            raw = resp.json().get("results", [])[:max_results]
            if raw:
                return [{"href": r.get("url", ""), "title": r.get("title", ""), "body": r.get("content", "")} for r in raw]
        except Exception:
            continue
    return []


def _search(query: str, max_results: int = 10) -> list[dict]:
    try:
        from ddgs import DDGS as DDGS_NEW
        results = list(DDGS_NEW().text(query, max_results=max_results))
        if results:
            return results
    except ImportError:
        try:
            from duckduckgo_search import DDGS as DDGS_OLD
            results = list(DDGS_OLD().text(query, max_results=max_results))
            if results:
                return results
        except Exception:
            pass
    except Exception as exc:
        logger.debug("ddgs erreur (%s) → bascule SearXNG", exc)
    return _searxng_search(query, max_results)


def _filter_candidates(results: list[dict], keywords: list[str]) -> list[tuple[int, int, str]]:
    active_kws = [kw for kw in keywords if normalize_name(kw) not in _DOMAIN_NOISE_WORDS]
    candidates: list[tuple[int, int, str]] = []
    for rank, result in enumerate(results, 1):
        raw_url = result.get("href", "")
        if not raw_url:
            continue
        url = _strip_to_root(raw_url)
        domain = urlparse(url).netloc.replace("www.", "")
        cleaned_domain = domain.replace(".", "").replace("-", "")

        if domain in DIRECTORY_DOMAINS or "autour-de-moi" in domain:
            continue
        if _is_canadian(url):
            continue
        if not active_kws:
            continue
        if any(normalize_name(kw) in cleaned_domain for kw in active_kws):
            candidates.append((_tld_priority(url), rank, url))

    candidates.sort(key=lambda x: (x[0], x[1]))
    return candidates


# ============================================================================
# MAIN SEARCH FUNCTION
# ============================================================================

def get_website(
    denomination: str,
    code_postal: str = "",
    commune: str = "",
    sector_keyword: str = "france",
    naf_code: str = "",
) -> tuple[str, str | None, float, str]:
    """Recherche le site d'une entreprise avec scoring de confiance.

    Passes :
      0 — URL directe (devinée depuis le nom, sans moteur de recherche)
      1 — alias (nom commercial entre parenthèses, si présent)
      2 — {nom}
      3 — {nom} {commune}
      4 — {nom} {secteur}

    Returns:
        (statut, url, confiance, methode)
    """
    logger.info("Recherche : '%s' (CP=%s, commune=%s, NAF=%s)", denomination, code_postal, commune, naf_code)

    try:
        search_name = re.sub(r"\s*\(.*?\)", "", denomination).strip()
        keywords = _extract_keywords(denomination)
        alias = _extract_alias(denomination)

        # ── Pass 0 : URL directe ─────────────────────────────────────────────
        direct_url = _try_direct_urls(search_name, [kw for kw in keywords if len(kw) >= 4])
        if direct_url:
            # Vérifie aussi le secteur pour les URLs directes
            conf, secteur_ok, _ = _compute_confidence(direct_url, keywords, code_postal, commune, naf_code)
            if secteur_ok or conf >= 4.0:
                logger.info("Pass 0 (direct) → %s", direct_url)
                return "TROUVÉ", direct_url, conf, "direct"

        def _best(results: list[dict], pass_name: str, kws: list[str] | None = None) -> tuple[str, str, float, str] | None:
            effective_kws = kws if kws is not None else keywords
            for _, _, url in _filter_candidates(results, effective_kws):
                conf, secteur_ok, snippet = _compute_confidence(url, effective_kws, code_postal, commune, naf_code)
                if conf >= CONFIDENCE_THRESHOLD and secteur_ok:
                    return "TROUVÉ", url, conf, pass_name
                if conf >= CONFIDENCE_THRESHOLD:
                    logger.debug("Secteur non détecté pour %s (conf=%.1f) — rejeté", url, conf)
                else:
                    logger.debug("Confiance insuffisante (%.1f) pour %s", conf, url)
            return None

        # ── Pass 1 : alias ───────────────────────────────────────────────────
        if alias and alias.lower() != search_name.lower():
            alias_kws = _extract_keywords(alias) or keywords
            hit = _best(_search(alias), "DDG_alias", alias_kws)
            if hit:
                logger.info("Pass 1 alias '%s' → %s (conf=%.1f)", alias, hit[1], hit[2])
                return hit

        # ── Pass 2 : {nom} ───────────────────────────────────────────────────
        hit = _best(_search(search_name), "DDG_nom")
        if hit:
            logger.info("Pass 2 nom → %s (conf=%.1f)", hit[1], hit[2])
            return hit

        # ── Pass 3 : {nom} {commune} ─────────────────────────────────────────
        if commune:
            hit = _best(_search(f"{search_name} {commune}"), "DDG_commune")
            if hit:
                logger.info("Pass 3 commune → %s (conf=%.1f)", hit[1], hit[2])
                return hit

        # ── Pass 4 : {nom} {secteur} ─────────────────────────────────────────
        hit = _best(_search(f"{search_name} {sector_keyword}"), "DDG_secteur")
        if hit:
            logger.info("Pass 4 secteur → %s (conf=%.1f)", hit[1], hit[2])
            return hit

        logger.warning("Aucun site trouvé pour '%s'.", denomination)
        return "NON TROUVÉ", None, 0.0, ""

    except Exception as exc:
        logger.error("Erreur pour '%s' : %s", denomination, exc, exc_info=True)
        return "ERREUR", None, 0.0, ""


# ============================================================================
# MAIN PROCESSING LOOP
# ============================================================================

def process_companies(config: FindWebsitesConfig) -> None:
    """Cherche les sites pour toutes les entreprises du CSV d'entrée.

    Supporte la reprise : les lignes avec statut_recherche non vide (sauf ERREUR)
    sont ignorées. Résultats écrits après chaque ligne.
    """
    output_dir = config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    input_path = config.input_csv
    output_path = output_dir / f"{input_path.stem}_websites.csv"

    logger.info("Démarrage — input='%s', output='%s', limit=%s", input_path, output_path, config.limit)

    try:
        df_input = pd.read_csv(input_path)
    except FileNotFoundError:
        logger.error("Fichier introuvable : '%s'", input_path)
        return

    # ── Resume ou départ de zéro ──────────────────────────────────────────────
    if output_path.exists():
        logger.info("Reprise depuis : '%s'", output_path)
        df_output = pd.read_csv(output_path)
        for col in ("site_web", "statut_recherche", "source_site_web", "confiance", "secteur_ok"):
            if col not in df_output.columns:
                df_output[col] = ""
    else:
        df_output = df_input.copy()
        df_output["site_web"] = ""
        df_output["statut_recherche"] = ""
        df_output["source_site_web"] = ""
        df_output["confiance"] = ""
        df_output["secteur_ok"] = ""

    for col in ("site_web", "statut_recherche", "source_site_web", "confiance", "secteur_ok"):
        df_output[col] = df_output[col].fillna("")

    rows_to_process = df_output[df_output["statut_recherche"].isin(["", "ERREUR"])].copy()
    if config.limit:
        rows_to_process = rows_to_process.head(config.limit)

    df_output.to_csv(output_path, index=False, encoding="utf-8")

    if rows_to_process.empty:
        logger.info("Tout déjà traité — rien à faire.")
        return

    logger.info("%d entreprises à traiter.", len(rows_to_process))

    try:
        for idx, row in tqdm(rows_to_process.iterrows(), total=len(rows_to_process), desc="Recherche sites"):
            denomination = row["denominationUniteLegale"]
            code_postal = str(row.get("codePostalEtablissement", "")).strip()
            commune = str(row.get("libelleCommuneEtablissement", "")).strip()
            naf_code = str(row.get("activitePrincipaleUniteLegale", "")).strip()

            status, website, conf, methode = get_website(
                denomination, code_postal, commune, config.sector_keyword, naf_code
            )

            found = status == "TROUVÉ"
            df_output.loc[idx, "statut_recherche"] = status
            df_output.loc[idx, "site_web"] = website if found else ""
            df_output.loc[idx, "source_site_web"] = methode if found else ""
            df_output.loc[idx, "confiance"] = str(round(conf, 1)) if found else ""
            df_output.loc[idx, "secteur_ok"] = "True" if found else ""

            df_output.to_csv(output_path, index=False, encoding="utf-8")
            logger.debug("Sauvegardé → '%s'", output_path)

            time.sleep(random.uniform(1, 3))

    except KeyboardInterrupt:
        logger.warning("Interrompu — progression sauvegardée.")
        sys.exit(0)
    except Exception as exc:
        logger.critical("Erreur fatale : %s — progression sauvegardée.", exc, exc_info=True)
        sys.exit(1)

    logger.info("Terminé — résultats dans '%s'.", output_path)


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
@click.option(
    "--sector-keyword",
    type=str,
    default="france",
    show_default=True,
    help="Mot-clé secteur pour la passe 4 (ex: nautisme, vin, architecte).",
)
def main(input_csv: str, output_dir: str, limit: int | None, sector_keyword: str) -> None:
    """Recherche les sites web des entreprises via DuckDuckGo (ddgs, sans navigateur).

    INPUT_CSV est le CSV de filtered_companies produit par prospect_analyzer.py.
    """
    setup_pipeline_logging(log_dir="Logs", sector_name="find_websites")
    logger.info("find_websites.py démarré — input='%s'", input_csv)

    try:
        config = FindWebsitesConfig(
            input_csv=Path(input_csv),
            output_dir=Path(output_dir),
            limit=limit,
            sector_keyword=sector_keyword,
        )
    except ValidationError as exc:
        click.echo(f"Erreur de configuration :\n{exc}", err=True)
        sys.exit(1)

    process_companies(config)


if __name__ == "__main__":
    main()
