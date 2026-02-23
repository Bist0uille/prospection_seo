#!/usr/bin/env python3
"""
Site health checker — classifie les entreprises par besoin web réel.

Part de filtered_companies_websites.csv (toutes les entreprises du secteur,
avec ou sans site trouvé) et produit un rapport priorisé :

  1. PAS DE SITE   — aucun site trouvé lors de la recherche           (signal #1)
  2. DOWN          — site trouvé mais inaccessible                    (signal #2)
  3. LENT          — site accessible mais > seuil ms                  (signal #3)
  4. SITE ANCIEN   — site up mais copyright ≤ 2 ans avant aujourd'hui (signal #4)
  5. SANS BLOG     — site ok mais aucun contenu/blog détecté          (signal #5)
  6. OK            — aucun problème identifié

Modificateurs commerciaux (affectent le tri mais pas le signal principal) :
  - agence_detectee  → pousse vers le bas dans sa catégorie
  - annee_copyright  → affiché dans le rapport
  - reseaux_sociaux  → Facebook, Instagram, LinkedIn… détectés en footer/page

Filtre géographique : --departements 17,33 filtre par les 2 premiers
chiffres du code postal (département INSEE).

Usage :
  python Scripts/site_health_checker.py Results/nautisme/filtered_companies_websites.csv
  python Scripts/site_health_checker.py Results/nautisme/filtered_companies_websites.csv --departements 17,33
  python Scripts/site_health_checker.py Results/nautisme/filtered_companies_websites.csv -o Results/nautisme/health.csv
  python Scripts/site_health_checker.py Results/nautisme/filtered_companies_websites.csv --slow-threshold 5000
"""

from __future__ import annotations

import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import click
import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from Scripts.core.logging_config import get_logger, setup_pipeline_logging
from Scripts.seo_auditor import (
    BLOG_URL_PATTERNS,
    HEADERS,
    _detect_blog_in_nav,
    _detect_rss,
)

logger = get_logger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
DEFAULT_SLOW_THRESHOLD_MS: int = 3000
REQUEST_TIMEOUT_S: int = 10
SITE_ANCIEN_YEARS: int = 2          # copyright < aujourd'hui - N ans → "site ancien"

# ── Priorité des signaux (ordre croissant = moins urgent) ────────────────────
PRIORITY: dict[str, int] = {
    "pas_de_site":  1,
    "down":         2,
    "lent":         3,
    "site_ancien":  4,
    "sans_blog":    5,
    "ok":           6,
}

# ── Réseaux sociaux détectés ──────────────────────────────────────────────────
_SOCIAL_DOMAINS: dict[str, str] = {
    "facebook.com":  "Facebook",
    "instagram.com": "Instagram",
    "linkedin.com":  "LinkedIn",
    "twitter.com":   "Twitter",
    "x.com":         "X",
    "youtube.com":   "YouTube",
    "tiktok.com":    "TikTok",
}

# ── Patterns textuels d'attribution d'agence ─────────────────────────────────
# Cherchés dans le footer et les commentaires HTML.
# Groupe 1 capturé = nom de l'agence quand disponible.
_AGENCY_TEXT_PATTERNS: list[re.Pattern] = [
    re.compile(r'r[eé]alis[eé]\s+par\s+["\']?([^<"\'\n,\.]{3,50})', re.I),
    re.compile(r'cr[eé][eé]\s+par\s+["\']?([^<"\'\n,\.]{3,50})', re.I),
    re.compile(r'd[eé]velopp[eé]\s+par\s+["\']?([^<"\'\n,\.]{3,50})', re.I),
    re.compile(r'design\s+(?:by|par)\s+["\']?([^<"\'\n,\.]{3,50})', re.I),
    re.compile(r'conception\s*(?::|by|par)\s*["\']?([^<"\'\n,\.]{3,50})', re.I),
    re.compile(r'int[eé]gr[eé]\s+par\s+["\']?([^<"\'\n,\.]{3,50})', re.I),
    re.compile(r'une\s+cr[eé]ation\s+["\']?([^<"\'\n,\.]{3,50})', re.I),
    re.compile(r'agence\s+(?:web\s+)?([A-Z][a-zA-Z0-9\s\-]{2,30})', re.I),
    re.compile(r'studio\s+([A-Z][a-zA-Z0-9\s\-]{2,30})', re.I),
    re.compile(r'powered\s+by\s+([^<"\'\n,\.]{3,40})', re.I),
]

# Domaines / mots dans les liens footer qui indiquent une agence
_AGENCY_LINK_KEYWORDS: tuple[str, ...] = (
    "agence", "studio", "webdesign", "web-design", "creation-site",
    "creationsite", "digitale", "communication", "marketing",
)

# Noms qui déclenchent un faux positif "powered by" — plugins, CMS, frameworks
_AGENCY_FALSE_POSITIVES: frozenset[str] = frozenset({
    "complianz", "wordpress", "woocommerce", "prestashop", "shopify",
    "wix", "squarespace", "webflow", "joomla", "drupal", "typo3",
    "bootstrap", "jquery", "php", "apache", "nginx", "iis",
    "google", "cloudflare", "stripe", "paypal", "hubspot",
    "cookiebot", "axeptio", "tarteaucitron", "onetrust", "didomi",
    "gdpr", "ccpa", "rgpd",
})

# Année courante pour le calcul d'ancienneté
_CURRENT_YEAR: int = datetime.now().year
_COPYRIGHT_RE = re.compile(
    r'(?:©|&copy;|copyright)\s*(?:\d{4}\s*[-–]\s*)?(\d{4})', re.I
)


# ============================================================================
# GEOGRAPHIC FILTER
# ============================================================================

def _departement(code_postal: str | float) -> str | None:
    s = str(code_postal).strip().replace(".0", "")
    return s[:2] if len(s) >= 2 else None


def filter_by_departements(df: pd.DataFrame, departements: list[str]) -> pd.DataFrame:
    if not departements or "codePostalEtablissement" not in df.columns:
        return df
    mask = df["codePostalEtablissement"].apply(
        lambda cp: _departement(cp) in departements
    )
    filtered = df[mask].copy()
    logger.info(
        "Filtre géographique (%s) : %d → %d entreprises",
        "+".join(departements), len(df), len(filtered),
    )
    return filtered


# ============================================================================
# COMMERCIAL SIGNAL DETECTORS  (opèrent sur BeautifulSoup + html brut)
# ============================================================================

def _detect_agency(soup: BeautifulSoup, html: str) -> tuple[bool, str | None, str | None]:
    """Détecte si le site mentionne une agence web créatrice.

    Recherche dans :
    1. Texte du footer (balise <footer> ou div.footer)
    2. Commentaires HTML
    3. Liens sortants dont le domaine/texte évoque une agence

    Returns:
        (agence_detectee, nom_agence_ou_None, url_agence_ou_None)
    """
    # ── Zone footer ───────────────────────────────────────────────────────────
    footer_el = soup.find("footer")
    if footer_el is None:
        footer_el = soup.find(
            "div", class_=re.compile(r"footer|bas-?de-?page|bottom", re.I)
        )
    # Fallback : derniers 3 000 caractères du HTML brut
    footer_text = footer_el.get_text(" ", strip=True) if footer_el else ""
    footer_html = str(footer_el) if footer_el else html[-3000:]

    # Texte à scanner = footer + commentaires HTML
    scan_zones = [footer_text, footer_html]
    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        scan_zones.append(str(comment))

    for zone in scan_zones:
        for pattern in _AGENCY_TEXT_PATTERNS:
            m = pattern.search(zone)
            if m:
                name = m.group(1).strip().rstrip(" .,;")
                # Éviter les faux positifs trop courts, génériques ou connus
                if len(name) < 3:
                    continue
                if name.lower() in ("nous", "votre", "notre", "cette", "un", "une"):
                    continue
                # Exclure les plugins, CMS et outils connus
                name_lower = name.lower()
                if any(fp in name_lower for fp in _AGENCY_FALSE_POSITIVES):
                    logger.debug("Agency false positive skipped: '%s'", name)
                    continue
                logger.debug("Agency detected via text pattern: '%s'", name)
                return True, name, None

    # ── Liens sortants suspects dans le footer ────────────────────────────────
    check_el = footer_el or soup
    for a in check_el.find_all("a", href=True):
        href  = a.get("href", "").lower()
        atext = a.get_text(strip=True).lower()
        if any(kw in href or kw in atext for kw in _AGENCY_LINK_KEYWORDS):
            # Exclure les liens vers soi-même et les liens de contact génériques
            if "contact" not in href and "mailto" not in href:
                raw_href = a["href"]
                agency_url = (
                    raw_href if raw_href.startswith("http")
                    else f"https:{raw_href}" if raw_href.startswith("//")
                    else None
                )
                domain = urlparse(raw_href).netloc.replace("www.", "")
                name   = a.get_text(strip=True) or domain
                logger.debug("Agency detected via footer link: '%s' (%s)", name, raw_href)
                return True, name[:60] if name else None, agency_url

    return False, None, None


def _detect_copyright_year(soup: BeautifulSoup, html: str) -> int | None:
    """Extrait la dernière année de copyright depuis le footer.

    Returns:
        Année (int) ou None si non trouvée.
    """
    footer_el = soup.find("footer")
    search_in = str(footer_el) if footer_el else html[-2000:]
    matches = _COPYRIGHT_RE.findall(search_in)
    if not matches:
        return None
    years = [int(y) for y in matches if 1990 <= int(y) <= _CURRENT_YEAR + 1]
    return max(years) if years else None


def _detect_social_links(soup: BeautifulSoup) -> dict[str, str]:
    """Détecte les liens réseaux sociaux présents sur la page.

    Returns:
        Dict plateforme → URL (ex: {'Facebook': 'https://facebook.com/page'}).
        En cas de doublons, garde le premier lien trouvé.
    """
    found: dict[str, str] = {}
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        href_lower = href.lower()
        for domain, name in _SOCIAL_DOMAINS.items():
            if domain in href_lower and name not in found:
                # Garder l'URL complète (pas juste le domaine racine)
                found[name] = href if href.startswith("http") else f"https:{href}" if href.startswith("//") else href
    return found


# ============================================================================
# SITE HEALTH CHECK
# ============================================================================

def check_site(url: str, slow_threshold_ms: int = DEFAULT_SLOW_THRESHOLD_MS) -> dict:
    """Vérifie accessibilité, vitesse, blog, agence, copyright, réseaux sociaux.

    Returns:
        Dict avec : is_down, down_reason, response_time_ms, is_slow,
        has_blog, blog_url, agence_detectee, agence_nom,
        annee_copyright, site_ancien, reseaux_sociaux.
    """
    result: dict = {
        "is_down":           False,
        "down_reason":       None,
        "response_time_ms":  None,
        "is_slow":           False,
        "has_blog":          False,
        "blog_url":          None,
        # ── commercial ──────────────────────────────────────
        "agence_detectee":   False,
        "agence_nom":        None,
        "agence_url":        None,
        "annee_copyright":   None,
        "site_ancien":       False,
        "reseaux_sociaux":   "",
    }

    if not url.startswith("http"):
        url = "https://" + url

    t0 = time.perf_counter()
    try:
        resp = requests.get(
            url,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT_S,
            allow_redirects=True,
        )
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        result["response_time_ms"] = elapsed_ms

        if resp.status_code >= 400:
            result["is_down"]     = True
            result["down_reason"] = f"HTTP {resp.status_code}"
            return result

        if elapsed_ms > slow_threshold_ms:
            result["is_slow"] = True

        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        # ── Blog ──────────────────────────────────────────────────────────────
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            for pattern in BLOG_URL_PATTERNS:
                if pattern in href:
                    result["has_blog"] = True
                    result["blog_url"] = a["href"]
                    break
            if result["has_blog"]:
                break

        if not result["has_blog"]:
            found, blog_url = _detect_blog_in_nav(soup, url)
            if found:
                result["has_blog"] = True
                result["blog_url"] = blog_url

        if not result["has_blog"] and _detect_rss(soup):
            result["has_blog"] = True
            result["blog_url"] = url

        # ── Agence ────────────────────────────────────────────────────────────
        agence, nom, agence_url = _detect_agency(soup, html)
        result["agence_detectee"] = agence
        result["agence_nom"]      = nom
        result["agence_url"]      = agence_url

        # ── Ancienneté (copyright) ────────────────────────────────────────────
        year = _detect_copyright_year(soup, html)
        result["annee_copyright"] = year
        if year and (_CURRENT_YEAR - year) >= SITE_ANCIEN_YEARS:
            result["site_ancien"] = True

        # ── Réseaux sociaux ───────────────────────────────────────────────────
        socials = _detect_social_links(soup)
        # Stockage : "Facebook|https://..., Instagram|https://..."
        result["reseaux_sociaux"] = ", ".join(
            f"{name}|{url}" for name, url in socials.items()
        )

    except requests.exceptions.ConnectionError:
        result["is_down"]     = True
        result["down_reason"] = "Connexion impossible (DNS/refusée)"

    except requests.exceptions.Timeout:
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        result["response_time_ms"] = elapsed_ms
        result["is_down"]          = True
        result["down_reason"]      = f"Timeout (>{REQUEST_TIMEOUT_S}s)"

    except Exception as exc:
        result["is_down"]     = True
        result["down_reason"] = str(exc)[:120]

    return result


# ============================================================================
# CLASSIFICATION
# ============================================================================

def _classify(check: dict | None) -> tuple[str, float]:
    """Retourne (signal, priorite_score) pour le tri.

    L'agence détectée n'influe plus sur le score — info uniquement.
    """
    if check is None:
        return "pas_de_site", float(PRIORITY["pas_de_site"])

    if check["is_down"]:
        return "down", float(PRIORITY["down"])
    if check["is_slow"]:
        return "lent", float(PRIORITY["lent"])
    if check.get("site_ancien"):
        return "site_ancien", float(PRIORITY["site_ancien"])
    if not check["has_blog"]:
        return "sans_blog", float(PRIORITY["sans_blog"])
    return "ok", float(PRIORITY["ok"])


# ============================================================================
# BATCH RUN
# ============================================================================

def run_health_check(
    input_path: str,
    output_path: str,
    slow_threshold_ms: int = DEFAULT_SLOW_THRESHOLD_MS,
    departements: list[str] | None = None,
) -> str:
    logger.info(
        "run_health_check('%s' → '%s', slow=%dms, depts=%s)",
        input_path, output_path, slow_threshold_ms, departements,
    )

    df = pd.read_csv(input_path)
    if departements:
        df = filter_by_departements(df, departements)

    total = len(df)
    logger.info("Entreprises à traiter : %d", total)

    has_url = (
        df["site_web"].notna()
        & (df["site_web"].astype(str).str.strip() != "")
        & (df["site_web"].astype(str) != "nan")
    )
    if "statut_recherche" in df.columns:
        has_url &= df["statut_recherche"].astype(str).str.upper() == "TROUVÉ"

    with_site    = df[has_url].copy()
    without_site = df[~has_url].copy()
    logger.info("  Avec site : %d | Sans site : %d", len(with_site), len(without_site))

    rows: list[dict] = []

    # ── Sans site ─────────────────────────────────────────────────────────────
    for _, row in without_site.iterrows():
        rows.append({
            "siren":            str(row.get("siren", "")).strip(),
            "entreprise":       str(row.get("denominationUniteLegale", "")).strip(),
            "ville":            str(row.get("libelleCommuneEtablissement", "")).strip(),
            "departement":      _departement(row.get("codePostalEtablissement", "")),
            "site_web":         "",
            "is_down":          False,
            "down_reason":      None,
            "response_time_ms": None,
            "is_slow":          False,
            "has_blog":         False,
            "blog_url":         None,
            "agence_detectee":  False,
            "agence_nom":       None,
            "agence_url":       None,
            "annee_copyright":  None,
            "site_ancien":      False,
            "reseaux_sociaux":  "",
            "signal":           "pas_de_site",
            "priorite_score":   float(PRIORITY["pas_de_site"]),
        })

    # ── Avec site ─────────────────────────────────────────────────────────────
    n_with = len(with_site)
    for i, (_, row) in enumerate(with_site.iterrows(), 1):
        url     = str(row["site_web"]).strip()
        company = str(row.get("denominationUniteLegale", "")).strip()
        logger.info("[%d/%d] %s — %s", i, n_with, company, url)

        check           = check_site(url, slow_threshold_ms=slow_threshold_ms)
        signal, p_score = _classify(check)

        logger.info(
            "  → %s | agence=%s%s| copyright=%s | social=%s",
            signal.upper(),
            check["agence_detectee"],
            f" ({check['agence_nom']}) " if check["agence_nom"] else " ",
            check["annee_copyright"],
            check["reseaux_sociaux"] or "—",
        )

        rows.append({
            "siren":            str(row.get("siren", "")).strip(),
            "entreprise":       company,
            "ville":            str(row.get("libelleCommuneEtablissement", "")).strip(),
            "departement":      _departement(row.get("codePostalEtablissement", "")),
            "site_web":         url,
            "is_down":          check["is_down"],
            "down_reason":      check["down_reason"],
            "response_time_ms": check["response_time_ms"],
            "is_slow":          check["is_slow"],
            "has_blog":         check["has_blog"],
            "blog_url":         check["blog_url"],
            "agence_detectee":  check["agence_detectee"],
            "agence_nom":       check["agence_nom"],
            "agence_url":       check["agence_url"],
            "annee_copyright":  check["annee_copyright"],
            "site_ancien":      check["site_ancien"],
            "reseaux_sociaux":  check["reseaux_sociaux"],
            "signal":           signal,
            "priorite_score":   p_score,
        })

    result_df = (
        pd.DataFrame(rows)
        .sort_values(["priorite_score", "entreprise"])
        .reset_index(drop=True)
    )
    result_df.to_csv(output_path, index=False)
    logger.info("CSV écrit : %s (%d lignes)", output_path, len(result_df))

    html_path = output_path.replace(".csv", ".html")
    _generate_html_report(result_df, html_path)
    logger.info("HTML : %s", html_path)

    counts = result_df["signal"].value_counts()
    n_agence = int(result_df["agence_detectee"].sum())
    logger.info("=" * 55)
    logger.info("  RÉSULTATS — %d entreprises", total)
    logger.info("  %-16s : %d", "Pas de site",   counts.get("pas_de_site", 0))
    logger.info("  %-16s : %d", "Down",           counts.get("down", 0))
    logger.info("  %-16s : %d", f"Lent (>{slow_threshold_ms//1000}s)", counts.get("lent", 0))
    logger.info("  %-16s : %d", "Site ancien",    counts.get("site_ancien", 0))
    logger.info("  %-16s : %d", "Sans blog",      counts.get("sans_blog", 0))
    logger.info("  %-16s : %d", "OK",             counts.get("ok", 0))
    logger.info("  %-16s : %d  (toutes catégories)", "Agence en place", n_agence)
    logger.info("=" * 55)

    return output_path


# ============================================================================
# HTML REPORT
# ============================================================================

_SIGNAL_LABELS: dict[str, tuple[str, str]] = {
    "pas_de_site": ("Pas de site",  "priority1"),
    "down":        ("Down",         "priority2"),
    "lent":        ("Lent",         "priority3"),
    "site_ancien": ("Site ancien",  "priority4"),
    "sans_blog":   ("Sans blog",    "priority5"),
    "ok":          ("OK",           "ok"),
}


def _generate_html_report(df: pd.DataFrame, output_path: str) -> None:
    counts   = df["signal"].value_counts()
    n_total  = len(df)
    n_nosite = counts.get("pas_de_site", 0)
    n_down   = counts.get("down", 0)
    n_slow   = counts.get("lent", 0)
    n_ancien = counts.get("site_ancien", 0)
    n_noblog = counts.get("sans_blog", 0)
    n_ok     = counts.get("ok", 0)
    n_agence = int(df["agence_detectee"].sum())

    rows_html: list[str] = []
    for _, row in df.iterrows():
        signal          = str(row.get("signal", "ok"))
        label, badge_cls = _SIGNAL_LABELS.get(signal, (signal, "ok"))
        company         = str(row.get("entreprise", "") or "").title()
        ville           = str(row.get("ville", "") or "").title()
        dept            = str(row.get("departement", "") or "")
        site            = str(row.get("site_web", "") or "")
        domain          = urlparse(site).netloc.replace("www.", "") if site else ""
        agence_det      = bool(row.get("agence_detectee"))
        agence_nom      = str(row.get("agence_nom") or "").strip()
        agence_url      = str(row.get("agence_url") or "").strip()
        annee_copy      = row.get("annee_copyright")
        reseaux         = str(row.get("reseaux_sociaux") or "").strip()

        # Lien site
        site_td = (
            f'<a href="{site}" target="_blank">{domain}</a>'
            if site else '<span class="na">—</span>'
        )

        # Temps réponse
        rt = row.get("response_time_ms")
        if rt is None or str(rt) in ("nan", "None", ""):
            rt_html = '<span class="na">—</span>'
        else:
            rt_int  = int(float(rt))
            color   = "#c62828" if rt_int > 3000 else ("#e65100" if rt_int > 1500 else "#388e3c")
            rt_html = f'<span style="color:{color};font-weight:600">{rt_int} ms</span>'

        # Down reason
        down_reason = str(row.get("down_reason") or "").strip()
        down_td = (
            f'<span class="down-reason">{down_reason}</span>'
            if down_reason and down_reason not in ("nan", "None")
            else '<span class="na">—</span>'
        )

        # Blog
        blog_url = str(row.get("blog_url") or "").strip()
        if bool(row.get("has_blog")) and blog_url and blog_url not in ("nan", "None"):
            blog_td = f'<span class="chip chip-ok">Oui</span> <a class="blog-link" href="{blog_url}" target="_blank">↗</a>'
        elif bool(row.get("has_blog")):
            blog_td = '<span class="chip chip-ok">Oui</span>'
        elif signal == "pas_de_site":
            blog_td = '<span class="na">—</span>'
        else:
            blog_td = '<span class="chip chip-bad">Non</span>'

        # Agence
        if agence_det:
            ag_label = agence_nom[:28] if agence_nom and agence_nom not in ("nan", "None") else "Oui"
            if agence_url and agence_url not in ("nan", "None"):
                agence_td = f'<a href="{agence_url}" target="_blank" class="chip chip-agence" title="{agence_nom}">{ag_label} ↗</a>'
            else:
                agence_td = f'<span class="chip chip-agence" title="{agence_nom}">{ag_label}</span>'
        else:
            agence_td = '<span class="na">—</span>'

        # Copyright / ancienneté
        if annee_copy and str(annee_copy) not in ("nan", "None", ""):
            age = _CURRENT_YEAR - int(float(annee_copy))
            copy_color = "#c62828" if age >= SITE_ANCIEN_YEARS else "#388e3c"
            copy_td = f'<span style="color:{copy_color};font-weight:600">© {int(float(annee_copy))}</span>'
            if age >= SITE_ANCIEN_YEARS:
                copy_td += f' <span class="chip chip-bad" style="font-size:10px">{age} ans</span>'
        else:
            copy_td = '<span class="na">—</span>'

        # Réseaux sociaux — format "Nom|URL, Nom|URL"
        if reseaux and reseaux not in ("nan", "None"):
            chips = []
            for entry in reseaux.split(", "):
                if "|" in entry:
                    plat, surl = entry.split("|", 1)
                    chips.append(
                        f'<a href="{surl}" target="_blank" class="chip chip-social">{plat}</a>'
                    )
                else:
                    chips.append(f'<span class="chip chip-social">{entry}</span>')
            social_td = " ".join(chips)
        else:
            social_td = '<span class="na">—</span>'

        # data-agence pour filtre JS
        data_agence = "1" if agence_det else "0"

        rows_html.append(f"""
    <tr data-signal="{signal}" data-agence="{data_agence}">
      <td class="center"><span class="badge {badge_cls}">{label}</span></td>
      <td class="name">{company}</td>
      <td class="geo">{ville} <span class="dept">{dept}</span></td>
      <td class="url">{site_td}</td>
      <td class="center">{rt_html}</td>
      <td class="center">{blog_td}</td>
      <td class="center">{agence_td}</td>
      <td class="center">{copy_td}</td>
      <td class="center">{social_td}</td>
      <td class="center">{down_td}</td>
    </tr>""")

    all_rows = "\n".join(rows_html)

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<title>Site Health Check — Prospects Web</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         background: #f5f5f5; color: #212121; padding: 32px; }}
  .header h1 {{ font-size: 22px; font-weight: 700; color: #1a237e; }}
  .header p  {{ font-size: 13px; color: #757575; margin-top: 4px; }}

  .meta {{ display: flex; gap: 14px; margin-top: 18px; flex-wrap: wrap; margin-bottom: 22px; }}
  .meta-card {{
    background: #fff; padding: 12px 18px; border-radius: 8px;
    box-shadow: 0 1px 4px rgba(0,0,0,.1); min-width: 110px;
    border-top: 4px solid #ccc; cursor: pointer; transition: box-shadow .15s;
    user-select: none;
  }}
  .meta-card:hover {{ box-shadow: 0 3px 10px rgba(0,0,0,.18); }}
  .meta-card.active {{ box-shadow: 0 0 0 2px #1a237e, 0 3px 10px rgba(0,0,0,.15); }}
  .meta-card .num {{ font-size: 26px; font-weight: 800; }}
  .meta-card .lbl {{ font-size: 10px; text-transform: uppercase; letter-spacing: .5px; color: #757575; margin-top: 2px; }}
  .meta-card.p1  {{ border-top-color: #b71c1c; }} .meta-card.p1  .num {{ color: #b71c1c; }}
  .meta-card.p2  {{ border-top-color: #c62828; }} .meta-card.p2  .num {{ color: #c62828; }}
  .meta-card.p3  {{ border-top-color: #e65100; }} .meta-card.p3  .num {{ color: #e65100; }}
  .meta-card.p4  {{ border-top-color: #f9a825; }} .meta-card.p4  .num {{ color: #e65100; }}
  .meta-card.p5  {{ border-top-color: #1565c0; }} .meta-card.p5  .num {{ color: #1565c0; }}
  .meta-card.pok {{ border-top-color: #388e3c; }} .meta-card.pok .num {{ color: #388e3c; }}
  .meta-card.all {{ border-top-color: #757575; }} .meta-card.all .num {{ color: #424242; }}
  .meta-card.pag {{ border-top-color: #7b1fa2; }} .meta-card.pag .num {{ color: #7b1fa2; }}

  .filters {{ display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 16px; align-items: center; }}
  .filters label {{ font-size: 12px; color: #555; }}
  .filters input[type=checkbox] {{ margin-right: 4px; }}

  table {{ width: 100%; border-collapse: collapse; background: #fff;
           box-shadow: 0 1px 4px rgba(0,0,0,.12); border-radius: 8px; overflow: hidden; }}
  thead th {{ background: #1a237e; color: #fff; font-size: 10px; font-weight: 600;
              text-transform: uppercase; letter-spacing: .5px; padding: 11px 12px; text-align: left; }}
  tbody tr {{ border-bottom: 1px solid #f0f0f0; transition: background .1s; }}
  tbody tr:hover {{ background: #fafafa; }}
  tbody tr.hidden {{ display: none; }}
  tbody td {{ padding: 9px 12px; font-size: 12px; vertical-align: middle; }}
  .name {{ font-weight: 600; font-size: 13px; }}
  .geo  {{ font-size: 11px; color: #555; white-space: nowrap; }}
  .dept {{ display: inline-block; background: #eeeeee; color: #424242;
           padding: 1px 5px; border-radius: 8px; font-size: 10px; font-weight: 700; margin-left: 3px; }}
  .url a {{ color: #1565c0; text-decoration: none; font-size: 12px; }}
  .url a:hover {{ text-decoration: underline; }}
  .center {{ text-align: center; }}
  .na {{ color: #bdbdbd; font-size: 11px; }}
  .down-reason {{ color: #b71c1c; font-size: 11px; }}
  .blog-link {{ color: #1565c0; text-decoration: none; font-size: 11px; margin-left: 3px; }}

  /* Badges signal principal */
  .badge {{ display: inline-block; padding: 3px 9px; border-radius: 12px;
            font-size: 11px; font-weight: 700; white-space: nowrap; }}
  .priority1 {{ background: #ffcdd2; color: #b71c1c; }}
  .priority2 {{ background: #ffebee; color: #c62828; }}
  .priority3 {{ background: #fff3e0; color: #e65100; }}
  .priority4 {{ background: #fff8e1; color: #f57f17; }}
  .priority5 {{ background: #e3f2fd; color: #1565c0; }}
  .ok        {{ background: #e8f5e9; color: #2e7d32; }}

  /* Chips inline */
  .chip {{ display: inline-block; padding: 1px 7px; border-radius: 10px;
           font-size: 10px; font-weight: 600; white-space: nowrap; }}
  .chip-ok     {{ background: #e8f5e9; color: #2e7d32; }}
  .chip-bad    {{ background: #ffebee; color: #c62828; }}
  .chip-agence {{ background: #f3e5f5; color: #7b1fa2; max-width: 140px;
                  overflow: hidden; text-overflow: ellipsis; display: inline-block;
                  text-decoration: none; }}
  a.chip-agence:hover {{ background: #e1bee7; }}
  .chip-social {{ background: #e8eaf6; color: #283593; text-decoration: none; }}
  a.chip-social:hover {{ background: #c5cae9; text-decoration: none; }}
</style>
</head>
<body>

<div class="header">
  <h1>Site Health Check — Prospects Web</h1>
  <p>Analyse complète · {n_total} entreprises · signaux commerciaux + techniques</p>
</div>

<div class="meta">
  <div class="meta-card all" data-filter="all" data-agence="all">
    <div class="num">{n_total}</div><div class="lbl">Total</div>
  </div>
  <div class="meta-card p1" data-filter="pas_de_site" data-agence="all">
    <div class="num">{n_nosite}</div><div class="lbl">Pas de site</div>
  </div>
  <div class="meta-card p2" data-filter="down" data-agence="all">
    <div class="num">{n_down}</div><div class="lbl">Down</div>
  </div>
  <div class="meta-card p3" data-filter="lent" data-agence="all">
    <div class="num">{n_slow}</div><div class="lbl">Lents</div>
  </div>
  <div class="meta-card p4" data-filter="site_ancien" data-agence="all">
    <div class="num">{n_ancien}</div><div class="lbl">Site ancien</div>
  </div>
  <div class="meta-card p5" data-filter="sans_blog" data-agence="all">
    <div class="num">{n_noblog}</div><div class="lbl">Sans blog</div>
  </div>
  <div class="meta-card pok" data-filter="ok" data-agence="all">
    <div class="num">{n_ok}</div><div class="lbl">OK</div>
  </div>
  <div class="meta-card pag" data-filter="all" data-agence="1">
    <div class="num">{n_agence}</div><div class="lbl">Agence en place</div>
  </div>
</div>

<div class="filters">
  <label><input type="checkbox" id="hide-agence"> Masquer les entreprises avec agence en place</label>
</div>

<table>
<thead>
<tr>
  <th>Signal</th>
  <th>Entreprise</th>
  <th>Ville</th>
  <th>Site web</th>
  <th>Temps rép.</th>
  <th>Blog</th>
  <th>Agence</th>
  <th>Copyright</th>
  <th>Réseaux</th>
  <th>Raison down</th>
</tr>
</thead>
<tbody>
{all_rows}
</tbody>
</table>

<script>
  const cards    = document.querySelectorAll('.meta-card');
  const rows     = document.querySelectorAll('tbody tr');
  const hideBox  = document.getElementById('hide-agence');

  let activeFilter = 'all';
  let hideAgence   = false;

  function applyFilters() {{
    rows.forEach(row => {{
      const signalMatch = activeFilter === 'all' || row.dataset.signal === activeFilter;
      const agenceMatch = !hideAgence || row.dataset.agence !== '1';
      row.classList.toggle('hidden', !(signalMatch && agenceMatch));
    }});
  }}

  cards.forEach(card => {{
    card.addEventListener('click', () => {{
      cards.forEach(c => c.classList.remove('active'));
      card.classList.add('active');
      const agenceFilter = card.dataset.agence;
      if (agenceFilter === '1') {{
        activeFilter = 'all';
        hideAgence = false;
        hideBox.checked = false;
        // Show only agence rows
        rows.forEach(row => {{
          row.classList.toggle('hidden', row.dataset.agence !== '1');
        }});
        return;
      }}
      activeFilter = card.dataset.filter;
      applyFilters();
    }});
  }});

  hideBox.addEventListener('change', () => {{
    hideAgence = hideBox.checked;
    applyFilters();
  }});

  document.querySelector('.meta-card.all').classList.add('active');
</script>
</body>
</html>"""

    Path(output_path).write_text(html, encoding="utf-8")
    logger.info("HTML report written: %s", output_path)


# ============================================================================
# CLI
# ============================================================================

@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("input_csv", type=click.Path(exists=True, dir_okay=False))
@click.option("--output", "-o", type=click.Path(dir_okay=False), default=None)
@click.option("--slow-threshold", type=int, default=DEFAULT_SLOW_THRESHOLD_MS, show_default=True)
@click.option("--departements", type=str, default=None,
              help="Codes département séparés par virgule (ex: 17,33).")
def main(input_csv, output, slow_threshold, departements):
    """Site health checker — classe les entreprises par besoin web réel.

    \b
    Exemples :
      python Scripts/site_health_checker.py Results/nautisme/filtered_companies_websites.csv
      python Scripts/site_health_checker.py Results/nautisme/filtered_companies_websites.csv --departements 17,33
    """
    input_path = Path(input_csv)
    if output is None:
        output = str(input_path.parent / "site_health.csv")

    dept_list = [d.strip() for d in departements.split(",")] if departements else None

    setup_pipeline_logging(log_dir="Logs", sector_name="site_health_checker")
    run_health_check(input_csv, output, slow_threshold_ms=slow_threshold, departements=dept_list)


if __name__ == "__main__":
    main()
