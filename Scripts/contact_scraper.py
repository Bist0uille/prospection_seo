#!/usr/bin/env python3
"""
Contact extraction — email + téléphone par site web.

Pour chaque prospect SEO identifié, extrait un point de contact direct
(email + téléphone) depuis les pages de contact et les footers des sites.

Stratégie en 2 passes :
  Passe 1 — requests (rapide, sans navigateur)
  Passe 2 — Playwright (fallback JS rendering si passe 1 sans résultat)

Standalone usage :
  python Scripts/contact_scraper.py Results/nautisme/final_prospect_report.csv
  python Scripts/contact_scraper.py Results/nautisme/final_prospect_report.csv -o out.csv
"""

from __future__ import annotations

import random
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import click
import pandas as pd
from bs4 import BeautifulSoup
from pydantic import ValidationError

# ── Project root on sys.path ──────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from Scripts.core.logging_config import get_logger, setup_pipeline_logging
from Scripts.core.models import ContactScraperConfig
from Scripts.seo_auditor import HEADERS, _safe_get

logger = get_logger(__name__)


# ============================================================================
# CONSTANTS
# ============================================================================

# Fallback paths tried when no contact link is found via nav crawl
CONTACT_PATHS: list[str] = [
    "/contact",
    "/nous-contacter",
    "/contactez-nous",
    "/contact-us",
    "/coordonnees",
]

# Keywords used to identify a "Contact" nav link on the homepage
_CONTACT_LINK_KEYWORDS: list[str] = [
    "contact", "nous contacter", "contactez", "coordonnées", "coordonnees",
    "nous joindre", "prendre contact", "joindre",
]

# Email regex: standard RFC-ish pattern
_EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
)

# Extensions and domains to exclude from email regex results
_EMAIL_BLACKLIST_EXTENSIONS: tuple[str, ...] = (".png", ".jpg", ".gif", ".svg", ".webp")
_EMAIL_BLACKLIST_DOMAINS: tuple[str, ...] = ("example.com", "sentry.io", "schema.org")

# French phone regex (covers 0X formats and +33 international)
_PHONE_FR_RE = re.compile(
    r"(?:\+33\s?|0)[1-9](?:[\s.\-]?\d{2}){4}"
)


# ============================================================================
# EMAIL EXTRACTION
# ============================================================================

def _extract_email(soup: BeautifulSoup) -> str | None:
    """Extract the first valid email from a parsed page.

    Priority 1 — ``<a href="mailto:...">`` links (reliable, structured).
    Priority 2 — regex scan of page text (fallback).

    Args:
        soup: Parsed BeautifulSoup object.

    Returns:
        Email address string, or ``None`` if not found.
    """
    # Priority 1: mailto links
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if href.lower().startswith("mailto:"):
            email = href[7:].strip().split("?")[0].strip()
            if email and _is_valid_email(email):
                logger.debug("Email via mailto: %s", email)
                return email

    # Priority 2: regex fallback on page text
    page_text = soup.get_text(" ", strip=True)
    for match in _EMAIL_RE.finditer(page_text):
        email = match.group()
        if _is_valid_email(email):
            logger.debug("Email via regex: %s", email)
            return email

    return None


def _is_valid_email(email: str) -> bool:
    """Return True if the email passes basic sanity filters.

    Rejects emails ending with image/asset extensions and known
    placeholder domains.

    Args:
        email: Raw email string.

    Returns:
        True if the email looks like a real contact email.
    """
    low = email.lower()
    if any(low.endswith(ext) for ext in _EMAIL_BLACKLIST_EXTENSIONS):
        return False
    domain_part = low.split("@")[-1] if "@" in low else ""
    if any(bl in domain_part for bl in _EMAIL_BLACKLIST_DOMAINS):
        return False
    return True


# ============================================================================
# PHONE EXTRACTION
# ============================================================================

def _extract_phone(soup: BeautifulSoup) -> str | None:
    """Extract the first valid French phone number from a parsed page.

    Priority 1 — ``<a href="tel:...">`` links (reliable, normalised).
    Priority 2 — regex scan of page text (fallback).

    Args:
        soup: Parsed BeautifulSoup object.

    Returns:
        Normalised phone string (``"0X XX XX XX XX"``), or ``None``.
    """
    # Priority 1: tel links
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if href.lower().startswith("tel:"):
            raw = href[4:].strip()
            normalized = _normalize_phone(raw)
            if normalized:
                logger.debug("Phone via tel: %s → %s", raw, normalized)
                return normalized

    # Priority 2: regex fallback on page text
    page_text = soup.get_text(" ", strip=True)
    for match in _PHONE_FR_RE.finditer(page_text):
        normalized = _normalize_phone(match.group())
        if normalized:
            logger.debug("Phone via regex: %s → %s", match.group(), normalized)
            return normalized

    return None


def _normalize_phone(raw: str) -> str | None:
    """Normalise a raw phone string to ``"0X XX XX XX XX"`` format.

    Handles:
    - ``+33 6 12 34 56 78``  → ``"06 12 34 56 78"``
    - ``0556.12.34.56``       → ``"05 56 12 34 56"``
    - ``0556-12-34-56``       → ``"05 56 12 34 56"``
    - ``0556123456``          → ``"05 56 12 34 56"``

    Args:
        raw: Raw phone string (may include spaces, dots, dashes, +33…).

    Returns:
        Normalised phone string, or ``None`` if not a valid 10-digit number.
    """
    # Strip all non-digit characters
    digits = re.sub(r"\D", "", raw)

    # Handle +33 international prefix (33 + 9 digits = 11 digits)
    if digits.startswith("33") and len(digits) == 11:
        digits = "0" + digits[2:]

    # Must be exactly 10 digits, starting with 0
    if len(digits) != 10 or not digits.startswith("0"):
        return None

    # Format as "0X XX XX XX XX"
    return f"{digits[0:2]} {digits[2:4]} {digits[4:6]} {digits[6:8]} {digits[8:10]}"


# ============================================================================
# CONTACT EXTRACTION — MAIN LOGIC
# ============================================================================

def _find_contact_link(soup: BeautifulSoup, root_url: str) -> str | None:
    """Find the contact page URL by scanning links on a parsed page.

    Looks for any ``<a>`` whose text or href contains a contact keyword,
    restricted to the same domain as ``root_url``.

    Args:
        soup:     Parsed page (typically the homepage).
        root_url: Root URL of the site (scheme + netloc, no path).

    Returns:
        Absolute URL of the contact page, or ``None`` if not found.
    """
    root_domain = urlparse(root_url).netloc
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        href = a.get("href", "").lower()
        if any(kw in text or kw in href for kw in _CONTACT_LINK_KEYWORDS):
            full_url = urljoin(root_url, a["href"])
            if urlparse(full_url).netloc == root_domain:
                logger.debug("  Contact link found: %s", full_url)
                return full_url
    return None


def extract_contacts(url: str) -> dict[str, str | None]:
    """Extract email and phone from a company website.

    Strategy (3 passes):
    1a. GET homepage → find contact nav link → follow it → extract
    1b. Extract from homepage itself (footer, inline)
    1c. Hardcoded fallback paths (/contact, /nous-contacter, …)
    2.  Playwright (JS rendering) — only if passes 1a/1b/1c all fail

    Args:
        url: Root URL of the site (e.g. ``"https://example.fr"``).

    Returns:
        ``{'email_contact': str|None, 'telephone': str|None}``
    """
    if not url or not isinstance(url, str) or url.lower() in ("nan", "none", ""):
        return {"email_contact": None, "telephone": None}

    # Normalise root URL (scheme + netloc only)
    if not url.startswith("http"):
        url = "https://" + url
    parsed = urlparse(url)
    root_url = f"{parsed.scheme}://{parsed.netloc}"

    logger.info("extract_contacts('%s')", root_url)

    # ── Pass 1a: Homepage → follow contact link ───────────────────────────────
    home_resp = _safe_get(root_url)
    if home_resp and home_resp.status_code == 200:
        home_soup = BeautifulSoup(home_resp.text, "html.parser")

        contact_url = _find_contact_link(home_soup, root_url)
        if contact_url and contact_url.rstrip("/") != root_url.rstrip("/"):
            resp = _safe_get(contact_url)
            if resp and resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                email = _extract_email(soup)
                phone = _extract_phone(soup)
                if email or phone:
                    logger.info(
                        "  Pass 1a (nav link '%s'): email=%s, phone=%s",
                        contact_url, email, phone,
                    )
                    return {"email_contact": email, "telephone": phone}

        # ── Pass 1b: Extract directly from homepage (footer / inline) ─────────
        email = _extract_email(home_soup)
        phone = _extract_phone(home_soup)
        if email or phone:
            logger.info(
                "  Pass 1b (homepage): email=%s, phone=%s", email, phone,
            )
            return {"email_contact": email, "telephone": phone}

    # ── Pass 1c: Hardcoded fallback paths ─────────────────────────────────────
    for path in CONTACT_PATHS:
        target = root_url + path
        resp = _safe_get(target)
        if resp is None or resp.status_code != 200:
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        email = _extract_email(soup)
        phone = _extract_phone(soup)
        if email or phone:
            logger.info(
                "  Pass 1c (path '%s'): email=%s, phone=%s", path, email, phone,
            )
            return {"email_contact": email, "telephone": phone}

    logger.info("  Pass 1: nothing found — trying Playwright fallback")

    # ── Pass 2: Playwright fallback ───────────────────────────────────────────
    return _extract_contacts_playwright(root_url)


def _extract_contacts_playwright(root_url: str) -> dict[str, str | None]:
    """Playwright-based fallback for JS-rendered contact extraction.

    Tries ``/contact`` then homepage (``/``).

    Args:
        root_url: Root URL without trailing path.

    Returns:
        ``{'email_contact': str|None, 'telephone': str|None}``
    """
    try:
        from playwright.sync_api import sync_playwright  # lazy import

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            for path in ["/contact", "/"]:
                try:
                    page.goto(root_url + path, timeout=15000)
                    html = page.content()
                    soup = BeautifulSoup(html, "html.parser")
                    email = _extract_email(soup)
                    phone = _extract_phone(soup)
                    if email or phone:
                        browser.close()
                        logger.info(
                            "  Pass 2 (Playwright) success on '%s': email=%s, phone=%s",
                            path, email, phone,
                        )
                        return {"email_contact": email, "telephone": phone}
                except Exception as exc:
                    logger.debug("  Playwright page error on '%s': %s", path, exc)

            browser.close()

    except ImportError:
        logger.debug("  Playwright not available — skipping pass 2")
    except Exception as exc:
        logger.warning("  Playwright error: %s", exc)

    return {"email_contact": None, "telephone": None}


# ============================================================================
# BATCH EXTRACTION
# ============================================================================

def run_contact_extraction(input_path: str, output_path: str) -> str:
    """Iterate over a prospects CSV and extract contacts for each site.

    Reads ``site_web`` column, enriches the DataFrame with ``email_contact``
    and ``telephone`` columns, and writes the result to ``output_path``.

    Args:
        input_path:  CSV with a ``site_web`` column.
        output_path: Enriched CSV with ``email_contact`` and ``telephone`` appended.

    Returns:
        output_path (for chaining).
    """
    logger.info(
        "run_contact_extraction('%s' → '%s')", input_path, output_path
    )
    df = pd.read_csv(input_path)

    if "email_contact" not in df.columns:
        df["email_contact"] = None
    if "telephone" not in df.columns:
        df["telephone"] = None

    total = len(df)
    for idx, row in df.iterrows():
        site = str(row.get("site_web", "") or "")
        name = str(row.get("entreprise", row.get("denominationUniteLegale", "?")))
        logger.info(
            "[%d/%d] Extracting contacts: %s — %s",
            int(idx) + 1, total, name, site,
        )
        contacts = extract_contacts(site)
        df.at[idx, "email_contact"] = contacts["email_contact"]
        df.at[idx, "telephone"] = contacts["telephone"]

        time.sleep(random.uniform(1, 3))  # polite rate-limiting

    df.to_csv(output_path, index=False)
    found_email = df["email_contact"].notna().sum()
    found_phone = df["telephone"].notna().sum()
    logger.info(
        "run_contact_extraction → %d emails, %d phones found; written to '%s'",
        found_email, found_phone, output_path,
    )
    return output_path


# ============================================================================
# HTML REPORT GENERATION
# ============================================================================

def _score_color(score: float) -> str:
    """Return a background colour for the score badge.

    Args:
        score: Prospect score (1–10 scale).

    Returns:
        CSS hex colour string.
    """
    if score >= 8:
        return "#c62828"   # dark red — high priority
    elif score >= 6:
        return "#e65100"   # orange — medium priority
    elif score >= 4:
        return "#f9a825"   # amber — lower priority
    else:
        return "#388e3c"   # green — low opportunity


def _score_text_color(score: float) -> str:
    if score >= 4:
        return "#fff"
    return "#212121"


def generate_html_report(
    csv_path: str,
    output_path: str,
    sector_name: str = "Secteur",
) -> str:
    """Generate a styled HTML prospect report from a final prospect CSV.

    Produces a table with columns: Score, Entreprise, Site web, Email,
    Téléphone, CMS, Pages, Blog, Sitemap, Signaux d'opportunité.

    Args:
        csv_path:    Path to the final_prospect_report CSV (with email_contact
                     and telephone columns if available).
        output_path: Path for the output HTML file.
        sector_name: Display name for the sector (used in the title).

    Returns:
        output_path (for chaining).
    """
    logger.info("generate_html_report('%s' → '%s')", csv_path, output_path)
    df = pd.read_csv(csv_path)

    # Ensure contact columns exist
    if "email_contact" not in df.columns:
        df["email_contact"] = None
    if "telephone" not in df.columns:
        df["telephone"] = None

    total = len(df)
    high_prio = int((df["score"] >= 7).sum()) if "score" in df.columns else 0
    med_prio  = int(((df["score"] >= 5) & (df["score"] < 7)).sum()) if "score" in df.columns else 0
    avg_score = f"{df['score'].mean():.1f}/10" if "score" in df.columns and not df.empty else "—"
    sector_display = sector_name.replace("_", " ").title()

    rows_html: list[str] = []
    for _, row in df.iterrows():
        score = float(row.get("score", 0) or 0)
        bg    = _score_color(score)
        fg    = _score_text_color(score)

        # Company + site
        name    = str(row.get("entreprise", "") or "").title()
        site    = str(row.get("site_web", "") or "")
        domain  = urlparse(site).netloc.replace("www.", "") if site else ""

        # Last update date — format YYYY-MM-DD → MM/YYYY
        raw_date = str(row.get("derniere_maj_site", "") or "").strip()
        date_str = ""
        if raw_date and raw_date not in ("nan", "None", ""):
            parts = raw_date.split("-")
            if len(parts) >= 2:
                date_str = f"{parts[1]}/{parts[0]}"

        site_td = (
            f'<a href="{site}" target="_blank">{domain}</a>'
            + (f'<br><span class="date-maj">{date_str}</span>' if date_str else "")
            if site else '<span class="na">—</span>'
        )

        # Email
        email = str(row.get("email_contact", "") or "").strip()
        email_td = (
            f'<a href="mailto:{email}">{email}</a>'
            if email else '<span class="na">—</span>'
        )

        # Phone
        phone = str(row.get("telephone", "") or "").strip()
        phone_digits = re.sub(r"\s", "", phone)
        phone_td = (
            f'<a href="tel:{phone_digits}">{phone}</a>'
            if phone else '<span class="na">—</span>'
        )

        # CMS
        cms = str(row.get("cms", "") or "").strip()
        cms_td = (
            f'<span class="cms">{cms}</span>'
            if cms and cms.lower() not in ("none", "nan", "")
            else '<span class="na">—</span>'
        )

        # Pages
        nb_pages = row.get("nb_pages", "")
        pages_td = str(int(float(nb_pages))) if nb_pages and str(nb_pages) not in ("nan", "") else "—"

        # Blog
        has_blog    = str(row.get("blog", "")).lower() in ("true", "1", "yes", "oui")
        blog_status = str(row.get("blog_status", row.get("activite", "")) or "").lower()

        # Last blog update date — format YYYY-MM-DD → MM/YYYY
        raw_blog_date = str(row.get("derniere_maj_blog", "") or "").strip()
        blog_date_str = ""
        if raw_blog_date and raw_blog_date not in ("nan", "None", ""):
            parts = raw_blog_date.split("-")
            if len(parts) >= 2:
                blog_date_str = f"{parts[1]}/{parts[0]}"

        if has_blog:
            if "abandonné" in blog_status or "abandonne" in blog_status:
                badge = '<span class="badge warn">Abandonné</span>'
            elif "semi" in blog_status:
                badge = '<span class="badge warn">Semi-actif</span>'
            else:
                badge = '<span class="badge ok">Oui</span>'
            blog_td = badge + (f'<br><span class="date-maj">{blog_date_str}</span>' if blog_date_str else "")
        else:
            blog_td = '<span class="badge bad">Non</span>'

        # Signaux
        resume = str(row.get("resume", "") or "")
        signals: list[str] = []
        if "Opportunités :" in resume:
            signal_part = resume.split("Opportunités :")[-1].strip().rstrip(".")
            signals = [s.strip() for s in signal_part.split(",") if s.strip()]
        signals_html = "".join(f"<li>{s}</li>" for s in signals)
        signaux_td = (
            f'<ul>{signals_html}</ul>'
            if signals else '<span class="na">—</span>'
        )

        rows_html.append(f"""
    <tr>
      <td class="score-cell" style="background:{bg};color:{fg}">{score}/10</td>
      <td class="name">{name}</td>
      <td class="url">{site_td}</td>
      <td class="url">{email_td}</td>
      <td class="url">{phone_td}</td>
      <td class="center">{cms_td}</td>
      <td class="center">{pages_td}</td>
      <td class="center">{blog_td}</td>
      <td class="signaux">{signaux_td}</td>
    </tr>""")

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<title>Prospects SEO — Secteur {sector_display}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         background: #f5f5f5; color: #212121; padding: 32px; }}
  .header {{ margin-bottom: 28px; }}
  .header h1 {{ font-size: 22px; font-weight: 700; color: #1a237e; }}
  .header p  {{ font-size: 13px; color: #757575; margin-top: 4px; }}
  .meta {{ display: flex; gap: 20px; margin-top: 12px; }}
  .meta-item {{ background: #fff; border-left: 4px solid #1a237e;
                padding: 10px 16px; border-radius: 4px; box-shadow: 0 1px 3px rgba(0,0,0,.1); }}
  .meta-item .num {{ font-size: 24px; font-weight: 700; color: #1a237e; }}
  .meta-item .lbl {{ font-size: 11px; color: #757575; text-transform: uppercase; letter-spacing:.5px; }}
  table {{ width: 100%; border-collapse: collapse; background: #fff;
           box-shadow: 0 1px 4px rgba(0,0,0,.12); border-radius: 8px; overflow: hidden; }}
  thead th {{ background: #1a237e; color: #fff; font-size: 11px; font-weight: 600;
              text-transform: uppercase; letter-spacing: .5px;
              padding: 12px 14px; text-align: left; }}
  tbody tr {{ border-bottom: 1px solid #f0f0f0; transition: background .1s; }}
  tbody tr:hover {{ background: #fafafa; }}
  tbody td {{ padding: 11px 14px; font-size: 13px; vertical-align: middle; }}
  .score-cell {{ font-size: 16px; font-weight: 700; text-align: center; width: 60px; }}
  .name {{ font-weight: 600; white-space: nowrap; }}
  .url a {{ color: #1565c0; text-decoration: none; font-size: 12px; }}
  .url a:hover {{ text-decoration: underline; }}
  .center {{ text-align: center; }}
  .cms {{ background: #e3f2fd; color: #0d47a1; padding: 2px 8px;
          border-radius: 12px; font-size: 11px; font-weight: 600; }}
  .badge {{ padding: 2px 9px; border-radius: 12px; font-size: 11px; font-weight: 600; }}
  .badge.bad  {{ background: #ffebee; color: #b71c1c; }}
  .badge.warn {{ background: #fff3e0; color: #e65100; }}
  .badge.ok   {{ background: #e8f5e9; color: #1b5e20; }}
  .na {{ color: #bdbdbd; }}
  .date-maj {{ font-size: 10px; color: #9e9e9e; }}
  .signaux ul {{ list-style: none; padding: 0; }}
  .signaux li {{ font-size: 11px; color: #555; padding: 1px 0; }}
  .signaux li::before {{ content: "• "; color: #c62828; }}
</style>
</head>
<body>
<div class="header">
  <h1>Analyse de prospection SEO — Secteur {sector_display}</h1>
  <p>Entreprises de 10+ salariés avec site web identifié — classées par score d'opportunité (sites anglophones exclus)</p>
  <div class="meta">
    <div class="meta-item"><div class="num">{total}</div><div class="lbl">Prospects analysés</div></div>
    <div class="meta-item"><div class="num">{high_prio}</div><div class="lbl">Haute priorité (≥7)</div></div>
    <div class="meta-item"><div class="num">{med_prio}</div><div class="lbl">Priorité moyenne (≥5)</div></div>
    <div class="meta-item"><div class="num">{avg_score}</div><div class="lbl">Score moyen</div></div>
  </div>
</div>

<table>
<thead>
  <tr>
    <th>Score</th>
    <th>Entreprise</th>
    <th>Site web</th>
    <th>Email</th>
    <th>Téléphone</th>
    <th>CMS</th>
    <th>Pages</th>
    <th>Blog</th>
    <th>Signaux d'opportunité</th>
  </tr>
</thead>
<tbody>
{"".join(rows_html)}
</tbody>
</table>
</body>
</html>
"""

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(html, encoding="utf-8")
    logger.info("HTML report written to '%s' (%d rows)", output_path, total)
    return output_path


# ============================================================================
# CLI
# ============================================================================

@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("input_csv", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--output", "-o",
    type=click.Path(dir_okay=False),
    default=None,
    show_default=True,
    help="Chemin du CSV de sortie (défaut: écrase l'entrée).",
)
@click.option(
    "--html",
    type=click.Path(dir_okay=False),
    default=None,
    help="Chemin du rapport HTML à régénérer (optionnel).",
)
@click.option(
    "--sector",
    type=str,
    default="Secteur",
    show_default=True,
    help="Nom du secteur pour le rapport HTML.",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Limiter le nombre de lignes traitées (pour les tests).",
)
def main(
    input_csv: str,
    output: str | None,
    html: str | None,
    sector: str,
    limit: int | None,
) -> None:
    """Extraction de contacts (email + téléphone) depuis les sites web prospects.

    INPUT_CSV doit contenir une colonne 'site_web'.
    """
    setup_pipeline_logging(log_dir="Logs", sector_name="contact_scraper")
    logger.info("contact_scraper.py started — input='%s'", input_csv)

    try:
        config = ContactScraperConfig(
            input_csv=Path(input_csv),
            output_csv=Path(output) if output else Path(input_csv),
        )
    except ValidationError as exc:
        click.echo(f"Erreur de configuration :\n{exc}", err=True)
        raise SystemExit(1)

    # Optionally limit rows
    if limit:
        df = pd.read_csv(str(config.input_csv))
        df_limited = df.head(limit)
        tmp_path = str(config.input_csv) + ".tmp_limit.csv"
        df_limited.to_csv(tmp_path, index=False)
        run_contact_extraction(tmp_path, str(config.output_csv))
        import os
        os.remove(tmp_path)
    else:
        run_contact_extraction(str(config.input_csv), str(config.output_csv))

    if html:
        generate_html_report(str(config.output_csv), html, sector_name=sector)
        click.echo(f"Rapport HTML écrit : {html}")

    click.echo(f"Contacts extraits → {config.output_csv}")


if __name__ == "__main__":
    main()
