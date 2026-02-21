#!/usr/bin/env python3
"""
Business-oriented SEO audit via lightweight BFS crawl.

Crawls each site (up to ``max_pages`` pages) and extracts concrete signals
actionable by a web agency: missing titles, absent blog, abandoned site,
detected CMS, etc.

Standalone usage:
  python Scripts/seo_auditor.py Results/nautisme/verified_websites.csv
  python Scripts/seo_auditor.py Results/nautisme/verified_websites.csv -o out.csv --max-pages 50
"""

from __future__ import annotations

import re
import sys
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import click
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pydantic import ValidationError

# ── Project root on sys.path ──────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from Scripts.core.logging_config import get_logger, setup_pipeline_logging
from Scripts.core.models import SeoAuditConfig

logger = get_logger(__name__)


# ============================================================================
# CONSTANTS
# ============================================================================

BLOG_URL_PATTERNS: list[str] = [
    "/blog", "/actualites", "/actualite", "/fil-dactualite", "/fil-actualite",
    "/news", "/articles", "/article", "/journal", "/mag", "/magazine",
    "/ressources", "/publications", "/posts", "/edito", "/chroniques",
    "/insights", "/presse", "/communiques", "/breves", "/dossiers", "/tribunes",
]

BLOG_NAV_KEYWORDS: list[str] = [
    "blog", "actualité", "actualités", "news", "journal", "magazine", "mag",
    "ressources", "publications", "édito", "edito", "insights", "presse",
    "communiqués", "brèves", "chroniques", "dossiers",
]

CMS_SIGNATURES: dict[str, list[str]] = {
    "WordPress": [
        "wp-content", "wp-includes", "wp-json", "/wp-admin",
        'meta name="generator" content="WordPress',
    ],
    "Wix": [
        "wix.com", "X-Wix-", "_wix_browser_sess",
        "static.wixstatic.com",
    ],
    "Shopify": [
        "cdn.shopify.com", "Shopify.theme", "myshopify.com",
    ],
    "Prestashop": [
        "PrestaShop", "prestashop", "/modules/ps_",
    ],
    "Webflow": [
        "webflow.com", "Webflow", "assets.website-files.com",
    ],
    "Squarespace": [
        "squarespace.com", "static1.squarespace.com", "Squarespace",
    ],
    "Joomla": [
        "Joomla!", "/media/jui/", "/components/com_",
    ],
    "Drupal": [
        "Drupal", "/sites/default/files/", "drupal.js",
    ],
}

DATE_PATTERN = re.compile(r"20[12]\d[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12]\d|3[01])")

# Structural pages legitimately sparse in content — excluded from 'pages_vides' count
EXCLUDED_FROM_EMPTY_COUNT: set[str] = {
    "/contact", "/mentions-legales", "/mentions_legales",
    "/cgv", "/cgu", "/privacy", "/politique",
    "/politique-de-confidentialite", "/login", "/connexion",
}

HEADERS: dict[str, str] = {
    "User-Agent": "Mozilla/5.0 (compatible; SEOAuditBot/1.0)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "fr-FR,fr;q=0.9",
}

REQUEST_TIMEOUT = 15


# ============================================================================
# HELPERS
# ============================================================================

def _safe_get(url: str, timeout: int = REQUEST_TIMEOUT) -> requests.Response | None:
    """Perform a GET request with error handling.

    Args:
        url:     Target URL.
        timeout: Request timeout in seconds.

    Returns:
        :class:`requests.Response` on success, or ``None`` on any error.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        logger.debug("GET %s → %d", url, resp.status_code)
        return resp
    except Exception as exc:
        logger.debug("GET %s failed: %s", url, exc)
        return None


def _extract_text_words(soup: BeautifulSoup) -> list[str]:
    """Extract visible text words from a parsed page without mutating the soup.

    Ignores content inside script, style, noscript, header, footer, nav tags.

    Args:
        soup: Parsed :class:`BeautifulSoup` object.

    Returns:
        List of whitespace-split words from visible text.
    """
    _IGNORE_TAGS = {"script", "style", "noscript", "header", "footer", "nav"}
    texts = [
        el.strip()
        for el in soup.find_all(string=True)
        if el.find_parent(_IGNORE_TAGS) is None and el.strip()
    ]
    return " ".join(texts).split()


def _detect_cms(html: str) -> str | None:
    """Detect the CMS from raw HTML content.

    Args:
        html: Raw HTML string of a crawled page.

    Returns:
        CMS name (e.g. ``'WordPress'``) or ``None`` if undetected.
    """
    for cms, signatures in CMS_SIGNATURES.items():
        for sig in signatures:
            if sig in html:
                logger.debug("CMS detected: %s (signature: %r)", cms, sig)
                return cms
    return None


def _detect_blog_in_nav(
    soup: BeautifulSoup, base_url: str
) -> tuple[bool, str | None]:
    """Search for a blog link within navigation elements.

    Looks inside ``<nav>``, ``<header>``, and divs with nav/menu classes.

    Args:
        soup:     Parsed page.
        base_url: Base URL for resolving relative hrefs.

    Returns:
        ``(found, blog_url)`` — blog_url is the resolved URL or None.
    """
    nav_selectors = soup.find_all(["nav", "header"])
    for div in soup.find_all("div", class_=True):
        classes = " ".join(div.get("class", [])).lower()
        if any(k in classes for k in ("nav", "menu", "navigation", "header")):
            nav_selectors.append(div)

    for el in nav_selectors:
        for a in el.find_all("a", href=True):
            text = a.get_text(strip=True).lower()
            href = a.get("href", "").lower()
            for kw in BLOG_NAV_KEYWORDS:
                if kw in text or kw in href:
                    full_url = urljoin(base_url, a["href"])
                    logger.debug("Blog link in nav: '%s' → %s", kw, full_url)
                    return True, full_url
    return False, None


def _detect_rss(soup: BeautifulSoup) -> bool:
    """Detect an RSS or Atom feed in the ``<head>``.

    Args:
        soup: Parsed page.

    Returns:
        True if a feed link is found.
    """
    for link in soup.find_all("link", attrs={"type": True}):
        t = link.get("type", "").lower()
        if "rss" in t or "atom" in t:
            return True
    return False


def _verify_blog_has_content(blog_url: str) -> bool:
    """Verify that a detected blog URL contains real articles.

    Avoids false positives (empty /blog page, ghost /actualites, …).

    Criteria (either is sufficient):
    - At least 2 distinct dates on the page
    - At least 2 links whose URL looks like an article

    Args:
        blog_url: URL of the candidate blog page.

    Returns:
        True if the blog seems to contain articles.
    """
    logger.debug("_verify_blog_has_content('%s')", blog_url)
    resp = _safe_get(blog_url, timeout=10)
    if not resp or resp.status_code != 200:
        return False
    soup = BeautifulSoup(resp.text, "html.parser")

    dates = _extract_dates(soup, blog_url)
    if len(dates) >= 2:
        logger.debug("  Blog verified: ≥2 dates found.")
        return True

    article_links = 0
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if (
            re.search(r"/20\d{2}/", href)
            or re.search(r"/(article|post|billet|actu)s?[-/]", href)
            or re.search(r"-\d{4}-\d{2}-\d{2}", href)
        ):
            article_links += 1
            if article_links >= 2:
                logger.debug("  Blog verified: ≥2 article-pattern links found.")
                return True

    logger.debug("  Blog NOT verified (no dates or article links).")
    return False


def _compute_publication_frequency(dates_parsed: list[datetime]) -> str | None:
    """Compute publication frequency from a list of datetimes.

    Args:
        dates_parsed: List of datetime objects.

    Returns:
        One of ``'hebdomadaire'``, ``'mensuelle'``, ``'trimestrielle'``, ``'rare'``,
        or ``None`` if fewer than 2 dates are provided.
    """
    if len(dates_parsed) < 2:
        return None
    sorted_dates = sorted(dates_parsed)
    intervals = [
        (sorted_dates[i + 1] - sorted_dates[i]).days
        for i in range(len(sorted_dates) - 1)
        if (sorted_dates[i + 1] - sorted_dates[i]).days > 0
    ]
    if not intervals:
        return None
    avg_days = sum(intervals) / len(intervals)
    logger.debug("  Avg publication interval: %.1f days", avg_days)
    if avg_days <= 14:
        return "hebdomadaire"
    elif avg_days <= 45:
        return "mensuelle"
    elif avg_days <= 100:
        return "trimestrielle"
    else:
        return "rare"


def _extract_dates(soup: BeautifulSoup, url: str) -> list[str]:
    """Extract date strings found on a page.

    Sources checked:
    - ``<time>`` tags (datetime attribute or text content)
    - Schema.org JSON-LD (datePublished, dateModified)
    - Date patterns in the URL itself

    Args:
        soup: Parsed page.
        url:  Page URL (used for URL-embedded dates).

    Returns:
        List of date strings matching ``YYYY-MM-DD`` or ``YYYY/MM/DD``.
    """
    dates: list[str] = []

    for time_tag in soup.find_all("time"):
        dt = time_tag.get("datetime", "") or time_tag.get_text()
        match = DATE_PATTERN.search(dt)
        if match:
            dates.append(match.group())

    for script in soup.find_all("script", type="application/ld+json"):
        text = script.get_text()
        for field in ("datePublished", "dateModified"):
            idx = text.find(field)
            if idx != -1:
                match = DATE_PATTERN.search(text[idx: idx + 50])
                if match:
                    dates.append(match.group())

    match = DATE_PATTERN.search(url)
    if match:
        dates.append(match.group())

    return dates


def _parse_date(date_str: str) -> datetime | None:
    """Parse a date string in ``YYYY-MM-DD`` or ``YYYY/MM/DD`` format.

    Args:
        date_str: Date string to parse.

    Returns:
        :class:`datetime` object, or ``None`` on parse failure.
    """
    date_str = date_str.replace("/", "-")
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        logger.debug("Failed to parse date: '%s'", date_str)
        return None


def _get_internal_links(
    soup: BeautifulSoup, base_url: str, base_domain: str
) -> set[str]:
    """Extract internal links from a parsed page for BFS crawling.

    Filters out:
    - External domains
    - Non-HTTP(S) schemes
    - Fragment-only links
    - Binary file extensions (.pdf, .jpg, .zip, …)

    Args:
        soup:        Parsed page.
        base_url:    Current page URL (for resolving relative hrefs).
        base_domain: Domain of the site being crawled (without www.).

    Returns:
        Set of cleaned internal URLs (no trailing slash, no fragment).
    """
    links: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        if parsed.netloc.replace("www.", "") == base_domain:
            clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if not re.search(
                r"\.(pdf|jpg|jpeg|png|gif|svg|css|js|zip|doc|xls|mp[34])$",
                clean, re.I,
            ):
                links.add(clean.rstrip("/"))
    return links


# ============================================================================
# SITE AUDIT
# ============================================================================

def audit_site(url: str, max_pages: int = 30) -> dict:
    """Perform a lightweight BFS crawl of a site and extract SEO signals.

    Args:
        url:       Entry URL for the site.
        max_pages: Maximum number of pages to crawl.

    Returns:
        Dictionary of SEO signals (see :func:`run_seo_audit` for column list).
    """
    logger.info("audit_site('%s', max_pages=%d)", url, max_pages)

    if not url.startswith("http"):
        url = "https://" + url
    parsed = urlparse(url)
    base_domain = parsed.netloc.replace("www.", "")
    start_url = f"{parsed.scheme}://{parsed.netloc}"

    result: dict = {
        # Structure
        "nb_pages": 0, "profondeur_max": 0, "has_sitemap": False,
        # Blog
        "has_blog": False, "blog_url": None, "has_rss": False,
        "blog_status": "absent", "derniere_maj_blog": None, "frequence_publication": None,
        # Activity
        "derniere_date": None, "activite_status": "inconnu",
        # SEO technical
        "pages_sans_title": 0, "pages_title_court": 0, "titles_dupliques": 0,
        "pages_sans_meta_desc": 0, "pages_sans_h1": 0, "pages_h1_multiple": 0,
        "pages_sans_canonical": 0,
        # Indexability
        "has_robots_txt": False, "pages_noindex": 0,
        # Content
        "mots_moyen_par_page": 0, "pages_vides": 0, "ratio_texte_html": 0.0,
        # Technology
        "cms_detecte": None,
        # Error
        "audit_erreur": None,
    }

    # ── robots.txt ────────────────────────────────────────────────────────────
    robots_resp = _safe_get(f"{start_url}/robots.txt", timeout=10)
    if (
        robots_resp
        and robots_resp.status_code == 200
        and "user-agent" in robots_resp.text.lower()
    ):
        result["has_robots_txt"] = True
        logger.debug("  robots.txt found.")

    # ── sitemap.xml ───────────────────────────────────────────────────────────
    sitemap_resp = _safe_get(f"{start_url}/sitemap.xml", timeout=10)
    if (
        sitemap_resp
        and sitemap_resp.status_code == 200
        and "<urlset" in sitemap_resp.text.lower()
    ):
        result["has_sitemap"] = True
        logger.debug("  sitemap.xml found.")

    # ── BFS crawl ─────────────────────────────────────────────────────────────
    visited: set[str] = set()
    queue: deque[tuple[str, int]] = deque()
    queue.append((url.rstrip("/"), 0))
    visited.add(url.rstrip("/"))

    all_titles: list[str] = []
    all_dates: list[str] = []
    blog_dates: list[str] = []
    nav_blog_candidate: str | None = None
    total_words = 0
    total_html_bytes = 0
    total_text_bytes = 0
    pages_crawled = 0
    cms_detected: str | None = None

    while queue and pages_crawled < max_pages:
        current_url, depth = queue.popleft()

        resp = _safe_get(current_url)
        if resp is None or resp.status_code != 200:
            continue
        if "text/html" not in resp.headers.get("Content-Type", ""):
            continue

        html = resp.text
        pages_crawled += 1
        total_html_bytes += len(html.encode("utf-8", errors="ignore"))
        logger.debug("  [%d/%d] Crawled: %s", pages_crawled, max_pages, current_url)

        soup = BeautifulSoup(html, "html.parser")

        # ── CMS detection (first page or until found) ─────────────────────────
        if cms_detected is None:
            cms_detected = _detect_cms(html)

        # ── Title ─────────────────────────────────────────────────────────────
        title_tag = soup.find("title")
        title_text = title_tag.get_text(strip=True) if title_tag else ""
        if not title_text:
            result["pages_sans_title"] += 1
        elif len(title_text) < 20:
            result["pages_title_court"] += 1
        all_titles.append(title_text)

        # ── Meta description ──────────────────────────────────────────────────
        meta_desc = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
        if not meta_desc or not meta_desc.get("content", "").strip():
            result["pages_sans_meta_desc"] += 1

        # ── H1 ────────────────────────────────────────────────────────────────
        h1_tags = soup.find_all("h1")
        if len(h1_tags) == 0:
            result["pages_sans_h1"] += 1
        elif len(h1_tags) > 1:
            result["pages_h1_multiple"] += 1

        # ── Canonical ─────────────────────────────────────────────────────────
        if not soup.find("link", rel="canonical"):
            result["pages_sans_canonical"] += 1

        # ── Noindex ───────────────────────────────────────────────────────────
        robots_meta = soup.find("meta", attrs={"name": re.compile(r"^robots$", re.I)})
        if robots_meta and "noindex" in (robots_meta.get("content", "") or "").lower():
            result["pages_noindex"] += 1

        # ── Content ───────────────────────────────────────────────────────────
        words = _extract_text_words(soup)
        word_count = len(words)
        total_words += word_count
        text_content = " ".join(words)
        total_text_bytes += len(text_content.encode("utf-8", errors="ignore"))

        url_path = urlparse(current_url).path.lower().rstrip("/")
        is_structural = any(
            url_path == p or url_path.startswith(p + "/")
            for p in EXCLUDED_FROM_EMPTY_COUNT
        )
        if word_count < 50 and not is_structural:
            result["pages_vides"] += 1

        # ── Dates ─────────────────────────────────────────────────────────────
        page_dates = _extract_dates(soup, current_url)
        all_dates.extend(page_dates)

        # ── Blog detection (URL patterns — highest priority) ──────────────────
        is_blog_page = False
        for pattern in BLOG_URL_PATTERNS:
            if pattern in current_url.lower():
                is_blog_page = True
                if not result["has_blog"]:
                    result["has_blog"] = True
                    result["blog_url"] = current_url
                    logger.debug("  Blog URL pattern detected: %s", current_url)
                break

        # Collect nav blog candidate (fallback, no has_blog yet)
        if nav_blog_candidate is None:
            found_in_nav, nav_blog_url = _detect_blog_in_nav(soup, current_url)
            if found_in_nav:
                nav_blog_candidate = nav_blog_url

        # RSS detection
        if not result["has_rss"] and _detect_rss(soup):
            result["has_rss"] = True
            logger.debug("  RSS feed detected on: %s", current_url)

        if is_blog_page:
            blog_dates.extend(page_dates)

        # ── Depth tracking ────────────────────────────────────────────────────
        if depth > result["profondeur_max"]:
            result["profondeur_max"] = depth

        # ── BFS queue ─────────────────────────────────────────────────────────
        if pages_crawled < max_pages:
            for link in _get_internal_links(soup, current_url, base_domain):
                if link not in visited:
                    visited.add(link)
                    queue.append((link, depth + 1))

        time.sleep(0.5)

    # ── Post-processing ───────────────────────────────────────────────────────
    result["nb_pages"] = pages_crawled
    result["cms_detecte"] = cms_detected
    logger.info(
        "  Crawl complete: %d pages, cms=%s, has_blog=%s",
        pages_crawled, cms_detected, result["has_blog"],
    )

    if pages_crawled == 0:
        result["audit_erreur"] = "Aucune page accessible"
        logger.warning("  No pages accessible for '%s'.", url)
        return result

    # Fallback 1: blog URL patterns found in the link graph but not yet crawled
    if not result["has_blog"]:
        for v in visited:
            for pattern in BLOG_URL_PATTERNS:
                if pattern in v.lower():
                    result["has_blog"] = True
                    result["blog_url"] = v
                    logger.debug("  Blog detected via uncrawled URL: %s", v)
                    break
            if result["has_blog"]:
                break

    # Fallback 2: nav link with verified content
    if not result["has_blog"] and nav_blog_candidate:
        if _verify_blog_has_content(nav_blog_candidate):
            result["has_blog"] = True
            result["blog_url"] = nav_blog_candidate
            logger.debug("  Blog detected via nav verification: %s", nav_blog_candidate)

    # ── Blog activity status ──────────────────────────────────────────────────
    if blog_dates:
        parsed_blog = [_parse_date(d) for d in blog_dates]
        parsed_blog = [d for d in parsed_blog if d is not None]
        if parsed_blog:
            latest_blog = max(parsed_blog)
            result["derniere_maj_blog"] = latest_blog.strftime("%Y-%m-%d")
            result["frequence_publication"] = _compute_publication_frequency(parsed_blog)
            days_since_blog = (datetime.now() - latest_blog).days
            if days_since_blog < 365:
                result["blog_status"] = "actif"
            elif days_since_blog < 730:
                result["blog_status"] = "semi-actif"
            else:
                result["blog_status"] = "abandonné"
            logger.debug(
                "  Blog status: %s (last post: %s, %d days ago)",
                result["blog_status"], result["derniere_maj_blog"], days_since_blog,
            )
        else:
            result["blog_status"] = "présent" if result["has_blog"] else "absent"
    else:
        result["blog_status"] = "présent" if result["has_blog"] else "absent"

    # ── Duplicate titles ratio ────────────────────────────────────────────────
    non_empty_titles = [t for t in all_titles if t]
    if non_empty_titles:
        unique_titles = set(non_empty_titles)
        dupes = len(non_empty_titles) - len(unique_titles)
        result["titles_dupliques"] = round(dupes / len(non_empty_titles), 2)
        logger.debug("  Title duplicate ratio: %.2f", result["titles_dupliques"])

    # ── Average word count ────────────────────────────────────────────────────
    result["mots_moyen_par_page"] = round(total_words / pages_crawled)

    # ── Text/HTML ratio ───────────────────────────────────────────────────────
    if total_html_bytes > 0:
        result["ratio_texte_html"] = round(total_text_bytes / total_html_bytes, 2)

    # ── Global activity status ────────────────────────────────────────────────
    # Rule: a site without a blog cannot be declared "active" based on generic
    # dates alone (footers, CGU, legal notices are not reliable indicators).
    parsed_blog_activity: list[datetime] = []
    if blog_dates:
        parsed_blog_activity = [_parse_date(d) for d in blog_dates if _parse_date(d)]

    parsed_all_dates: list[datetime] = []
    if all_dates:
        parsed_all_dates = [_parse_date(d) for d in all_dates if _parse_date(d)]

    if result["has_blog"] and parsed_blog_activity:
        latest = max(parsed_blog_activity)
        result["derniere_date"] = latest.strftime("%Y-%m-%d")
        days_since = (datetime.now() - latest).days
        if days_since < 365:
            result["activite_status"] = "actif"
        elif days_since < 730:
            result["activite_status"] = "semi-actif"
        else:
            result["activite_status"] = "abandonné"
    elif parsed_all_dates:
        latest = max(parsed_all_dates)
        result["derniere_date"] = latest.strftime("%Y-%m-%d")
        days_since = (datetime.now() - latest).days
        # Capped at semi-actif without a reliable blog source
        result["activite_status"] = "semi-actif" if days_since < 730 else "abandonné"

    logger.info(
        "  Activity: %s | Blog: %s | Pages: %d | CMS: %s",
        result["activite_status"], result["blog_status"],
        result["nb_pages"], result["cms_detecte"],
    )
    return result


# ============================================================================
# BATCH AUDIT
# ============================================================================

def run_seo_audit(
    input_path: str,
    output_path: str,
    max_pages: int = 30,
) -> str:
    """Iterate over a prospects CSV and run :func:`audit_site` on each.

    Only rows where ``site_verifie == True`` are audited.  All audit columns
    are added to the output DataFrame.

    Args:
        input_path:  CSV with ``site_web`` and ``site_verifie`` columns.
        output_path: Enriched CSV with SEO audit columns appended.
        max_pages:   Maximum pages to crawl per site.

    Returns:
        output_path (for chaining).
    """
    logger.info(
        "run_seo_audit('%s' → '%s', max_pages=%d)",
        input_path, output_path, max_pages,
    )
    df = pd.read_csv(input_path)

    audit_columns = [
        "nb_pages", "profondeur_max", "has_sitemap",
        "has_blog", "blog_url", "has_rss", "blog_status",
        "derniere_maj_blog", "frequence_publication",
        "derniere_date", "activite_status",
        "pages_sans_title", "pages_title_court", "titles_dupliques",
        "pages_sans_meta_desc", "pages_sans_h1", "pages_h1_multiple",
        "pages_sans_canonical",
        "has_robots_txt", "pages_noindex",
        "mots_moyen_par_page", "pages_vides", "ratio_texte_html",
        "cms_detecte", "audit_erreur",
    ]
    for col in audit_columns:
        df[col] = None

    mask = df["site_verifie"] == True  # noqa: E712
    sites_to_audit = df[mask]
    total = len(sites_to_audit)
    logger.info("  Sites to audit: %d", total)

    audited = 0
    for idx in sites_to_audit.index:
        url  = str(df.at[idx, "site_web"])
        name = str(df.at[idx, "denominationUniteLegale"])
        audited += 1
        logger.info("[%d/%d] Auditing: %s — %s", audited, total, name, url)

        try:
            audit = audit_site(url, max_pages=max_pages)
            for col in audit_columns:
                df.at[idx, col] = audit.get(col)
        except Exception as exc:
            logger.error("  Audit error for '%s': %s", url, exc, exc_info=True)
            df.at[idx, "audit_erreur"] = str(exc)

    df.to_csv(output_path, index=False)
    logger.info(
        "run_seo_audit → %d sites audited, written to '%s'", audited, output_path
    )
    return output_path


# ============================================================================
# CLI
# ============================================================================

@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("input_csv", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--output", "-o",
    type=click.Path(dir_okay=False),
    default="Results/seo_audit.csv",
    show_default=True,
    help="Chemin du CSV de sortie.",
)
@click.option(
    "--max-pages",
    type=int,
    default=30,
    show_default=True,
    help="Nombre max de pages à crawler par site.",
)
def main(input_csv: str, output: str, max_pages: int) -> None:
    """Audit SEO business-oriented via lightweight BFS crawl.

    INPUT_CSV must contain 'site_web' and 'site_verifie' columns.
    """
    setup_pipeline_logging(log_dir="Logs", sector_name="seo_audit")
    logger.info("seo_auditor.py started — input='%s'", input_csv)

    try:
        config = SeoAuditConfig(
            input_csv=Path(input_csv),
            output=Path(output),
            max_pages=max_pages,
        )
    except ValidationError as exc:
        click.echo(f"Erreur de configuration :\n{exc}", err=True)
        raise SystemExit(1)

    run_seo_audit(str(config.input_csv), str(config.output), max_pages=config.max_pages)


if __name__ == "__main__":
    main()
