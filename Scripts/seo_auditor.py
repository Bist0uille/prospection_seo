#!/usr/bin/env python3
"""
Audit SEO business-oriented par crawl léger.

Crawle chaque site (max 30 pages, BFS) et extrait des signaux concrets
exploitables par une agence : pages sans title, blog absent, site abandonné,
CMS détecté, etc.
"""

import re
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import deque
from datetime import datetime


# ============================================================================
# CONSTANTES
# ============================================================================

BLOG_URL_PATTERNS = [
    '/blog', '/actualites', '/actualite', '/fil-dactualite', '/fil-actualite',
    '/news', '/articles', '/article', '/journal', '/mag', '/magazine',
    '/ressources', '/publications', '/posts', '/edito', '/chroniques',
    '/insights', '/presse', '/communiques', '/breves', '/dossiers', '/tribunes',
]

BLOG_NAV_KEYWORDS = [
    'blog', 'actualité', 'actualités', 'news', 'journal', 'magazine', 'mag',
    'ressources', 'publications', 'édito', 'edito', 'insights', 'presse',
    'communiqués', 'brèves', 'chroniques', 'dossiers',
]

CMS_SIGNATURES = {
    'WordPress': [
        'wp-content', 'wp-includes', 'wp-json', '/wp-admin',
        'meta name="generator" content="WordPress',
    ],
    'Wix': [
        'wix.com', 'X-Wix-', '_wix_browser_sess',
        'static.wixstatic.com',
    ],
    'Shopify': [
        'cdn.shopify.com', 'Shopify.theme', 'myshopify.com',
    ],
    'Prestashop': [
        'PrestaShop', 'prestashop', '/modules/ps_',
    ],
    'Webflow': [
        'webflow.com', 'Webflow', 'assets.website-files.com',
    ],
    'Squarespace': [
        'squarespace.com', 'static1.squarespace.com', 'Squarespace',
    ],
    'Joomla': [
        'Joomla!', '/media/jui/', '/components/com_',
    ],
    'Drupal': [
        'Drupal', '/sites/default/files/', 'drupal.js',
    ],
}

DATE_PATTERN = re.compile(r'20[12]\d[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12]\d|3[01])')

# Pages structurelles légitimement pauvres en contenu → exclues du comptage pages_vides
EXCLUDED_FROM_EMPTY_COUNT = {
    '/contact', '/mentions-legales', '/mentions_legales',
    '/cgv', '/cgu', '/privacy', '/politique',
    '/politique-de-confidentialite', '/login', '/connexion',
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; SEOAuditBot/1.0)',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'fr-FR,fr;q=0.9',
}

REQUEST_TIMEOUT = 15


# ============================================================================
# HELPERS
# ============================================================================

def _safe_get(url, timeout=REQUEST_TIMEOUT):
    """GET request avec gestion d'erreur."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return resp
    except Exception:
        return None


def _extract_text_words(soup):
    """Extrait le texte visible et retourne la liste de mots (sans muter le soup).

    Parcourt les nœuds texte en ignorant ceux encapsulés dans des balises non-contenu.
    Contrairement à decompose(), cette approche ne modifie pas le soup passé en paramètre.
    """
    _IGNORE_TAGS = {'script', 'style', 'noscript', 'header', 'footer', 'nav'}
    texts = [
        el.strip()
        for el in soup.find_all(string=True)
        if el.find_parent(_IGNORE_TAGS) is None and el.strip()
    ]
    return ' '.join(texts).split()


def _detect_cms(html):
    """Détecte le CMS à partir du HTML brut."""
    for cms, signatures in CMS_SIGNATURES.items():
        for sig in signatures:
            if sig in html:
                return cms
    return None


def _detect_blog_in_nav(soup, base_url):
    """Cherche un lien blog dans la navigation (nav, header, menus).

    Returns:
        (found: bool, blog_url: str | None)
    """
    # Éléments de navigation typiques
    nav_selectors = soup.find_all(['nav', 'header'])
    # Également les divs avec class contenant nav/menu
    for div in soup.find_all('div', class_=True):
        classes = ' '.join(div.get('class', [])).lower()
        if any(k in classes for k in ('nav', 'menu', 'navigation', 'header')):
            nav_selectors.append(div)

    for el in nav_selectors:
        for a in el.find_all('a', href=True):
            text = a.get_text(strip=True).lower()
            href = a.get('href', '').lower()
            for kw in BLOG_NAV_KEYWORDS:
                if kw in text or kw in href:
                    full_url = urljoin(base_url, a['href'])
                    return True, full_url
    return False, None


def _detect_rss(soup):
    """Détecte un flux RSS ou Atom dans le <head>."""
    for link in soup.find_all('link', attrs={'type': True}):
        t = link.get('type', '').lower()
        if 'rss' in t or 'atom' in t:
            return True
    return False


def _verify_blog_has_content(blog_url):
    """Vérifie qu'une URL blog détectée contient réellement des articles.

    Évite les faux positifs (/blog vide, /actualites fantôme, etc.).

    Critères (l'un suffit) :
    - Au moins 2 dates distinctes sur la page
    - Au moins 2 liens dont l'URL ressemble à un article (date dans l'URL,
      segment /article/, /post/, etc.)

    Returns:
        bool: True si le blog semble contenir des articles.
    """
    resp = _safe_get(blog_url, timeout=10)
    if not resp or resp.status_code != 200:
        return False
    soup = BeautifulSoup(resp.text, 'html.parser')

    # Critère 1 : au moins 2 dates → contenu daté présent
    dates = _extract_dates(soup, blog_url)
    if len(dates) >= 2:
        return True

    # Critère 2 : au moins 2 liens ressemblant à des articles
    article_links = 0
    for a in soup.find_all('a', href=True):
        href = a['href'].lower()
        if (re.search(r'/20\d{2}/', href)
                or re.search(r'/(article|post|billet|actu)s?[-/]', href)
                or re.search(r'-\d{4}-\d{2}-\d{2}', href)):
            article_links += 1
            if article_links >= 2:
                return True

    return False


def _compute_publication_frequency(dates_parsed):
    """Calcule la fréquence de publication à partir d'une liste de datetime.

    Returns:
        str label (ex. 'hebdomadaire', 'mensuelle', 'trimestrielle', 'rare')
        ou None si pas assez de données.
    """
    if len(dates_parsed) < 2:
        return None
    sorted_dates = sorted(dates_parsed)
    # Intervalles en jours entre publications successives
    intervals = [
        (sorted_dates[i + 1] - sorted_dates[i]).days
        for i in range(len(sorted_dates) - 1)
        if (sorted_dates[i + 1] - sorted_dates[i]).days > 0
    ]
    if not intervals:
        return None
    avg_days = sum(intervals) / len(intervals)
    if avg_days <= 14:
        return 'hebdomadaire'
    elif avg_days <= 45:
        return 'mensuelle'
    elif avg_days <= 100:
        return 'trimestrielle'
    else:
        return 'rare'


def _extract_dates(soup, url):
    """Extrait les dates trouvées dans la page (balises <time>, schema, URL)."""
    dates = []

    # Balises <time>
    for time_tag in soup.find_all('time'):
        dt = time_tag.get('datetime', '') or time_tag.get_text()
        match = DATE_PATTERN.search(dt)
        if match:
            dates.append(match.group())

    # Schema.org datePublished / dateModified
    for script in soup.find_all('script', type='application/ld+json'):
        text = script.get_text()
        for field in ['datePublished', 'dateModified']:
            idx = text.find(field)
            if idx != -1:
                match = DATE_PATTERN.search(text[idx:idx+50])
                if match:
                    dates.append(match.group())

    # Dates dans l'URL
    match = DATE_PATTERN.search(url)
    if match:
        dates.append(match.group())

    return dates


def _parse_date(date_str):
    """Parse une date au format YYYY-MM-DD ou YYYY/MM/DD."""
    date_str = date_str.replace('/', '-')
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return None


def _get_internal_links(soup, base_url, base_domain):
    """Extrait les liens internes depuis le HTML parsé."""
    links = set()
    for a in soup.find_all('a', href=True):
        href = a['href']
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        # Garder seulement les liens internes, HTTP(S), sans fragments
        if parsed.netloc.replace('www.', '') == base_domain:
            clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            # Ignorer les fichiers non-HTML
            if not re.search(r'\.(pdf|jpg|jpeg|png|gif|svg|css|js|zip|doc|xls|mp[34])$',
                             clean, re.I):
                links.add(clean.rstrip('/'))
    return links


# ============================================================================
# AUDIT D'UN SITE
# ============================================================================

def audit_site(url, max_pages=30):
    """
    Crawl léger d'un site et extraction de signaux SEO business.

    Args:
        url: URL de départ du site.
        max_pages: Nombre maximum de pages à crawler.

    Returns:
        dict avec tous les signaux SEO extraits.
    """
    # Normaliser l'URL
    if not url.startswith('http'):
        url = 'https://' + url
    parsed = urlparse(url)
    base_domain = parsed.netloc.replace('www.', '')
    start_url = f"{parsed.scheme}://{parsed.netloc}"

    result = {
        # Structure
        'nb_pages': 0,
        'profondeur_max': 0,
        'has_sitemap': False,
        # Blog
        'has_blog': False,
        'blog_url': None,
        'has_rss': False,
        'blog_status': 'absent',
        'derniere_maj_blog': None,
        'frequence_publication': None,
        # Activité
        'derniere_date': None,
        'activite_status': 'inconnu',
        # SEO technique
        'pages_sans_title': 0,
        'pages_title_court': 0,
        'titles_dupliques': 0,
        'pages_sans_meta_desc': 0,
        'pages_sans_h1': 0,
        'pages_h1_multiple': 0,
        'pages_sans_canonical': 0,
        # Indexabilité
        'has_robots_txt': False,
        'pages_noindex': 0,
        # Contenu
        'mots_moyen_par_page': 0,
        'pages_vides': 0,
        'ratio_texte_html': 0.0,
        # Technologie
        'cms_detecte': None,
        # Erreur
        'audit_erreur': None,
    }

    # ----- Vérifier robots.txt -----
    robots_resp = _safe_get(f"{start_url}/robots.txt", timeout=10)
    if robots_resp and robots_resp.status_code == 200 and 'user-agent' in robots_resp.text.lower():
        result['has_robots_txt'] = True

    # ----- Vérifier sitemap.xml -----
    sitemap_resp = _safe_get(f"{start_url}/sitemap.xml", timeout=10)
    if sitemap_resp and sitemap_resp.status_code == 200 and '<urlset' in sitemap_resp.text.lower():
        result['has_sitemap'] = True

    # ----- BFS crawl -----
    visited = set()
    queue = deque()
    queue.append((url.rstrip('/'), 0))
    visited.add(url.rstrip('/'))

    all_titles = []
    all_dates = []
    blog_dates = []      # dates collectées uniquement sur les pages blog
    nav_blog_candidate = None  # premier lien blog trouvé dans la nav (fallback)
    total_words = 0
    total_html_bytes = 0
    total_text_bytes = 0
    pages_crawled = 0
    cms_detected = None

    while queue and pages_crawled < max_pages:
        current_url, depth = queue.popleft()

        resp = _safe_get(current_url)
        if resp is None or resp.status_code != 200:
            continue
        content_type = resp.headers.get('Content-Type', '')
        if 'text/html' not in content_type:
            continue

        html = resp.text
        pages_crawled += 1
        total_html_bytes += len(html.encode('utf-8', errors='ignore'))

        soup = BeautifulSoup(html, 'html.parser')

        # --- CMS (détection sur la première page ou jusqu'à trouvé) ---
        if cms_detected is None:
            cms_detected = _detect_cms(html)

        # --- Title ---
        title_tag = soup.find('title')
        title_text = title_tag.get_text(strip=True) if title_tag else ''
        if not title_text:
            result['pages_sans_title'] += 1
        elif len(title_text) < 20:
            result['pages_title_court'] += 1
        all_titles.append(title_text)

        # --- Meta description ---
        meta_desc = soup.find('meta', attrs={'name': re.compile(r'^description$', re.I)})
        if not meta_desc or not meta_desc.get('content', '').strip():
            result['pages_sans_meta_desc'] += 1

        # --- H1 ---
        h1_tags = soup.find_all('h1')
        if len(h1_tags) == 0:
            result['pages_sans_h1'] += 1
        elif len(h1_tags) > 1:
            result['pages_h1_multiple'] += 1

        # --- Canonical ---
        canonical = soup.find('link', rel='canonical')
        if not canonical:
            result['pages_sans_canonical'] += 1

        # --- Noindex ---
        robots_meta = soup.find('meta', attrs={'name': re.compile(r'^robots$', re.I)})
        if robots_meta and 'noindex' in (robots_meta.get('content', '') or '').lower():
            result['pages_noindex'] += 1

        # --- Contenu ---
        words = _extract_text_words(soup)  # réutilise le soup existant (pas de double parsing)
        word_count = len(words)
        total_words += word_count
        text_content = ' '.join(words)
        total_text_bytes += len(text_content.encode('utf-8', errors='ignore'))
        # Exclure les pages structurelles légitimement courtes (contact, CGV, etc.)
        url_path = urlparse(current_url).path.lower().rstrip('/')
        is_structural_page = any(
            url_path == p or url_path.startswith(p + '/')
            for p in EXCLUDED_FROM_EMPTY_COUNT
        )
        if word_count < 50 and not is_structural_page:
            result['pages_vides'] += 1

        # --- Dates ---
        page_dates = _extract_dates(soup, current_url)
        all_dates.extend(page_dates)

        # --- Blog detection (URL patterns — priorité maximale) ---
        is_blog_page = False
        for pattern in BLOG_URL_PATTERNS:
            if pattern in current_url.lower():
                is_blog_page = True
                if not result['has_blog']:
                    result['has_blog'] = True
                    result['blog_url'] = current_url
                break

        # Collecter un candidat nav comme fallback (sans déclencher has_blog)
        if nav_blog_candidate is None:
            found_in_nav, nav_blog_url = _detect_blog_in_nav(soup, current_url)
            if found_in_nav:
                nav_blog_candidate = nav_blog_url

        # RSS detection (sur chaque page)
        if not result['has_rss'] and _detect_rss(soup):
            result['has_rss'] = True

        # Collecter les dates spécifiquement depuis les pages blog
        if is_blog_page:
            blog_dates.extend(page_dates)

        # --- Profondeur ---
        if depth > result['profondeur_max']:
            result['profondeur_max'] = depth

        # --- Liens internes (BFS) ---
        if pages_crawled < max_pages:
            internal_links = _get_internal_links(soup, current_url, base_domain)
            for link in internal_links:
                if link not in visited:
                    visited.add(link)
                    queue.append((link, depth + 1))

        # Délai entre requêtes
        time.sleep(0.5)

    # ----- Post-traitement -----

    result['nb_pages'] = pages_crawled
    result['cms_detecte'] = cms_detected

    if pages_crawled == 0:
        result['audit_erreur'] = 'Aucune page accessible'
        return result

    # Fallback 1 : patterns URL dans les liens découverts mais non crawlés
    if not result['has_blog']:
        for v in visited:
            for pattern in BLOG_URL_PATTERNS:
                if pattern in v.lower():
                    result['has_blog'] = True
                    result['blog_url'] = v
                    break
            if result['has_blog']:
                break

    # Fallback 2 : nav detection avec vérification de contenu réel
    # (évite les /blog vides, /actualites fantômes, etc.)
    if not result['has_blog'] and nav_blog_candidate:
        if _verify_blog_has_content(nav_blog_candidate):
            result['has_blog'] = True
            result['blog_url'] = nav_blog_candidate

    # Blog : date de dernière publication, fréquence et statut d'activité blog
    if blog_dates:
        parsed_blog = [_parse_date(d) for d in blog_dates]
        parsed_blog = [d for d in parsed_blog if d is not None]
        if parsed_blog:
            latest_blog = max(parsed_blog)
            result['derniere_maj_blog'] = latest_blog.strftime('%Y-%m-%d')
            result['frequence_publication'] = _compute_publication_frequency(parsed_blog)
            days_since_blog = (datetime.now() - latest_blog).days
            if days_since_blog < 365:
                result['blog_status'] = 'actif'
            elif days_since_blog < 730:
                result['blog_status'] = 'semi-actif'
            else:
                result['blog_status'] = 'abandonné'
        else:
            result['blog_status'] = 'présent' if result['has_blog'] else 'absent'
    else:
        result['blog_status'] = 'présent' if result['has_blog'] else 'absent'

    # Titles dupliqués → ratio 0.0–1.0 (pertinent quelle que soit la taille du site)
    non_empty_titles = [t for t in all_titles if t]
    if non_empty_titles:
        unique_titles = set(non_empty_titles)
        dupes = len(non_empty_titles) - len(unique_titles)
        result['titles_dupliques'] = round(dupes / len(non_empty_titles), 2)

    # Contenu moyen
    result['mots_moyen_par_page'] = round(total_words / pages_crawled)

    # Ratio texte/HTML
    if total_html_bytes > 0:
        result['ratio_texte_html'] = round(total_text_bytes / total_html_bytes, 2)

    # Activité globale : priorité aux dates de blog (source fiable),
    # fallback sur all_dates mais plafonné à 'semi-actif'.
    # Règle business : un site sans blog ne peut pas être déclaré "actif"
    # sur la seule base de dates génériques (footers, CGU, mentions légales…).
    parsed_blog_for_activity = []
    if blog_dates:
        parsed_blog_for_activity = [_parse_date(d) for d in blog_dates]
        parsed_blog_for_activity = [d for d in parsed_blog_for_activity if d is not None]

    parsed_all_dates = []
    if all_dates:
        parsed_all_dates = [_parse_date(d) for d in all_dates]
        parsed_all_dates = [d for d in parsed_all_dates if d is not None]

    if result['has_blog'] and parsed_blog_for_activity:
        # Source fiable : dates issues des pages blog
        latest = max(parsed_blog_for_activity)
        result['derniere_date'] = latest.strftime('%Y-%m-%d')
        days_since = (datetime.now() - latest).days
        if days_since < 365:
            result['activite_status'] = 'actif'
        elif days_since < 730:
            result['activite_status'] = 'semi-actif'
        else:
            result['activite_status'] = 'abandonné'
    elif parsed_all_dates:
        # Fallback dates globales : impossible de confirmer "actif" sans blog
        latest = max(parsed_all_dates)
        result['derniere_date'] = latest.strftime('%Y-%m-%d')
        days_since = (datetime.now() - latest).days
        if days_since < 730:
            result['activite_status'] = 'semi-actif'  # plafond sans blog
        else:
            result['activite_status'] = 'abandonné'

    return result


# ============================================================================
# AUDIT BATCH
# ============================================================================

def run_seo_audit(input_path, output_path, max_pages=30):
    """
    Itère sur un CSV de prospects, lance audit_site() sur chaque site vérifié,
    et ajoute les colonnes d'audit au DataFrame.

    Args:
        input_path: CSV avec colonnes site_web et site_verifie.
        output_path: CSV enrichi des colonnes SEO.
        max_pages: Pages max à crawler par site.
    """
    print(f"\nAudit SEO business — crawl léger (max {max_pages} pages/site)")
    df = pd.read_csv(input_path)

    # Colonnes d'audit à ajouter
    audit_columns = [
        'nb_pages', 'profondeur_max', 'has_sitemap',
        'has_blog', 'blog_url', 'has_rss', 'blog_status',
        'derniere_maj_blog', 'frequence_publication',
        'derniere_date', 'activite_status',
        'pages_sans_title', 'pages_title_court', 'titles_dupliques',
        'pages_sans_meta_desc', 'pages_sans_h1', 'pages_h1_multiple',
        'pages_sans_canonical',
        'has_robots_txt', 'pages_noindex',
        'mots_moyen_par_page', 'pages_vides', 'ratio_texte_html',
        'cms_detecte', 'audit_erreur',
    ]
    for col in audit_columns:
        df[col] = None

    # Filtrer les sites vérifiés
    mask = df['site_verifie'] == True
    sites_to_audit = df[mask]
    total = len(sites_to_audit)
    print(f"  Sites à auditer : {total}")

    audited = 0
    for idx in sites_to_audit.index:
        url = str(df.at[idx, 'site_web'])
        name = str(df.at[idx, 'denominationUniteLegale'])
        audited += 1
        print(f"  [{audited}/{total}] {name} — {url}")

        try:
            audit = audit_site(url, max_pages=max_pages)
            for col in audit_columns:
                df.at[idx, col] = audit.get(col)
        except Exception as e:
            print(f"    Erreur : {e}")
            df.at[idx, 'audit_erreur'] = str(e)

    df.to_csv(output_path, index=False)
    print(f"-> {audited} sites audités. Fichier : {output_path}")
    return output_path


# ============================================================================
# CLI
# ============================================================================

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Audit SEO business par crawl léger')
    parser.add_argument('input_csv', help='CSV avec colonnes site_web et site_verifie')
    parser.add_argument('--output', '-o', default='Results/seo_audit.csv',
                        help='Chemin du CSV de sortie')
    parser.add_argument('--max-pages', type=int, default=30,
                        help='Nombre max de pages à crawler par site')
    args = parser.parse_args()

    run_seo_audit(args.input_csv, args.output, max_pages=args.max_pages)
