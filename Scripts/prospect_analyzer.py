"""
Prospect analyser — filtering, website verification, and opportunity scoring.

Pipeline steps covered:
  Step 1 — filter_companies_by_employees : Filter the INSEE CSV by NAF codes,
            employee bands, and administrative status.
  Step 3 — verify_websites_by_domain     : Verify that each URL matches the
            company name (keyword matching).
  Step 5 — create_prospect_scoring_v2    : Score each verified company on a
            1–10 business opportunity scale.
"""

from __future__ import annotations

import re
from datetime import datetime
from urllib.parse import urlparse

import numpy as np
import pandas as pd

from Scripts.core.logging_config import get_logger

logger = get_logger(__name__)


# ── Constants ─────────────────────────────────────────────────────────────────

EMPLOYEE_CODES_DEFAULT: list[str] = [
    "11", "12", "21", "22", "31", "32", "41", "42", "51", "52", "53",
]

# Words excluded from keyword extraction (same list as find_websites.py)
_STOP_WORDS: set[str] = {
    "sa", "sas", "sarl", "eurl", "snc", "ste", "et", "de", "la", "les", "des",
}


# ============================================================================
# HELPERS
# ============================================================================

def get_domain(url: str) -> str:
    """Extract the primary domain from a URL (without 'www.' prefix).

    Args:
        url: Any URL string.

    Returns:
        Domain string (e.g. ``'example.fr'``), or empty string on error.
    """
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return ""


def normalize_name(name: str) -> str:
    """Normalise a company name for comparison against a domain.

    Lowercases and strips all non-alphanumeric characters.

    Args:
        name: Raw company name.

    Returns:
        Normalised alphanumeric string (e.g. ``'boatcompany'``).
    """
    name = name.lower()
    return re.sub(r"[^a-z0-9]", "", name)


def extract_keywords(company_name: str) -> list[str]:
    """Extract significant keywords from a company name.

    Mirrors the logic in find_websites.py:
    - Split on whitespace and hyphens
    - Exclude stop-words (SA, SARL, SAS, …)
    - Exclude words of 2 characters or fewer
    - Normalise each word (lowercase, alphanumeric only)

    Args:
        company_name: Legal company name (denominationUniteLegale).

    Returns:
        List of normalised keyword strings.
    """
    words = re.split(r"[\s-]+", company_name)
    keywords = [
        normalize_name(w)
        for w in words
        if w.lower() not in _STOP_WORDS and len(w) > 2
    ]
    result = [k for k in keywords if k]
    logger.debug("extract_keywords('%s') → %s", company_name, result)
    return result


# ============================================================================
# STEP 1 — FILTER COMPANIES
# ============================================================================

def filter_companies_by_employees(
    input_path: str,
    output_path: str,
    naf_codes: list[str] | None = None,
    naf_code_prefixes: list[str] | None = None,
    employee_codes: list[str] | None = None,
) -> str:
    """Filter companies by employee band, NAF codes, and administrative status.

    Applies the following filters in order:
    1. Active companies only (etatAdministratifUniteLegale == 'A')
    2. Employee band codes (trancheEffectifsUniteLegale)
    3. Deduplication by SIREN (keep headquarter if available)
    4. NAF exact codes  OR  NAF prefix codes (mutually exclusive, exact takes priority)

    Args:
        input_path:        Path to the source INSEE CSV.
        output_path:       Path for the filtered output CSV.
        naf_codes:         List of exact NAF codes (e.g. ``['3012Z', '3011Z']``).
        naf_code_prefixes: List of NAF prefixes — used only when naf_codes is None.
        employee_codes:    INSEE employee-band codes to keep; defaults to 10+ employees.

    Returns:
        output_path (for chaining).
    """
    logger.info("filter_companies_by_employees('%s' → '%s')", input_path, output_path)
    df = pd.read_csv(input_path, dtype=str)

    initial_count = len(df)
    logger.info("  Source records: %d", initial_count)

    # ── Normalise NAF column (remove dots: "30.12Z" → "3012Z") ───────────────
    naf_col = "activitePrincipaleUniteLegale"
    if naf_col in df.columns:
        df[naf_col] = df[naf_col].astype(str).str.replace(".", "", regex=False)
        logger.debug("NAF codes normalised (dots removed).")

    # ── Active companies only ─────────────────────────────────────────────────
    admin_col = "etatAdministratifUniteLegale"
    if admin_col in df.columns:
        before = len(df)
        df = df[df[admin_col] == "A"].copy()
        logger.info(
            "  After active-status filter: %d (removed %d closed)", len(df), before - len(df)
        )

    # ── Employee band filter ──────────────────────────────────────────────────
    codes = employee_codes or EMPLOYEE_CODES_DEFAULT
    tranche_col = "trancheEffectifsUniteLegale"
    df[tranche_col] = df[tranche_col].replace("nan", np.nan)
    before = len(df)
    df.dropna(subset=[tranche_col], inplace=True)
    filtered_df = df[df[tranche_col].isin(codes)].copy()
    logger.info(
        "  After employee-band filter (%s): %d (removed %d)",
        codes, len(filtered_df), before - len(filtered_df),
    )

    # ── Deduplication by SIREN ────────────────────────────────────────────────
    siren_col = "siren"
    if siren_col in filtered_df.columns:
        before_dedup = len(filtered_df)
        if "etablissementSiege" in filtered_df.columns:
            filtered_df = filtered_df.sort_values("etablissementSiege", ascending=False)
        filtered_df = filtered_df.drop_duplicates(subset=siren_col, keep="first").copy()
        removed = before_dedup - len(filtered_df)
        logger.info(
            "  After SIREN deduplication: %d (removed %d duplicates)",
            len(filtered_df), removed,
        )

    # ── NAF code filter ───────────────────────────────────────────────────────
    if naf_codes:
        before = len(filtered_df)
        filtered_df = filtered_df[
            filtered_df[naf_col].isin(naf_codes)
        ].copy()
        logger.info(
            "  After NAF exact filter (%d codes): %d (removed %d)",
            len(naf_codes), len(filtered_df), before - len(filtered_df),
        )
    elif naf_code_prefixes:
        before = len(filtered_df)
        filtered_df = filtered_df[
            filtered_df[naf_col].apply(
                lambda x: any(str(x).startswith(prefix) for prefix in naf_code_prefixes)
            )
        ].copy()
        logger.info(
            "  After NAF prefix filter: %d (removed %d)",
            len(filtered_df), before - len(filtered_df),
        )

    filtered_df.to_csv(output_path, index=False)
    logger.info(
        "filter_companies_by_employees → %d companies written to '%s'",
        len(filtered_df), output_path,
    )
    return output_path


# ============================================================================
# STEP 1b — FILTER COMPANIES (PostgreSQL variant)
# ============================================================================

def filter_companies_by_employees_pg(
    engine: object,
    output_path: str,
    naf_codes: list[str] | None = None,
    employee_codes: list[str] | None = None,
) -> str:
    """Filter companies from PostgreSQL using the same logic as the CSV variant.

    Queries the ``unites_legales`` table with active-status, employee-band, and
    NAF-code filters, then writes the result to ``output_path``.

    Args:
        engine:         SQLAlchemy Engine connected to the botparser database.
        output_path:    Path for the filtered output CSV.
        naf_codes:      List of exact NAF codes (e.g. ``['3012Z', '3011Z']``).
        employee_codes: INSEE employee-band codes to keep; defaults to 10+.

    Returns:
        output_path (for chaining).
    """
    from sqlalchemy import text as sa_text  # lazy import — optional dependency

    logger.info("filter_companies_by_employees_pg(→ '%s')", output_path)

    codes = employee_codes or EMPLOYEE_CODES_DEFAULT

    # Build parameterised WHERE clause
    conditions: list[str] = ['"etatAdministratifUniteLegale" = \'A\'']
    params: dict[str, str] = {}

    emp_placeholders = ", ".join(f":emp_{i}" for i in range(len(codes)))
    conditions.append(f'"trancheEffectifsUniteLegale" IN ({emp_placeholders})')
    for i, code in enumerate(codes):
        params[f"emp_{i}"] = code

    if naf_codes:
        naf_placeholders = ", ".join(f":naf_{i}" for i in range(len(naf_codes)))
        conditions.append(
            f'"activitePrincipaleUniteLegale" IN ({naf_placeholders})'
        )
        for i, code in enumerate(naf_codes):
            params[f"naf_{i}"] = code

    where_clause = " AND ".join(conditions)
    query = sa_text(f"SELECT * FROM unites_legales WHERE {where_clause}")

    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params=params)

    logger.info(
        "filter_companies_by_employees_pg → %d companies from DB", len(df)
    )

    df.to_csv(output_path, index=False)
    logger.info(
        "filter_companies_by_employees_pg → written to '%s'", output_path
    )
    return output_path


# ============================================================================
# STEP 3 — VERIFY WEBSITES
# ============================================================================

# Domains that are directories / aggregators — never a company's own site
_BLOCKLIST: frozenset[str] = frozenset({
    "societe.com", "pagesjaunes.fr", "pappers.fr",
    "annuaire-entreprises.data.gouv.fr", "verif.com",
    "entreprises.lefigaro.fr", "fr.kompass.com", "facebook.com",
    "linkedin.com", "youtube.com", "wikipedia.org", "doctrine.fr",
    "app.dataprospects.fr", "reseauexcellence.fr", "actunautique.com",
    "lagazettefrance.fr", "kompass.com", "france3-regions.franceinfo.fr",
})


def verify_websites_by_domain(input_path: str, output_path: str) -> str:
    """Verify that each discovered URL actually belongs to the company.

    Uses exactly the same keyword-matching logic as find_websites.py to
    guarantee consistency across pipeline steps:
    - Split company name into significant words (stop-words removed, len > 2)
    - Check whether any keyword appears in the cleaned domain name

    Also rejects:
    - Blocklisted directory domains
    - Non-French URLs (/en/ path or .ca TLD)

    Args:
        input_path:  CSV with 'site_web' and 'denominationUniteLegale' columns.
        output_path: CSV enriched with 'site_verifie' and 'verification_raison'.

    Returns:
        output_path (for chaining).
    """
    logger.info("verify_websites_by_domain('%s' → '%s')", input_path, output_path)
    df = pd.read_csv(input_path)
    df["site_verifie"] = False
    df["verification_raison"] = ""
    verified_count = 0

    for index, row in df.iterrows():
        url = str(row.get("site_web", "") or "")
        company_name = str(row.get("denominationUniteLegale", "") or "")

        if not url or url == "nan":
            df.loc[index, "verification_raison"] = "URL manquante"
            logger.debug("Row %d: URL missing — skipped.", index)
            continue

        domain = get_domain(url)

        # ── Blocklist ─────────────────────────────────────────────────────────
        if any(blocked in domain for blocked in _BLOCKLIST):
            reason = "Domaine sur la liste de blocage"
            df.loc[index, "verification_raison"] = reason
            logger.debug("Row %d: %s (%s)", index, reason, domain)
            continue

        # ── Non-French URL filter ─────────────────────────────────────────────
        parsed_url = urlparse(url)
        path_lower = parsed_url.path.lower()
        domain_lower = domain.lower()

        if re.search(r"/(en|en-[a-z]{2})(/|$)", path_lower):
            reason = "URL rejetée : chemin en version anglaise (/en/)"
            df.loc[index, "verification_raison"] = reason
            logger.debug("Row %d: %s — %s", index, reason, url)
            continue

        if domain_lower.endswith(".ca"):
            reason = "URL rejetée : TLD canadien (.ca)"
            df.loc[index, "verification_raison"] = reason
            logger.debug("Row %d: %s — %s", index, reason, url)
            continue

        cleaned_domain = domain.replace(".", "").replace("-", "")
        if not cleaned_domain:
            reason = "Domaine vide après nettoyage"
            df.loc[index, "verification_raison"] = reason
            logger.warning("Row %d: %s — URL=%s", index, reason, url)
            continue

        # ── Keyword matching ──────────────────────────────────────────────────
        keywords = extract_keywords(company_name)
        if not keywords:
            reason = "Aucun mot-clé extractible du nom"
            df.loc[index, "verification_raison"] = reason
            logger.warning("Row %d: %s — company='%s'", index, reason, company_name)
            continue

        matched_keyword: str | None = None
        for keyword in keywords:
            if keyword in cleaned_domain:
                matched_keyword = keyword
                break

        if matched_keyword:
            df.loc[index, "site_verifie"] = True
            df.loc[index, "verification_raison"] = (
                f"Mot-clé '{matched_keyword}' trouvé dans le domaine"
            )
            verified_count += 1
            logger.debug(
                "Row %d: VERIFIED — keyword='%s', domain='%s'",
                index, matched_keyword, domain,
            )
        else:
            reason = f"Aucun mot-clé {keywords} dans '{domain}'"
            df.loc[index, "verification_raison"] = reason
            logger.debug("Row %d: NOT verified — %s", index, reason)

    df.to_csv(output_path, index=False)
    logger.info(
        "verify_websites_by_domain → %d/%d verified, written to '%s'",
        verified_count, len(df), output_path,
    )
    return output_path


# ============================================================================
# STEP 5 — SCORING v2
# ============================================================================

def create_prospect_scoring_v2(input_path: str, output_path: str) -> str:
    """Compute business opportunity scores (v2) for each verified prospect.

    Scores are on a 1–10 scale: **higher = more likely to need an agency**.

    Positive signals (opportunity):
        Blog abandonné                         +5
        Blog semi-actif                        +2
        No blog (often outdated static site)   +1
        nb_pages < 5                           +3
        nb_pages 5–9                           +1
        mots_moyen_par_page < 150              +2
        ratio_texte_html < 0.15               +2
        CMS undetected (hand-built)            +2
        CMS Wix / Squarespace                  +1
        No sitemap                             +1
        pages sans meta desc > 20%             +0.5 per 20% tranche
        pages sans H1 > 20%                    +0.5 per 20% tranche
        titles dupliqués > 30%                 +0.5
        pages vides > 20%                      +0.5 per 20% tranche

    Negative signals (less priority):
        Active blog (weekly/monthly)           −4
        nb_pages > 50                          −3
        mots_moyen_par_page > 400              −2

    Args:
        input_path:  CSV from the SEO audit step with site_verifie == True rows.
        output_path: Final prospect report CSV sorted by score descending.

    Returns:
        output_path (for chaining).
    """
    logger.info("create_prospect_scoring_v2('%s' → '%s')", input_path, output_path)
    df = pd.read_csv(input_path)
    df["prospect_score"] = 0.0
    df["prospect_summary"] = ""

    scored_count = 0
    for index, row in df.iterrows():
        if not row.get("site_verifie", False):
            logger.debug("Row %d: site_verifie=False — skipped.", index)
            continue
        nb_pages = row.get("nb_pages", 0) or 0
        if nb_pages == 0:
            logger.debug(
                "Row %d (%s): nb_pages=0 — skipped.",
                index, row.get("denominationUniteLegale", "?"),
            )
            continue

        score = 0.0
        logger.debug(
            "Scoring row %d — company='%s', nb_pages=%d",
            index, row.get("denominationUniteLegale", "?"), nb_pages,
        )

        # ── Blog activity (strongest signal) ─────────────────────────────────
        has_blog = bool(row.get("has_blog", False))
        blog_status = str(row.get("blog_status", "") or "").lower()
        frequence = str(row.get("frequence_publication", "") or "").lower()

        if has_blog:
            if blog_status == "abandonné":
                # Stronger signal the older the blog — compute actual age
                derniere_maj = str(row.get("derniere_maj_blog", "") or "")
                blog_age_years = None
                if derniere_maj and derniere_maj not in ("nan", "None", ""):
                    try:
                        parsed = datetime.strptime(derniere_maj[:10], "%Y-%m-%d")
                    except ValueError:
                        parsed = None
                    if parsed:
                        blog_age_years = (datetime.now() - parsed).days / 365
                if blog_age_years is not None and blog_age_years > 4:
                    score += 7
                    logger.debug("  +7 (blog abandonné >4 ans)")
                else:
                    score += 5
                    logger.debug("  +5 (blog abandonné)")
            elif blog_status == "semi-actif":
                score += 2
                logger.debug("  +2 (blog semi-actif)")
            elif blog_status == "actif" and frequence in ("hebdomadaire", "mensuelle"):
                score -= 4
                logger.debug("  -4 (blog actif, fréquent)")
        else:
            score += 1
            logger.debug("  +1 (no blog)")

        # ── Overall site activity (fallback when blog has no date signal) ─────
        # Handles sites where blog exists but crawler found no dates (blog_status
        # stays "présent"), yet the site is clearly zombie based on last activity.
        activite = str(row.get("activite_status", "") or "").lower()
        blog_has_date_signal = blog_status in ("abandonné", "semi-actif", "actif")
        if not blog_has_date_signal:
            if activite == "abandonné":
                score += 4
                logger.debug("  +4 (activite_status abandonné, blog sans dates)")
            elif activite == "semi-actif":
                score += 1
                logger.debug("  +1 (activite_status semi-actif, blog sans dates)")

        # ── Site size ─────────────────────────────────────────────────────────
        if nb_pages < 5:
            score += 3
            logger.debug("  +3 (nb_pages < 5)")
        elif nb_pages < 10:
            score += 1
            logger.debug("  +1 (nb_pages 5–9)")
        elif nb_pages > 50:
            score -= 3
            logger.debug("  -3 (nb_pages > 50)")

        # ── Content density ───────────────────────────────────────────────────
        mots_moyen = row.get("mots_moyen_par_page", 0) or 0
        if mots_moyen < 150:
            score += 2
            logger.debug("  +2 (mots_moyen < 150)")
        elif mots_moyen > 400:
            score -= 2
            logger.debug("  -2 (mots_moyen > 400)")

        ratio_texte = row.get("ratio_texte_html", 0) or 0
        if ratio_texte < 0.15:
            score += 2
            logger.debug("  +2 (ratio_texte < 0.15)")

        # ── CMS signal ────────────────────────────────────────────────────────
        cms = str(row.get("cms_detecte", "") or "").strip()
        if not cms or cms.lower() in ("none", ""):
            score += 2
            logger.debug("  +2 (no CMS detected)")
        elif cms in ("Wix", "Squarespace"):
            score += 1
            logger.debug("  +1 (Wix/Squarespace CMS)")

        # ── Sitemap ───────────────────────────────────────────────────────────
        if not row.get("has_sitemap", False):
            score += 1
            logger.debug("  +1 (no sitemap)")

        # ── SEO problems (commercial attack angles) ───────────────────────────
        pages_sans_meta = row.get("pages_sans_meta_desc", 0) or 0
        pages_sans_h1 = row.get("pages_sans_h1", 0) or 0
        title_ratio = row.get("titles_dupliques", 0) or 0
        pages_vides = row.get("pages_vides", 0) or 0

        ratio_meta = pages_sans_meta / nb_pages
        ratio_h1 = pages_sans_h1 / nb_pages
        ratio_vides = pages_vides / nb_pages

        score += 0.5 * int(ratio_meta / 0.2)
        score += 0.5 * int(ratio_h1 / 0.2)
        if title_ratio > 0.30:
            score += 0.5
        score += 0.5 * int(ratio_vides / 0.2)

        # ── Clamp to 1–10 ─────────────────────────────────────────────────────
        score = max(1.0, min(10.0, round(score, 1)))
        df.loc[index, "prospect_score"] = score
        scored_count += 1
        logger.debug("  Final score: %.1f", score)

        # ── Summary text ──────────────────────────────────────────────────────
        opportunities: list[str] = []
        if has_blog:
            if blog_status == "abandonné":
                opportunities.append("blog abandonné")
            elif blog_status == "semi-actif":
                opportunities.append("blog semi-actif")
        else:
            opportunities.append("pas de blog")
        if not row.get("has_sitemap", False):
            opportunities.append("pas de sitemap")
        if nb_pages < 5:
            opportunities.append(f"{int(nb_pages)} pages seulement")
        if mots_moyen < 150:
            opportunities.append(f"contenu faible ({int(mots_moyen)} mots/page)")
        if ratio_texte < 0.15:
            opportunities.append(f"ratio texte/HTML faible ({ratio_texte:.0%})")
        if cms and cms.lower() not in ("none", ""):
            opportunities.append(f"CMS : {cms}")
        if pages_sans_meta > 0:
            opportunities.append(f"{int(pages_sans_meta)} pages sans meta desc")
        if pages_sans_h1 > 0:
            opportunities.append(f"{int(pages_sans_h1)} pages sans H1")

        summary = f"Score {score}/10."
        if opportunities:
            summary += " Opportunités : " + ", ".join(opportunities) + "."
        df.loc[index, "prospect_summary"] = summary

    logger.info("  Scored %d companies.", scored_count)

    # ── Creation year extraction ───────────────────────────────────────────────
    if "dateCreationUniteLegale" in df.columns:
        df["annee_creation"] = (
            df["dateCreationUniteLegale"].astype(str).str[:4].replace("nan", "")
        )

    # ── Column renaming for output readability ─────────────────────────────────
    col_map: dict[str, str] = {
        "denominationUniteLegale": "entreprise",
        "site_web":                "site_web",
        "prospect_score":          "score",
        "annee_creation":          "annee_creation",
        "cms_detecte":             "cms",
        "nb_pages":                "nb_pages",
        "has_blog":                "blog",
        "blog_url":                "blog_url",
        "has_rss":                 "rss",
        "derniere_maj_blog":       "derniere_maj_blog",
        "frequence_publication":   "frequence_publication",
        "activite_status":         "activite",
        "derniere_date":           "derniere_maj_site",
        "has_sitemap":             "sitemap",
        "pages_sans_meta_desc":    "pages_sans_meta_desc",
        "pages_sans_h1":           "pages_sans_h1",
        "mots_moy_page":           "mots_moy_page",
        "mots_moyen_par_page":     "mots_moy_page",
        "prospect_summary":        "resume",
    }
    existing = {k: v for k, v in col_map.items() if k in df.columns}

    # Deduplicate if both "mots_moy_page" variants are present
    seen_targets: set[str] = set()
    deduped: dict[str, str] = {}
    for src, tgt in existing.items():
        if tgt not in seen_targets:
            deduped[src] = tgt
            seen_targets.add(tgt)

    # Keep only companies with a score > 0
    df = df[df["prospect_score"] > 0].copy()

    final_df = (
        df.reindex(columns=list(deduped.keys()))
        .rename(columns=deduped)
        .sort_values(by="score", ascending=False)
    )

    final_df.to_csv(output_path, index=False)
    logger.info(
        "create_prospect_scoring_v2 → %d prospects in final report '%s'",
        len(final_df), output_path,
    )
    return output_path


# ============================================================================
# LEGACY SCORING (kept for reference)
# ============================================================================

def create_prospect_scoring(input_path: str, output_path: str) -> str:
    """Lighthouse-based scoring (legacy, v1).

    .. deprecated::
        Use :func:`create_prospect_scoring_v2` instead.
        This function is retained for backward compatibility only.
    """
    logger.warning(
        "create_prospect_scoring (v1 / Lighthouse) is deprecated. "
        "Use create_prospect_scoring_v2 instead."
    )
    df = pd.read_csv(input_path)

    score_cols = ["performance", "accessibilite", "bonnes_pratiques", "seo", "prospect_score"]
    for col in score_cols:
        df[col] = 0.0
    df["prospect_summary"] = ""

    if "lighthouse_report_path" not in df.columns:
        df["lighthouse_report_path"] = ""

    for index, row in df.iterrows():
        report_path = row["lighthouse_report_path"]
        if pd.isna(report_path) or not str(report_path).endswith(".json"):
            continue
        try:
            with open(report_path, "r", encoding="utf-8") as fh:
                report = json.load(fh)
            perf = report["categories"]["performance"]["score"] or 0
            acc  = report["categories"]["accessibility"]["score"] or 0
            bp   = report["categories"]["best-practices"]["score"] or 0
            seo  = report["categories"]["seo"]["score"] or 0

            prospect_score = ((1 - seo) * 1.5 + (1 - perf) * 1.2 + (1 - acc) * 0.8) / 3.5 * 10
            prospect_score = max(1, min(10, round(prospect_score, 1)))

            df.loc[index, "performance"]       = int(perf * 100)
            df.loc[index, "accessibilite"]     = int(acc * 100)
            df.loc[index, "bonnes_pratiques"]  = int(bp * 100)
            df.loc[index, "seo"]               = int(seo * 100)
            df.loc[index, "prospect_score"]    = prospect_score

            summary = f"Score de prospection: {prospect_score}/10. "
            if prospect_score > 7:
                summary += "Excellent prospect. Points faibles majeurs en "
                if seo  < 0.8: summary += "SEO, "
                if perf < 0.7: summary += "Performance, "
            elif prospect_score > 4:
                summary += "Prospect modéré. Améliorations possibles en "
                if seo  < 0.9: summary += "SEO, "
                if perf < 0.8: summary += "Performance, "
            else:
                summary += "Prospect faible. Site déjà bien optimisé."
            df.loc[index, "prospect_summary"] = summary.strip(", ") + "."

        except (FileNotFoundError, json.JSONDecodeError, KeyError) as exc:
            logger.error("Error reading Lighthouse report '%s': %s", report_path, exc)
            df.loc[index, "prospect_summary"] = "Erreur d'analyse du rapport."

    final_cols = [
        "siren", "denominationUniteLegale", "trancheEffectifsUniteLegale",
        "site_web", "prospect_score", "performance", "seo", "accessibilite",
        "bonnes_pratiques", "prospect_summary", "lighthouse_report_path",
    ]
    final_df = (
        df.reindex(columns=final_cols).sort_values(by="prospect_score", ascending=False)
    )
    final_df.to_csv(output_path, index=False)
    logger.info("create_prospect_scoring (v1) → '%s'", output_path)
    return output_path


# ============================================================================
# STANDALONE USAGE (for direct script invocation / debugging)
# ============================================================================

if __name__ == "__main__":
    # Legacy standalone usage kept for debugging convenience
    INPUT_CSV        = "Results/websites_results.csv"
    FILTERED_CSV     = "Results/filtered_companies.csv"
    VERIFIED_CSV     = "Results/verified_websites.csv"
    LIGHTHOUSE_CSV   = "Results/lighthouse_reports.csv"
    FINAL_REPORT_CSV = "Results/final_prospect_report.csv"

    filtered_file   = filter_companies_by_employees(INPUT_CSV, FILTERED_CSV)
    verified_file   = verify_websites_by_domain(filtered_file, VERIFIED_CSV)
    lighthouse_file = run_lighthouse_reports(verified_file, LIGHTHOUSE_CSV)
    create_prospect_scoring(lighthouse_file, FINAL_REPORT_CSV)

    logger.info("Processus terminé.")
