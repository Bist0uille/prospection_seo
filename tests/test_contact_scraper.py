"""
Unit tests for Scripts/contact_scraper.py helper functions.

All network calls are mocked — no actual HTTP requests are made.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from bs4 import BeautifulSoup

from Scripts.contact_scraper import (
    _extract_email,
    _extract_phone,
    _find_contact_link,
    _normalize_phone,
    extract_contacts,
    run_contact_extraction,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


# ============================================================================
# _extract_email
# ============================================================================

class TestExtractEmail:
    def test_mailto_link_priority(self):
        """Priority 1: <a href="mailto:..."> is extracted reliably."""
        html = '<a href="mailto:contact@example.fr">Nous écrire</a>'
        assert _extract_email(_soup(html)) == "contact@example.fr"

    def test_mailto_with_subject_stripped(self):
        """Query params after '?' in mailto must be stripped."""
        html = '<a href="mailto:info@test.fr?subject=Hello">Mail</a>'
        assert _extract_email(_soup(html)) == "info@test.fr"

    def test_regex_fallback_plain_text(self):
        """Fallback: email in plain page text when no mailto link exists."""
        html = "<p>Contactez-nous : info@monsite.fr pour plus d'infos.</p>"
        assert _extract_email(_soup(html)) == "info@monsite.fr"

    def test_regex_fallback_ignores_image_extensions(self):
        """Regex fallback must ignore pseudo-emails with image extensions."""
        html = "<p>Image: logo@banner.png disponible ici.</p>"
        assert _extract_email(_soup(html)) is None

    def test_regex_fallback_ignores_example_domain(self):
        """Regex fallback must ignore addresses at example.com."""
        html = "<p>Send to test@example.com</p>"
        assert _extract_email(_soup(html)) is None

    def test_regex_fallback_ignores_sentry_domain(self):
        """Regex fallback must ignore addresses at sentry.io."""
        html = "<p>Error reported to abc123@sentry.io</p>"
        assert _extract_email(_soup(html)) is None

    def test_no_email_returns_none(self):
        """Page without any email should return None."""
        html = "<p>Pas d'adresse email ici.</p>"
        assert _extract_email(_soup(html)) is None

    def test_mailto_takes_priority_over_regex(self):
        """Mailto link must be preferred over a regex match in plain text."""
        html = (
            '<a href="mailto:preferred@site.fr">Mail</a>'
            "<p>Autre: other@site.fr</p>"
        )
        assert _extract_email(_soup(html)) == "preferred@site.fr"


# ============================================================================
# _extract_phone
# ============================================================================

class TestExtractPhone:
    def test_tel_link_priority(self):
        """Priority 1: <a href="tel:..."> is extracted and normalised."""
        html = '<a href="tel:0556123456">Appelez-nous</a>'
        result = _extract_phone(_soup(html))
        assert result == "05 56 12 34 56"

    def test_tel_link_with_spaces(self):
        """Tel link with spaces in the number is normalised correctly."""
        html = '<a href="tel:05 56 12 34 56">Tel</a>'
        result = _extract_phone(_soup(html))
        assert result == "05 56 12 34 56"

    def test_regex_fallback_plain_text(self):
        """Fallback: phone number in page text when no tel link exists."""
        html = "<p>Contactez-nous au 05 56 12 34 56 pour un devis.</p>"
        result = _extract_phone(_soup(html))
        assert result == "05 56 12 34 56"

    def test_regex_fallback_dots_format(self):
        """Fallback: dotted French phone format (05.56.12.34.56)."""
        html = "<p>Tel: 05.56.12.34.56</p>"
        result = _extract_phone(_soup(html))
        assert result == "05 56 12 34 56"

    def test_no_phone_returns_none(self):
        """Page without any phone should return None."""
        html = "<p>Aucune information de contact ici.</p>"
        assert _extract_phone(_soup(html)) is None


# ============================================================================
# _normalize_phone
# ============================================================================

class TestNormalizePhone:
    def test_10_digits_no_separator(self):
        assert _normalize_phone("0556123456") == "05 56 12 34 56"

    def test_dots_format(self):
        assert _normalize_phone("05.56.12.34.56") == "05 56 12 34 56"

    def test_dashes_format(self):
        assert _normalize_phone("05-56-12-34-56") == "05 56 12 34 56"

    def test_spaces_format(self):
        assert _normalize_phone("05 56 12 34 56") == "05 56 12 34 56"

    def test_plus33_format(self):
        assert _normalize_phone("+33556123456") == "05 56 12 34 56"

    def test_plus33_with_spaces(self):
        assert _normalize_phone("+33 5 56 12 34 56") == "05 56 12 34 56"

    def test_33_prefix_without_plus(self):
        assert _normalize_phone("33556123456") == "05 56 12 34 56"

    def test_invalid_short_number(self):
        assert _normalize_phone("0123") is None

    def test_invalid_too_long(self):
        assert _normalize_phone("055612345678") is None

    def test_not_starting_with_zero(self):
        # 10 digits but not starting with 0 after stripping +33 → invalid
        assert _normalize_phone("1556123456") is None


# ============================================================================
# _find_contact_link
# ============================================================================

class TestFindContactLink:
    def test_finds_contact_text_in_nav(self):
        html = '<nav><a href="/contact">Contact</a></nav>'
        soup = _soup(html)
        result = _find_contact_link(soup, "https://example.fr")
        assert result == "https://example.fr/contact"

    def test_finds_nous_contacter(self):
        html = '<a href="/nous-contacter">Nous contacter</a>'
        soup = _soup(html)
        result = _find_contact_link(soup, "https://example.fr")
        assert result == "https://example.fr/nous-contacter"

    def test_finds_keyword_in_href(self):
        html = '<a href="/page-contact-entreprise">Joindre l\'équipe</a>'
        soup = _soup(html)
        result = _find_contact_link(soup, "https://example.fr")
        assert result == "https://example.fr/page-contact-entreprise"

    def test_ignores_external_domain(self):
        html = '<a href="https://other.com/contact">Contact externe</a>'
        soup = _soup(html)
        result = _find_contact_link(soup, "https://example.fr")
        assert result is None

    def test_no_contact_link_returns_none(self):
        html = '<a href="/produits">Produits</a><a href="/about">À propos</a>'
        soup = _soup(html)
        result = _find_contact_link(soup, "https://example.fr")
        assert result is None

    def test_absolute_internal_url(self):
        html = '<a href="https://example.fr/nous-contacter">Contact</a>'
        soup = _soup(html)
        result = _find_contact_link(soup, "https://example.fr")
        assert result == "https://example.fr/nous-contacter"


# ============================================================================
# extract_contacts
# ============================================================================

class TestExtractContacts:
    def test_empty_url_returns_none_values(self):
        """Empty or nan URL should return None for both fields without HTTP calls."""
        assert extract_contacts("") == {"email_contact": None, "telephone": None}
        assert extract_contacts("nan") == {"email_contact": None, "telephone": None}

    def test_pass1a_follows_nav_contact_link(self):
        """Pass 1a: email found after following the contact link from homepage."""
        homepage = MagicMock()
        homepage.status_code = 200
        homepage.text = '<nav><a href="/contact">Contact</a></nav>'

        contact_page = MagicMock()
        contact_page.status_code = 200
        contact_page.text = '<a href="mailto:hello@test.fr">Mail</a>'

        def fake_get(url, **kw):
            if url.endswith("/contact"):
                return contact_page
            return homepage

        with patch("Scripts.contact_scraper._safe_get", side_effect=fake_get):
            result = extract_contacts("https://test.fr")

        assert result["email_contact"] == "hello@test.fr"

    def test_pass1b_falls_back_to_homepage_content(self):
        """Pass 1b: no contact link in nav, email found inline on homepage."""
        homepage = MagicMock()
        homepage.status_code = 200
        homepage.text = '<footer><a href="mailto:info@site.fr">Mail</a></footer>'

        # No contact link in nav → pass 1a finds nothing, pass 1b extracts from homepage
        with patch("Scripts.contact_scraper._safe_get", return_value=homepage):
            result = extract_contacts("https://site.fr")

        assert result["email_contact"] == "info@site.fr"

    def test_pass1c_hardcoded_paths_fallback(self):
        """Pass 1c: homepage has no contact link and no inline email, tries /contact path."""
        empty_home = MagicMock()
        empty_home.status_code = 200
        empty_home.text = "<p>Accueil sans contact.</p>"

        contact_page = MagicMock()
        contact_page.status_code = 200
        contact_page.text = '<a href="tel:0556123456">Appelez</a>'

        def fake_get(url, **kw):
            if url.endswith("/contact"):
                return contact_page
            return empty_home

        with patch("Scripts.contact_scraper._safe_get", side_effect=fake_get):
            result = extract_contacts("https://example.fr")

        assert result["telephone"] == "05 56 12 34 56"

    def test_no_contact_found_calls_playwright(self):
        """When all requests pass fail, Playwright fallback is called."""
        with patch("Scripts.contact_scraper._safe_get", return_value=None), \
             patch("Scripts.contact_scraper._extract_contacts_playwright",
                   return_value={"email_contact": "pw@test.fr", "telephone": None}) as mock_pw:
            result = extract_contacts("https://example.fr")

        mock_pw.assert_called_once()
        assert result["email_contact"] == "pw@test.fr"

    def test_no_contact_found_returns_none_values(self):
        """When no email or phone found on any page or playwright, both are None."""
        empty = MagicMock()
        empty.status_code = 200
        empty.text = "<p>Aucun contact.</p>"

        with patch("Scripts.contact_scraper._safe_get", return_value=empty), \
             patch("Scripts.contact_scraper._extract_contacts_playwright",
                   return_value={"email_contact": None, "telephone": None}):
            result = extract_contacts("https://example.fr")

        assert result == {"email_contact": None, "telephone": None}


# ============================================================================
# run_contact_extraction
# ============================================================================

class TestRunContactExtraction:
    def test_csv_round_trip(self, tmp_path):
        """CSV in → enriched CSV out with email_contact and telephone columns."""
        input_data = pd.DataFrame({
            "entreprise":  ["ACME Marine", "SeaTech"],
            "site_web":    ["https://acmemarine.fr", "https://seatech.fr"],
            "score":       [7.0, 5.0],
        })
        input_csv = tmp_path / "report.csv"
        output_csv = tmp_path / "enriched.csv"
        input_data.to_csv(input_csv, index=False)

        mock_contacts = [
            {"email_contact": "contact@acmemarine.fr", "telephone": "05 56 00 11 22"},
            {"email_contact": None, "telephone": None},
        ]

        with patch("Scripts.contact_scraper.extract_contacts", side_effect=mock_contacts), \
             patch("Scripts.contact_scraper.time.sleep"):  # avoid real sleep
            run_contact_extraction(str(input_csv), str(output_csv))

        result = pd.read_csv(output_csv)
        assert "email_contact" in result.columns
        assert "telephone" in result.columns
        assert result.loc[0, "email_contact"] == "contact@acmemarine.fr"
        assert result.loc[0, "telephone"] == "05 56 00 11 22"
        assert pd.isna(result.loc[1, "email_contact"])
        assert pd.isna(result.loc[1, "telephone"])

    def test_existing_columns_overwritten(self, tmp_path):
        """If email_contact / telephone already exist in CSV, values are updated."""
        input_data = pd.DataFrame({
            "entreprise":    ["ACME Marine"],
            "site_web":      ["https://acmemarine.fr"],
            "email_contact": ["old@old.fr"],
            "telephone":     ["00 00 00 00 00"],
        })
        csv_path = tmp_path / "report.csv"
        input_data.to_csv(csv_path, index=False)

        with patch("Scripts.contact_scraper.extract_contacts",
                   return_value={"email_contact": "new@acmemarine.fr", "telephone": "01 23 45 67 89"}), \
             patch("Scripts.contact_scraper.time.sleep"):
            run_contact_extraction(str(csv_path), str(csv_path))

        result = pd.read_csv(csv_path)
        assert result.loc[0, "email_contact"] == "new@acmemarine.fr"
        assert result.loc[0, "telephone"] == "01 23 45 67 89"
