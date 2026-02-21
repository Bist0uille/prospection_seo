"""
Unit tests for Scripts/seo_auditor.py helper functions.

All network calls are mocked — no actual HTTP requests are made.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from bs4 import BeautifulSoup

from Scripts.seo_auditor import (
    _compute_publication_frequency,
    _detect_cms,
    _detect_rss,
    _extract_dates,
    _get_internal_links,
    _parse_date,
    _extract_text_words,
)


# ============================================================================
# _detect_cms
# ============================================================================

class TestDetectCms:
    def test_wordpress_wp_content(self):
        html = '<link rel="stylesheet" href="/wp-content/themes/main.css">'
        assert _detect_cms(html) == "WordPress"

    def test_wix_static_domain(self):
        html = '<img src="https://static.wixstatic.com/image.jpg">'
        assert _detect_cms(html) == "Wix"

    def test_shopify_cdn(self):
        html = '<script src="https://cdn.shopify.com/app.js"></script>'
        assert _detect_cms(html) == "Shopify"

    def test_squarespace_domain(self):
        html = '<link href="https://static1.squarespace.com/style.css">'
        assert _detect_cms(html) == "Squarespace"

    def test_webflow_domain(self):
        html = '<script src="https://assets.website-files.com/script.js"></script>'
        assert _detect_cms(html) == "Webflow"

    def test_joomla_signature(self):
        html = "<!-- Joomla! -->"
        assert _detect_cms(html) == "Joomla"

    def test_drupal_signature(self):
        html = '<script src="/sites/default/files/drupal.js"></script>'
        assert _detect_cms(html) == "Drupal"

    def test_unknown_cms_returns_none(self):
        html = "<html><body><p>Plain site</p></body></html>"
        assert _detect_cms(html) is None

    def test_empty_html_returns_none(self):
        assert _detect_cms("") is None


# ============================================================================
# _parse_date
# ============================================================================

class TestParseDate:
    def test_dash_format(self):
        dt = _parse_date("2023-06-15")
        assert dt == datetime(2023, 6, 15)

    def test_slash_format(self):
        dt = _parse_date("2023/06/15")
        assert dt == datetime(2023, 6, 15)

    def test_invalid_returns_none(self):
        assert _parse_date("not-a-date") is None

    def test_incomplete_date_returns_none(self):
        assert _parse_date("2023-06") is None

    def test_boundary_date(self):
        dt = _parse_date("2020-01-01")
        assert dt is not None
        assert dt.year == 2020


# ============================================================================
# _compute_publication_frequency
# ============================================================================

class TestComputePublicationFrequency:
    def _dt(self, *args) -> datetime:
        return datetime(*args)

    def test_weekly(self):
        dates = [self._dt(2024, 1, 1), self._dt(2024, 1, 8), self._dt(2024, 1, 15)]
        assert _compute_publication_frequency(dates) == "hebdomadaire"

    def test_monthly(self):
        dates = [self._dt(2024, 1, 1), self._dt(2024, 2, 1), self._dt(2024, 3, 1)]
        assert _compute_publication_frequency(dates) == "mensuelle"

    def test_quarterly(self):
        dates = [self._dt(2024, 1, 1), self._dt(2024, 4, 1), self._dt(2024, 7, 1)]
        assert _compute_publication_frequency(dates) == "trimestrielle"

    def test_rare(self):
        dates = [self._dt(2022, 1, 1), self._dt(2023, 6, 1), self._dt(2024, 1, 1)]
        assert _compute_publication_frequency(dates) == "rare"

    def test_single_date_returns_none(self):
        assert _compute_publication_frequency([self._dt(2024, 1, 1)]) is None

    def test_empty_list_returns_none(self):
        assert _compute_publication_frequency([]) is None

    def test_duplicate_dates_ignored(self):
        # Two identical dates yield no positive interval → None
        dates = [self._dt(2024, 1, 1), self._dt(2024, 1, 1)]
        result = _compute_publication_frequency(dates)
        # No positive intervals → None
        assert result is None


# ============================================================================
# _extract_dates
# ============================================================================

class TestExtractDates:
    def _soup(self, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "html.parser")

    def test_time_tag_datetime_attr(self):
        soup = self._soup('<time datetime="2023-06-15">15 juin</time>')
        dates = _extract_dates(soup, "https://example.fr/article")
        assert "2023-06-15" in dates

    def test_time_tag_text_content(self):
        soup = self._soup("<time>2023-06-15</time>")
        dates = _extract_dates(soup, "https://example.fr")
        assert "2023-06-15" in dates

    def test_url_contains_date(self):
        soup = self._soup("<p>No dates here</p>")
        dates = _extract_dates(soup, "https://example.fr/2023/06/15/article")
        assert any("2023" in d for d in dates)

    def test_no_dates_returns_empty(self):
        soup = self._soup("<p>No dates here at all</p>")
        dates = _extract_dates(soup, "https://example.fr/")
        assert dates == []

    def test_schema_org_date_published(self):
        html = """<script type="application/ld+json">
        {"@type": "Article", "datePublished": "2023-06-15"}
        </script>"""
        soup = self._soup(html)
        dates = _extract_dates(soup, "https://example.fr/")
        assert "2023-06-15" in dates


# ============================================================================
# _extract_text_words
# ============================================================================

class TestExtractTextWords:
    def _soup(self, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "html.parser")

    def test_extracts_paragraph_text(self):
        soup = self._soup("<p>Hello World</p>")
        words = _extract_text_words(soup)
        assert "Hello" in words
        assert "World" in words

    def test_ignores_script_content(self):
        soup = self._soup("<script>var x = 'hidden';</script><p>visible</p>")
        words = _extract_text_words(soup)
        assert "hidden" not in words
        assert "visible" in words

    def test_ignores_style_content(self):
        soup = self._soup("<style>.cls { color: red; }</style><p>text</p>")
        words = _extract_text_words(soup)
        assert "color" not in words

    def test_empty_page_returns_empty(self):
        soup = self._soup("<html><body></body></html>")
        words = _extract_text_words(soup)
        assert words == []


# ============================================================================
# _get_internal_links
# ============================================================================

class TestGetInternalLinks:
    def _soup(self, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "html.parser")

    def test_returns_internal_links(self):
        html = '<a href="/page">Page</a>'
        soup = self._soup(html)
        links = _get_internal_links(soup, "https://example.fr", "example.fr")
        assert "https://example.fr/page" in links

    def test_excludes_external_links(self):
        html = '<a href="https://other.com/page">External</a>'
        soup = self._soup(html)
        links = _get_internal_links(soup, "https://example.fr", "example.fr")
        assert len(links) == 0

    def test_excludes_binary_files(self):
        html = '<a href="/file.pdf">PDF</a><a href="/image.jpg">Image</a>'
        soup = self._soup(html)
        links = _get_internal_links(soup, "https://example.fr", "example.fr")
        assert len(links) == 0

    def test_handles_www_subdomain(self):
        html = '<a href="https://www.example.fr/about">About</a>'
        soup = self._soup(html)
        links = _get_internal_links(soup, "https://example.fr", "example.fr")
        assert "https://www.example.fr/about" in links

    def test_trailing_slash_stripped(self):
        html = '<a href="/page/">Page</a>'
        soup = self._soup(html)
        links = _get_internal_links(soup, "https://example.fr", "example.fr")
        # Should be without trailing slash
        assert "https://example.fr/page" in links

    def test_deduplication(self):
        html = '<a href="/page">P1</a><a href="/page/">P2</a>'
        soup = self._soup(html)
        links = _get_internal_links(soup, "https://example.fr", "example.fr")
        # /page and /page/ should resolve to the same cleaned URL
        assert len(links) == 1


# ============================================================================
# _detect_rss
# ============================================================================

class TestDetectRss:
    def _soup(self, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "html.parser")

    def test_detects_rss_feed(self):
        html = '<link rel="alternate" type="application/rss+xml" href="/feed">'
        assert _detect_rss(self._soup(html)) is True

    def test_detects_atom_feed(self):
        html = '<link rel="alternate" type="application/atom+xml" href="/feed.atom">'
        assert _detect_rss(self._soup(html)) is True

    def test_no_feed_returns_false(self):
        html = '<link rel="stylesheet" type="text/css" href="/style.css">'
        assert _detect_rss(self._soup(html)) is False
