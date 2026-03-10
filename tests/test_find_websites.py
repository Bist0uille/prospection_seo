"""
Unit tests for Scripts/find_websites.py — fonctions pures uniquement.

Aucun appel réseau, aucun appel DDG.
"""
from __future__ import annotations

import pytest

from Scripts.find_websites import (
    _extract_alias,
    _extract_keywords,
    _is_secteur_ok,
    _compute_confidence,
    _filter_candidates,
    normalize_name,
    _candidate_urls,
    CONFIDENCE_THRESHOLD,
)


# ============================================================================
# normalize_name
# ============================================================================

class TestNormalizeName:
    def test_lowercase(self):
        assert normalize_name("BATEAU") == "bateau"

    def test_strips_non_alphanum(self):
        # Les accents ne sont PAS normalisés — ils sont supprimés (ô → absent)
        assert normalize_name("Côte d'Azur") == "ctedazur"

    def test_removes_spaces(self):
        assert normalize_name("CHANTIER NAVAL") == "chantiernaval"


# ============================================================================
# _extract_alias
# ============================================================================

class TestExtractAlias:
    def test_simple(self):
        assert _extract_alias("GUYMARINE (GUYMARINE)") == "GUYMARINE"

    def test_longer_alias(self):
        assert _extract_alias("ROMUALD VIRLOUVET (VIRLOUVET YACHTING)") == "VIRLOUVET YACHTING"

    def test_no_alias(self):
        assert _extract_alias("AP YACHT CONCEPTION") is None

    def test_too_short(self):
        # ≤3 chars → rejeté
        assert _extract_alias("NAUTITECH CATAMARANS (CIM)") is None

    def test_ou_separator(self):
        # Coupe sur " OU ", prend le premier
        assert _extract_alias("EMILIEN FAURENS (FAURSAIL OU FAURENS EMILIEN)") == "FAURSAIL"

    def test_ou_case_insensitive(self):
        assert _extract_alias("SOCIETE (ALPHA ou BETA)") == "ALPHA"

    def test_exactly_three_chars_rejected(self):
        assert _extract_alias("TEST (ABC)") is None

    def test_four_chars_accepted(self):
        assert _extract_alias("TEST (ABCD)") == "ABCD"


# ============================================================================
# _extract_keywords
# ============================================================================

class TestExtractKeywords:
    def test_normal_words(self):
        kws = _extract_keywords("CHANTIER NAVAL COUACH")
        assert "CHANTIER" in kws
        assert "NAVAL" in kws
        assert "COUACH" in kws

    def test_stop_words_excluded(self):
        kws = _extract_keywords("SARL DE LA MER")
        assert "SARL" not in kws
        assert "DE" not in kws
        assert "LA" not in kws

    def test_short_word_excluded(self):
        kws = _extract_keywords("MER ET VOILE")
        # "ET" est un stop word → exclu
        assert "ET" not in kws
        # "MER" est tout-majuscule ≥2 chars → traité comme acronyme, inclus
        assert "MER" in kws
        # "VOILE" ≥4 chars → inclus
        assert "VOILE" in kws

    def test_acronym_accepted(self):
        # Acronymes tout-majuscules ≥2 chars
        kws = _extract_keywords("AP YACHT CONCEPTION")
        assert "AP" in kws
        assert "YACHT" in kws

    def test_mixed_case_not_acronym(self):
        # "Marine" n'est pas tout-majuscule → besoin de ≥4 chars
        kws = _extract_keywords("MARINE SELLERIE")
        assert "MARINE" in kws
        assert "SELLERIE" in kws

    def test_parenthetical_main_word(self):
        # Le nom principal est toujours extrait (les parens restent dans les tokens)
        kws = _extract_keywords("ROSEWEST (DESIGN YACHT)")
        assert "ROSEWEST" in kws


# ============================================================================
# _is_secteur_ok
# ============================================================================

class TestIsSecteurOk:
    def test_french_keyword(self):
        assert _is_secteur_ok("Chantier naval spécialisé dans la construction de bateaux")

    def test_english_keyword(self):
        assert _is_secteur_ok("Boatyard and sailing equipment")

    def test_naf_extra_repair(self):
        # "repair" est dans NAF 3315Z extra
        assert _is_secteur_ok("Boat repair and maintenance services", naf_code="3315Z")

    def test_naf_extra_charter(self):
        assert _is_secteur_ok("Bareboat charter and rental", naf_code="7734Z")

    def test_naf_extra_cruise(self):
        assert _is_secteur_ok("Passenger cruise and excursions", naf_code="5010Z")

    def test_no_keyword(self):
        assert not _is_secteur_ok("Agence de rencontres et sorties culturelles")

    def test_empty_snippet(self):
        assert not _is_secteur_ok("")

    def test_case_insensitive(self):
        # normalize_name met tout en minuscule
        assert _is_secteur_ok("YACHT Club de la Rochelle")

    def test_yacht_keyword(self):
        assert _is_secteur_ok("Naval Shipyard Couach")  # "shipyard" → True

    def test_false_positive_rencontre(self):
        assert not _is_secteur_ok("Site de rencontres transgenres premium")


# ============================================================================
# _compute_confidence
# ============================================================================

class TestComputeConfidence:
    """Tests sur la logique de scoring — mocking HTTP avec monkeypatch."""

    def _mock_response(self, monkeypatch, status: int = 200, text: str = ""):
        import requests

        class FakeResp:
            status_code = status
            def __init__(self, t): self.text = t

        monkeypatch.setattr(
            requests, "get", lambda *a, **kw: FakeResp(text)
        )

    def test_keyword_in_domain_scores_2(self, monkeypatch):
        self._mock_response(monkeypatch, 200, "bateau voilier navigation")
        score, ok, _ = _compute_confidence(
            "https://www.rosewest.fr/", ["ROSEWEST"], "", ""
        )
        # "rosewest" in "rosewestfr" → +2.0, .fr → +0.5
        assert score >= 2.5

    def test_fr_tld_bonus(self, monkeypatch):
        self._mock_response(monkeypatch, 200, "bateau voilier")
        score_fr, _, _ = _compute_confidence("https://example.fr/", ["EXAMPLE"], "", "")
        score_com, _, _ = _compute_confidence("https://example.com/", ["EXAMPLE"], "", "")
        assert score_fr > score_com

    def test_antibot_returns_domain_score(self, monkeypatch):
        import requests

        class FakeResp:
            status_code = 403
            text = ""
        monkeypatch.setattr(requests, "get", lambda *a, **kw: FakeResp())
        score, ok, snippet = _compute_confidence("https://www.test.fr/", ["TEST"], "", "")
        assert score > 0   # score domaine conservé
        assert not ok      # secteur_ok False (pas de contenu)

    def test_404_returns_zero(self, monkeypatch):
        import requests

        class FakeResp:
            status_code = 404
            text = ""
        monkeypatch.setattr(requests, "get", lambda *a, **kw: FakeResp())
        score, ok, snippet = _compute_confidence("https://www.dead.fr/", ["DEAD"], "", "")
        assert score == 0.0

    def test_code_postal_bonus(self, monkeypatch):
        self._mock_response(monkeypatch, 200, "17000 voilier port La Rochelle")
        score_with, _, _ = _compute_confidence("https://x.fr/", ["X"], "17000", "")
        score_without, _, _ = _compute_confidence("https://x.fr/", ["X"], "99999", "")
        assert score_with > score_without

    def test_commune_bonus(self, monkeypatch):
        self._mock_response(monkeypatch, 200, "La Rochelle voilier")
        score_with, _, _ = _compute_confidence("https://x.fr/", ["X"], "", "LA ROCHELLE")
        score_without, _, _ = _compute_confidence("https://x.fr/", ["X"], "", "BORDEAUX")
        assert score_with >= score_without


# ============================================================================
# _filter_candidates
# ============================================================================

class TestFilterCandidates:
    def _make_results(self, urls: list[str]) -> list[dict]:
        return [{"href": u} for u in urls]

    def test_keyword_match(self):
        results = self._make_results(["https://www.guymarine.fr/"])
        candidates = _filter_candidates(results, ["GUYMARINE"])
        assert len(candidates) == 1
        assert candidates[0][2] == "https://www.guymarine.fr/"

    def test_directory_blocked(self):
        results = self._make_results(["https://www.societe.com/guymarine"])
        assert _filter_candidates(results, ["GUYMARINE"]) == []

    def test_canadian_blocked(self):
        results = self._make_results(["https://www.guymarine.ca/"])
        assert _filter_candidates(results, ["GUYMARINE"]) == []

    def test_fr_before_com(self):
        results = self._make_results([
            "https://www.couach.com/",
            "https://www.couach.fr/",
        ])
        candidates = _filter_candidates(results, ["COUACH"])
        assert len(candidates) == 2
        # .fr doit être en premier (tld_priority=0)
        assert candidates[0][2] == "https://www.couach.fr/"

    def test_no_match_returns_empty(self):
        results = self._make_results(["https://www.unrelated.com/"])
        assert _filter_candidates(results, ["GUYMARINE"]) == []

    def test_noise_word_not_used_for_matching(self):
        # "maritime" est dans _DOMAIN_NOISE_WORDS → ne suffit pas pour matcher
        results = self._make_results(["https://www.maritime-services.fr/"])
        candidates = _filter_candidates(results, ["MARITIME"])
        assert candidates == []


# ============================================================================
# _candidate_urls
# ============================================================================

class TestCandidateUrls:
    def test_generates_fr_and_com(self):
        urls = _candidate_urls("GUYMARINE")
        assert any(".fr" in u for u in urls)
        assert any(".com" in u for u in urls)

    def test_strips_legal_suffixes(self):
        urls = _candidate_urls("CHANTIER SARL")
        assert not any("sarl" in u for u in urls)

    def test_empty_name(self):
        assert _candidate_urls("") == []
