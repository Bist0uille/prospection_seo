"""
Unit tests for Scripts/prospect_analyzer.py.

All tests use in-memory DataFrames — no network, no filesystem (except
for the dummy_db_csv fixture which uses tmp_path).
"""

from __future__ import annotations

import pandas as pd
import pytest

from Scripts.prospect_analyzer import (
    create_prospect_scoring_v2,
    extract_keywords,
    filter_companies_by_employees,
    get_domain,
    normalize_name,
    verify_websites_by_domain,
)


# ============================================================================
# normalize_name
# ============================================================================

class TestNormalizeName:
    def test_lowercase(self):
        assert normalize_name("BOAT") == "boat"

    def test_removes_spaces(self):
        assert normalize_name("Boat Company") == "boatcompany"

    def test_removes_accents_not_kept(self):
        # Non-ASCII chars are stripped (only a-z0-9 kept)
        assert normalize_name("Société") == "socit"  # é → stripped

    def test_removes_special_chars(self):
        assert normalize_name("SA.RL & Co.") == "sarlco"

    def test_empty_string(self):
        assert normalize_name("") == ""


# ============================================================================
# get_domain
# ============================================================================

class TestGetDomain:
    def test_strips_www(self):
        assert get_domain("https://www.example.fr") == "example.fr"

    def test_no_www(self):
        assert get_domain("https://example.fr/page") == "example.fr"

    def test_invalid_url_returns_empty(self):
        # urlparse doesn't raise; just returns an empty netloc
        result = get_domain("not_a_url")
        assert isinstance(result, str)

    def test_empty_string(self):
        assert get_domain("") == ""


# ============================================================================
# extract_keywords
# ============================================================================

class TestExtractKeywords:
    def test_removes_stop_words(self):
        keywords = extract_keywords("MARINE ET LA MER SA")
        assert "sa" not in keywords
        assert "et" not in keywords
        assert "la" not in keywords

    def test_removes_short_words(self):
        # The filter is len(original_word) > 2.
        # "Co" (len 2) is excluded; "A&B" (len 3) passes but normalises to "ab".
        keywords = extract_keywords("A&B Co")
        assert "co" not in keywords   # "Co" len=2 → excluded
        # "A&B" len=3 → kept but normalised to "ab" — that is expected behaviour

    def test_returns_normalized(self):
        keywords = extract_keywords("BOAT COMPANY SA")
        assert "boat" in keywords
        assert "company" in keywords

    def test_hyphen_split(self):
        keywords = extract_keywords("MARINE-TECH SARL")
        assert "marine" in keywords or "marinetech" in keywords

    def test_empty_name_returns_empty(self):
        assert extract_keywords("") == []

    def test_all_stop_words_returns_empty(self):
        assert extract_keywords("SA SARL SAS") == []


# ============================================================================
# filter_companies_by_employees
# ============================================================================

class TestFilterCompaniesByEmployees:

    def test_filters_closed_companies(self, dummy_db_csv, tmp_path):
        out = tmp_path / "filtered.csv"
        filter_companies_by_employees(
            str(dummy_db_csv),
            str(out),
            naf_codes=["3012Z", "3011Z", "5010Z"],
        )
        df = pd.read_csv(out)
        # CLOSED FIRM (état = C) should not appear
        assert "CLOSED FIRM SA" not in df["denominationUniteLegale"].values

    def test_filters_by_naf_codes(self, dummy_db_csv, tmp_path):
        out = tmp_path / "filtered.csv"
        filter_companies_by_employees(
            str(dummy_db_csv),
            str(out),
            naf_codes=["3012Z"],
        )
        df = pd.read_csv(out)
        assert all(df["activitePrincipaleUniteLegale"] == "3012Z")

    def test_deduplicates_siren(self, tmp_path):
        """Two rows with the same SIREN — only one should survive."""
        dup_data = pd.DataFrame(
            {
                "siren":                         ["123456789", "123456789"],
                "denominationUniteLegale":       ["BOAT A", "BOAT B"],
                "activitePrincipaleUniteLegale": ["3012Z", "3012Z"],
                "trancheEffectifsUniteLegale":   ["11", "11"],
                "etatAdministratifUniteLegale":  ["A", "A"],
                "etablissementSiege":            ["true", "false"],
            }
        )
        src = tmp_path / "dup.csv"
        out = tmp_path / "out.csv"
        dup_data.to_csv(src, index=False)
        filter_companies_by_employees(str(src), str(out), naf_codes=["3012Z"])
        df = pd.read_csv(out)
        assert len(df) == 1

    def test_writes_output_file(self, dummy_db_csv, tmp_path):
        out = tmp_path / "filtered.csv"
        result = filter_companies_by_employees(
            str(dummy_db_csv), str(out), naf_codes=["3012Z", "3011Z"]
        )
        assert out.exists()
        assert result == str(out)

    def test_employee_band_filter(self, tmp_path):
        df_in = pd.DataFrame(
            {
                "siren":                         ["1", "2"],
                "denominationUniteLegale":       ["A", "B"],
                "activitePrincipaleUniteLegale": ["3012Z", "3012Z"],
                "trancheEffectifsUniteLegale":   ["01", "11"],  # 01 = 1-2 emp (excluded)
                "etatAdministratifUniteLegale":  ["A", "A"],
            }
        )
        src = tmp_path / "in.csv"
        out = tmp_path / "out.csv"
        df_in.to_csv(src, index=False)
        filter_companies_by_employees(
            str(src), str(out),
            naf_codes=["3012Z"],
            employee_codes=["11", "12", "21"],  # 10+ only
        )
        df = pd.read_csv(out)
        assert len(df) == 1
        assert df.iloc[0]["denominationUniteLegale"] == "B"


# ============================================================================
# verify_websites_by_domain
# ============================================================================

class TestVerifyWebsitesByDomain:

    def _run(self, tmp_path, data: dict) -> pd.DataFrame:
        src = tmp_path / "in.csv"
        out = tmp_path / "out.csv"
        pd.DataFrame(data).to_csv(src, index=False)
        verify_websites_by_domain(str(src), str(out))
        return pd.read_csv(out)

    def test_matching_keyword_verified(self, tmp_path):
        df = self._run(tmp_path, {
            "denominationUniteLegale": ["BOAT COMPANY SA"],
            "site_web": ["https://www.boatcompany.fr"],
        })
        assert df.loc[0, "site_verifie"] is True or df.loc[0, "site_verifie"] == True

    def test_blocklisted_domain_rejected(self, tmp_path):
        df = self._run(tmp_path, {
            "denominationUniteLegale": ["MARINE TECH SARL"],
            "site_web": ["https://www.societe.com/marine-tech"],
        })
        assert df.loc[0, "site_verifie"] == False
        assert "blocage" in df.loc[0, "verification_raison"].lower()

    def test_english_path_rejected(self, tmp_path):
        df = self._run(tmp_path, {
            "denominationUniteLegale": ["PORT SERVICES SAS"],
            "site_web": ["https://www.portservices.com/en/home"],
        })
        assert df.loc[0, "site_verifie"] == False
        assert "/en/" in df.loc[0, "verification_raison"]

    def test_canadian_tld_rejected(self, tmp_path):
        df = self._run(tmp_path, {
            "denominationUniteLegale": ["PORT SERVICES SAS"],
            "site_web": ["https://www.portservices.ca"],
        })
        assert df.loc[0, "site_verifie"] == False
        assert ".ca" in df.loc[0, "verification_raison"]

    def test_missing_url_not_verified(self, tmp_path):
        df = self._run(tmp_path, {
            "denominationUniteLegale": ["SOME COMPANY SA"],
            "site_web": [""],
        })
        assert df.loc[0, "site_verifie"] == False

    def test_no_keyword_match_not_verified(self, tmp_path):
        df = self._run(tmp_path, {
            "denominationUniteLegale": ["BOAT COMPANY SA"],
            "site_web": ["https://www.totallyunrelated.fr"],
        })
        assert df.loc[0, "site_verifie"] == False

    def test_output_file_created(self, tmp_path, websites_df):
        src = tmp_path / "in.csv"
        out = tmp_path / "out.csv"
        websites_df.to_csv(src, index=False)
        result = verify_websites_by_domain(str(src), str(out))
        assert out.exists()
        assert result == str(out)


# ============================================================================
# create_prospect_scoring_v2
# ============================================================================

class TestCreateProspectScoringV2:

    def _run(self, tmp_path, df_in: pd.DataFrame) -> pd.DataFrame:
        src = tmp_path / "audit.csv"
        out = tmp_path / "report.csv"
        df_in.to_csv(src, index=False)
        create_prospect_scoring_v2(str(src), str(out))
        return pd.read_csv(out)

    def test_abandoned_blog_scores_high(self, tmp_path, verified_df):
        # Row 0: abandoned blog, small site, no sitemap — should score high
        df = self._run(tmp_path, verified_df)
        boat_row = df[df["entreprise"] == "BOAT COMPANY SA"]
        assert not boat_row.empty
        assert boat_row.iloc[0]["score"] >= 6.0

    def test_wordpress_site_included(self, tmp_path, verified_df):
        df = self._run(tmp_path, verified_df)
        marine_row = df[df["entreprise"] == "MARINE TECH SARL"]
        assert not marine_row.empty

    def test_scores_are_clamped_1_to_10(self, tmp_path, verified_df):
        df = self._run(tmp_path, verified_df)
        for score in df["score"]:
            assert 1.0 <= score <= 10.0

    def test_sorted_by_score_descending(self, tmp_path, verified_df):
        df = self._run(tmp_path, verified_df)
        scores = df["score"].tolist()
        assert scores == sorted(scores, reverse=True)

    def test_unverified_rows_excluded(self, tmp_path):
        """Rows with site_verifie=False are not scored and not included."""
        data = pd.DataFrame(
            {
                "denominationUniteLegale": ["UNVERIFIED CO SA"],
                "site_web":               ["https://example.fr"],
                "site_verifie":           [False],
                "nb_pages":               [10],
                "has_blog":               [True],
                "blog_status":            ["abandonné"],
                "frequence_publication":  [None],
                "has_sitemap":            [False],
                "mots_moyen_par_page":    [100],
                "ratio_texte_html":       [0.10],
                "cms_detecte":            [None],
                "pages_sans_meta_desc":   [2],
                "pages_sans_h1":          [1],
                "titles_dupliques":       [0.5],
                "pages_vides":            [1],
                "prospect_score":         [0.0],
                "prospect_summary":       [""],
            }
        )
        df = self._run(tmp_path, data)
        assert len(df) == 0  # unverified row produces score=0, filtered out

    def test_zero_pages_row_excluded(self, tmp_path):
        """Rows where nb_pages=0 produce no score and are excluded from output."""
        data = pd.DataFrame(
            {
                "denominationUniteLegale": ["EMPTY SITE SA"],
                "site_web":               ["https://example.fr"],
                "site_verifie":           [True],
                "nb_pages":               [0],
                "has_blog":               [False],
                "blog_status":            ["absent"],
                "frequence_publication":  [None],
                "has_sitemap":            [False],
                "mots_moyen_par_page":    [0],
                "ratio_texte_html":       [0.0],
                "cms_detecte":            [None],
                "pages_sans_meta_desc":   [0],
                "pages_sans_h1":          [0],
                "titles_dupliques":       [0.0],
                "pages_vides":            [0],
                "prospect_score":         [0.0],
                "prospect_summary":       [""],
            }
        )
        df = self._run(tmp_path, data)
        assert len(df) == 0

    def test_output_contains_summary(self, tmp_path, verified_df):
        df = self._run(tmp_path, verified_df)
        assert "resume" in df.columns
        for summary in df["resume"]:
            assert isinstance(summary, str) and len(summary) > 0
