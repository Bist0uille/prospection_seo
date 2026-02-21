"""
Shared pytest fixtures for the botparser test suite.
"""

from __future__ import annotations

import pandas as pd
import pytest

from Scripts.core.logging_config import reset_logging


# ── Logging isolation ─────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _reset_logging():
    """Tear down botparser logging handlers between tests.

    Prevents handler accumulation when multiple tests call
    setup_pipeline_logging().
    """
    yield
    reset_logging()


# ── Sector files ──────────────────────────────────────────────────────────────

@pytest.fixture
def sector_file(tmp_path):
    """A valid sector .txt file with 2 APE codes and comment/blank lines."""
    content = (
        "# Secteur test\n"
        "3012Z - Construction de bateaux de plaisance\n"
        "\n"
        "3011Z\n"
        "# another comment\n"
    )
    f = tmp_path / "test_sector.txt"
    f.write_text(content, encoding="utf-8")
    return f


@pytest.fixture
def empty_sector_file(tmp_path):
    """A sector file containing only comments and blank lines (no codes)."""
    content = "# No codes here\n\n# still no codes\n"
    f = tmp_path / "empty_sector.txt"
    f.write_text(content, encoding="utf-8")
    return f


@pytest.fixture
def dummy_db_csv(tmp_path):
    """A minimal INSEE-like companies CSV for pipeline testing."""
    df = pd.DataFrame(
        {
            "siren":                          ["123456789", "987654321", "111222333", "444555666"],
            "denominationUniteLegale":        ["BOAT COMPANY SA", "MARINE TECH SARL", "PORT SERVICES SAS", "CLOSED FIRM SA"],
            "activitePrincipaleUniteLegale":  ["3012Z",           "3011Z",             "5010Z",             "3012Z"],
            "trancheEffectifsUniteLegale":    ["11",              "12",                "21",                "11"],
            "etatAdministratifUniteLegale":   ["A",               "A",                 "A",                 "C"],
            "etablissementSiege":             ["true",            "false",             "true",              "true"],
        }
    )
    path = tmp_path / "companies.csv"
    df.to_csv(path, index=False)
    return path


# ── Website DataFrames ────────────────────────────────────────────────────────

@pytest.fixture
def websites_df():
    """Sample DataFrame as produced by find_websites.py."""
    return pd.DataFrame(
        {
            "siren":                       ["123456789", "987654321", "111222333"],
            "denominationUniteLegale":     ["BOAT COMPANY SA", "MARINE TECH SARL", "OTHER FIRM SAS"],
            "site_web":                    [
                "https://www.boatcompany.fr",
                "https://www.societe.com/marine-tech",  # blocklisted
                "https://www.otherfirm.fr/en/home",     # English URL
            ],
            "statut_recherche":            ["TROUVÉ", "TROUVÉ", "TROUVÉ"],
        }
    )


# ── SEO audit DataFrames ──────────────────────────────────────────────────────

@pytest.fixture
def verified_df():
    """Sample DataFrame with verified sites and SEO audit fields."""
    return pd.DataFrame(
        {
            "siren":                        ["123456789", "987654321"],
            "denominationUniteLegale":      ["BOAT COMPANY SA", "MARINE TECH SARL"],
            "site_web":                     ["https://boatcompany.fr", "https://marinetech.fr"],
            "site_verifie":                 [True, True],
            "nb_pages":                     [4, 12],
            "has_blog":                     [True, False],
            "blog_status":                  ["abandonné", "absent"],
            "frequence_publication":        [None, None],
            "has_sitemap":                  [False, True],
            "mots_moyen_par_page":          [100, 250],
            "ratio_texte_html":             [0.10, 0.20],
            "cms_detecte":                  [None, "WordPress"],
            "pages_sans_meta_desc":         [2, 1],
            "pages_sans_h1":                [1, 0],
            "titles_dupliques":             [0.5, 0.0],
            "pages_vides":                  [1, 0],
            "dateCreationUniteLegale":      ["2005-03-15", "2018-07-22"],
            "prospect_score":               [0.0, 0.0],
            "prospect_summary":             ["", ""],
        }
    )
