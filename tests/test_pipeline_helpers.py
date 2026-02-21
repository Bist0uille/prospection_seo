"""
Unit tests for helper functions in Scripts/run_full_pipeline.py.
"""

from __future__ import annotations

import pytest

from Scripts.run_full_pipeline import get_employee_codes, load_ape_codes


# ============================================================================
# get_employee_codes
# ============================================================================

class TestGetEmployeeCodes:
    """Tests for get_employee_codes()."""

    def test_min_10_returns_correct_codes(self):
        codes = get_employee_codes(10)
        assert "11" in codes  # 10+ employees
        assert "12" in codes
        assert "21" in codes
        assert "53" in codes  # 10 000+

    def test_min_10_excludes_small_bands(self):
        codes = get_employee_codes(10)
        assert "NN" not in codes  # unknown
        assert "00" not in codes  # 0 employees
        assert "01" not in codes  # 1–2 employees
        assert "02" not in codes  # 3–5
        assert "03" not in codes  # 6–9

    def test_min_0_returns_all_codes(self):
        codes = get_employee_codes(0)
        assert "NN" in codes
        assert "00" in codes
        assert "01" in codes
        assert "53" in codes

    def test_min_50_returns_subset(self):
        codes = get_employee_codes(50)
        assert "21" in codes   # 50+
        assert "11" not in codes  # 10–19

    def test_min_very_large_returns_only_top_band(self):
        codes = get_employee_codes(10000)
        assert codes == ["53"]

    def test_min_above_max_returns_empty(self):
        # No band has lower_bound >= 99999
        codes = get_employee_codes(99999)
        assert codes == []

    def test_returns_list(self):
        assert isinstance(get_employee_codes(10), list)

    def test_all_elements_are_strings(self):
        for code in get_employee_codes(0):
            assert isinstance(code, str)


# ============================================================================
# load_ape_codes
# ============================================================================

class TestLoadApeCodes:
    """Tests for load_ape_codes()."""

    def test_parses_codes_with_description(self, sector_file):
        codes = load_ape_codes(sector_file)
        assert "3012Z" in codes
        assert "3011Z" in codes

    def test_ignores_comments(self, sector_file):
        codes = load_ape_codes(sector_file)
        assert not any(c.startswith("#") for c in codes)

    def test_ignores_blank_lines(self, sector_file):
        codes = load_ape_codes(sector_file)
        assert "" not in codes

    def test_count_correct(self, sector_file):
        codes = load_ape_codes(sector_file)
        assert len(codes) == 2

    def test_empty_file_raises(self, empty_sector_file):
        with pytest.raises(ValueError, match="Aucun code APE"):
            load_ape_codes(empty_sector_file)

    def test_returns_list(self, sector_file):
        assert isinstance(load_ape_codes(sector_file), list)

    def test_code_only_line(self, tmp_path):
        """Lines with only a code (no description) are parsed correctly."""
        f = tmp_path / "codes_only.txt"
        f.write_text("3012Z\n3011Z\n", encoding="utf-8")
        codes = load_ape_codes(f)
        assert codes == ["3012Z", "3011Z"]

    def test_mixed_format(self, tmp_path):
        """Mix of 'CODE - description' and 'CODE' lines both parse correctly."""
        content = "3012Z - Desc one\n3011Z\n# ignored\n5010Z - Desc three\n"
        f = tmp_path / "mixed.txt"
        f.write_text(content, encoding="utf-8")
        codes = load_ape_codes(f)
        assert codes == ["3012Z", "3011Z", "5010Z"]
