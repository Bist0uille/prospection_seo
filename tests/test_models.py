"""
Unit tests for Scripts/core/models.py — Pydantic validation and sanitisation.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from Scripts.core.models import ApeCodeList, FindWebsitesConfig, PipelineConfig, SeoAuditConfig


# ============================================================================
# ApeCodeList
# ============================================================================

class TestApeCodeList:
    def test_valid_single_code(self):
        m = ApeCodeList(codes=["3012Z"])
        assert m.codes == ["3012Z"]

    def test_valid_multiple_codes(self):
        m = ApeCodeList(codes=["3012Z", "3011Z", "5010Z"])
        assert m.codes == ["3012Z", "3011Z", "5010Z"]

    def test_codes_are_uppercased(self):
        m = ApeCodeList(codes=["3012z"])
        assert m.codes == ["3012Z"]

    def test_codes_are_stripped(self):
        m = ApeCodeList(codes=["  3012Z  "])
        assert m.codes == ["3012Z"]

    def test_deduplication(self):
        m = ApeCodeList(codes=["3012Z", "3012Z", "3011Z"])
        assert m.codes == ["3012Z", "3011Z"]

    def test_from_csv_string(self):
        m = ApeCodeList.from_csv("3012Z,3011Z, 5010Z")
        assert m.codes == ["3012Z", "3011Z", "5010Z"]

    def test_invalid_format_raises(self):
        with pytest.raises(ValidationError, match="invalid"):
            ApeCodeList(codes=["INVALID"])

    def test_numeric_only_raises(self):
        with pytest.raises(ValidationError):
            ApeCodeList(codes=["30123"])   # no uppercase letter

    def test_empty_list_raises(self):
        with pytest.raises(ValidationError):
            ApeCodeList(codes=[])

    def test_whitespace_only_string_raises(self):
        with pytest.raises(ValidationError):
            ApeCodeList.from_csv("   ,  ")


# ============================================================================
# PipelineConfig
# ============================================================================

class TestPipelineConfig:

    # ── Valid configurations ──────────────────────────────────────────────────

    def test_sector_path(self, sector_file):
        cfg = PipelineConfig(sector=sector_file, min_employees=10)
        assert cfg.sector == sector_file
        assert cfg.codes is None

    def test_codes_string(self):
        cfg = PipelineConfig(codes="3012Z,3011Z")
        assert cfg.codes == ["3012Z", "3011Z"]

    def test_codes_list(self):
        cfg = PipelineConfig(codes=["3012Z", "3011Z"])
        assert cfg.codes == ["3012Z", "3011Z"]

    def test_defaults(self, sector_file):
        cfg = PipelineConfig(sector=sector_file)
        assert cfg.min_employees == 10
        assert cfg.fresh is True
        assert cfg.limit is None
        assert cfg.skip_audit is False
        assert cfg.keep_intermediates is False

    def test_name_sanitisation_special_chars(self, sector_file):
        cfg = PipelineConfig(sector=sector_file, name="mon secteur/test")
        assert cfg.name == "mon_secteur_test"

    def test_name_sanitisation_path_traversal(self, sector_file):
        cfg = PipelineConfig(sector=sector_file, name="../../etc/passwd")
        # Path traversal slashes become underscores
        assert ".." not in cfg.name
        assert "/" not in cfg.name

    def test_name_none_allowed(self, sector_file):
        cfg = PipelineConfig(sector=sector_file, name=None)
        assert cfg.name is None

    def test_codes_are_uppercased(self):
        cfg = PipelineConfig(codes="3012z")
        assert cfg.codes == ["3012Z"]

    def test_fresh_false(self, sector_file):
        cfg = PipelineConfig(sector=sector_file, fresh=False)
        assert cfg.fresh is False

    def test_limit_must_be_positive(self, sector_file):
        with pytest.raises(ValidationError):
            PipelineConfig(sector=sector_file, limit=0)

    def test_min_employees_zero_allowed(self, sector_file):
        cfg = PipelineConfig(sector=sector_file, min_employees=0)
        assert cfg.min_employees == 0

    def test_min_employees_negative_raises(self, sector_file):
        with pytest.raises(ValidationError):
            PipelineConfig(sector=sector_file, min_employees=-1)

    # ── Missing source errors ─────────────────────────────────────────────────

    def test_neither_sector_nor_codes_raises(self):
        with pytest.raises(ValidationError, match="sector.*codes|codes.*sector"):
            PipelineConfig()

    # ── File existence validation ─────────────────────────────────────────────

    def test_sector_nonexistent_raises(self, tmp_path):
        with pytest.raises(ValidationError, match="not found"):
            PipelineConfig(sector=tmp_path / "nonexistent.txt")

    def test_db_nonexistent_raises(self, sector_file, tmp_path):
        with pytest.raises(ValidationError, match="not found"):
            PipelineConfig(sector=sector_file, db=tmp_path / "ghost.csv")

    def test_db_none_allowed(self, sector_file):
        cfg = PipelineConfig(sector=sector_file, db=None)
        assert cfg.db is None

    def test_invalid_ape_code_raises(self):
        with pytest.raises(ValidationError, match="invalid"):
            PipelineConfig(codes="BADCODE")


# ============================================================================
# FindWebsitesConfig
# ============================================================================

class TestFindWebsitesConfig:

    def test_valid(self, dummy_db_csv):
        cfg = FindWebsitesConfig(input_csv=dummy_db_csv)
        assert cfg.input_csv == dummy_db_csv
        assert cfg.output_dir == Path("Results")

    def test_custom_output_dir(self, dummy_db_csv, tmp_path):
        cfg = FindWebsitesConfig(input_csv=dummy_db_csv, output_dir=tmp_path)
        assert cfg.output_dir == tmp_path

    def test_nonexistent_input_raises(self, tmp_path):
        with pytest.raises(ValidationError, match="not found"):
            FindWebsitesConfig(input_csv=tmp_path / "ghost.csv")

    def test_limit_must_be_positive(self, dummy_db_csv):
        with pytest.raises(ValidationError):
            FindWebsitesConfig(input_csv=dummy_db_csv, limit=0)

    def test_limit_none_allowed(self, dummy_db_csv):
        cfg = FindWebsitesConfig(input_csv=dummy_db_csv, limit=None)
        assert cfg.limit is None


# ============================================================================
# SeoAuditConfig
# ============================================================================

class TestSeoAuditConfig:

    def test_valid(self, dummy_db_csv):
        cfg = SeoAuditConfig(input_csv=dummy_db_csv)
        assert cfg.max_pages == 30

    def test_max_pages_bounds(self, dummy_db_csv):
        with pytest.raises(ValidationError):
            SeoAuditConfig(input_csv=dummy_db_csv, max_pages=0)
        with pytest.raises(ValidationError):
            SeoAuditConfig(input_csv=dummy_db_csv, max_pages=201)

    def test_max_pages_valid_range(self, dummy_db_csv):
        cfg = SeoAuditConfig(input_csv=dummy_db_csv, max_pages=100)
        assert cfg.max_pages == 100

    def test_nonexistent_input_raises(self, tmp_path):
        with pytest.raises(ValidationError, match="not found"):
            SeoAuditConfig(input_csv=tmp_path / "ghost.csv")
