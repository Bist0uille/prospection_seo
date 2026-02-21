"""
Pydantic models for input validation and sanitisation.

All user-facing inputs (CLI arguments, file paths, APE codes, …) are funnelled
through these models *before* being used by any pipeline logic.  This ensures:

  * Type coercion  (e.g. "10" → 10 for integers)
  * Early validation  with clear, actionable error messages
  * Sanitisation  (path-traversal characters, empty strings, …)

Pydantic v2 is required (``pydantic>=2.0``).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# ── Regex constants ───────────────────────────────────────────────────────────

# APE / NAF code: exactly 4 digits followed by one uppercase letter (e.g. "3012Z")
_APE_CODE_RE = re.compile(r"^\d{4}[A-Z]$")

# Safe sector/directory name: alphanumeric, underscores, hyphens
_SAFE_NAME_RE = re.compile(r"[^\w\-]")


# ── Private helpers ───────────────────────────────────────────────────────────

def _sanitize_name(value: str) -> str:
    """Replace any character that is not alphanumeric, underscore, or hyphen."""
    return _SAFE_NAME_RE.sub("_", value.strip())


def _parse_ape_codes(raw: object) -> List[str]:
    """Parse and validate a list of APE codes from various input shapes.

    Accepts:
        * A comma-separated string: ``"3012Z,3011Z"``
        * A plain list:             ``["3012Z", "3011Z"]``

    Returns:
        Deduplicated list of validated, uppercased APE codes.

    Raises:
        ValueError: If any code does not match the APE format.
    """
    if isinstance(raw, str):
        raw = [c.strip() for c in raw.split(",") if c.strip()]

    validated: List[str] = []
    for item in raw:
        code = str(item).strip().upper()
        if not _APE_CODE_RE.match(code):
            raise ValueError(
                f"APE code '{code}' is invalid — "
                f"expected 4 digits + 1 uppercase letter (e.g. '3012Z')."
            )
        if code not in validated:
            validated.append(code)

    if not validated:
        raise ValueError("At least one valid APE code is required.")

    return validated


# ── Standalone validator model ────────────────────────────────────────────────

class ApeCodeList(BaseModel):
    """A validated, deduplicated list of APE / NAF codes."""

    model_config = ConfigDict(str_strip_whitespace=True)

    codes: List[str] = Field(..., min_length=1)

    @field_validator("codes", mode="before")
    @classmethod
    def validate_codes(cls, v: object) -> List[str]:
        return _parse_ape_codes(v)

    @classmethod
    def from_csv(cls, raw: str) -> "ApeCodeList":
        """Convenience constructor from a comma-separated string."""
        return cls(codes=raw)


# ── Pipeline model ────────────────────────────────────────────────────────────

class PipelineConfig(BaseModel):
    """Validated and sanitised configuration for the full pipeline.

    Either ``sector`` (path to a ``.txt`` file listing APE codes) or ``codes``
    (a comma-separated string / list of APE codes) must be provided.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    # ── Source of APE codes (mutually exclusive, but one is required) ─────────
    sector: Optional[Path] = Field(
        default=None,
        description="Path to a sector .txt file containing APE codes.",
    )
    codes: Optional[List[str]] = Field(
        default=None,
        description="Explicit comma-separated list of APE codes.",
    )

    # ── Identity ──────────────────────────────────────────────────────────────
    name: Optional[str] = Field(
        default=None,
        description="Sector name used for the output directory.",
    )
    db: Optional[Path] = Field(
        default=None,
        description="Path to the INSEE CSV database file.",
    )
    pg_dsn: Optional[str] = Field(
        default=None,
        description="PostgreSQL DSN (e.g. postgresql://user:pass@host/db). "
                    "When set, step 1 queries the DB instead of reading a CSV.",
    )

    # ── Pipeline knobs ────────────────────────────────────────────────────────
    min_employees: int = Field(
        default=10,
        ge=0,
        description="Minimum employee count threshold (INSEE tranche).",
    )
    fresh: bool = Field(
        default=True,
        description="Remove previous results before running.",
    )
    limit: Optional[int] = Field(
        default=None,
        gt=0,
        description="Cap on number of companies processed (for testing).",
    )
    skip_audit: bool = Field(
        default=False,
        description="Skip the SEO audit step (step 4).",
    )
    keep_intermediates: bool = Field(
        default=False,
        description="Keep intermediate CSV files after the pipeline.",
    )

    # ── Validators ────────────────────────────────────────────────────────────

    @model_validator(mode="after")
    def require_sector_or_codes(self) -> "PipelineConfig":
        """Ensure at least one source of APE codes is provided."""
        if self.sector is None and not self.codes:
            raise ValueError(
                "Provide either --sector <file> or --codes <CODE1,CODE2,...>."
            )
        return self

    @field_validator("sector")
    @classmethod
    def sector_file_must_exist(cls, v: Optional[Path]) -> Optional[Path]:
        if v is not None and not v.exists():
            raise ValueError(f"Sector file not found: '{v}'.")
        return v

    @field_validator("db")
    @classmethod
    def db_file_must_exist(cls, v: Optional[Path]) -> Optional[Path]:
        if v is not None and not v.exists():
            raise ValueError(f"Database file not found: '{v}'.")
        return v

    @field_validator("name")
    @classmethod
    def sanitize_sector_name(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        sanitized = _sanitize_name(v)
        if not sanitized:
            raise ValueError("Sector name is empty after sanitisation.")
        return sanitized

    @field_validator("codes", mode="before")
    @classmethod
    def parse_ape_codes(cls, v: object) -> Optional[List[str]]:
        if v is None:
            return None
        return _parse_ape_codes(v)

    @field_validator("pg_dsn")
    @classmethod
    def validate_pg_dsn(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        if not (v.startswith("postgresql://") or v.startswith("postgres://")):
            raise ValueError(
                "pg_dsn must start with 'postgresql://' or 'postgres://'."
            )
        return v


# ── find_websites.py model ────────────────────────────────────────────────────

class FindWebsitesConfig(BaseModel):
    """Validated input configuration for the ``find_websites.py`` entrypoint."""

    model_config = ConfigDict(str_strip_whitespace=True)

    input_csv: Path = Field(..., description="Path to the input CSV file.")
    output_dir: Path = Field(
        default=Path("Results"),
        description="Directory where the output CSV will be written.",
    )
    limit: Optional[int] = Field(
        default=None,
        gt=0,
        description="Max number of companies to process (testing).",
    )

    @field_validator("input_csv")
    @classmethod
    def input_must_exist(cls, v: Path) -> Path:
        if not v.exists():
            raise ValueError(f"Input CSV not found: '{v}'.")
        return v


# ── seo_auditor.py model ──────────────────────────────────────────────────────

class SeoAuditConfig(BaseModel):
    """Validated input configuration for the ``seo_auditor.py`` standalone CLI."""

    model_config = ConfigDict(str_strip_whitespace=True)

    input_csv: Path = Field(
        ...,
        description="CSV file with 'site_web' and 'site_verifie' columns.",
    )
    output: Path = Field(
        default=Path("Results/seo_audit.csv"),
        description="Path of the output CSV.",
    )
    max_pages: int = Field(
        default=30,
        ge=1,
        le=200,
        description="Maximum pages to crawl per site.",
    )

    @field_validator("input_csv")
    @classmethod
    def input_must_exist(cls, v: Path) -> Path:
        if not v.exists():
            raise ValueError(f"Input CSV not found: '{v}'.")
        return v


# ── contact_scraper.py model ──────────────────────────────────────────────────

class ContactScraperConfig(BaseModel):
    """Validated input configuration for the ``contact_scraper.py`` standalone CLI."""

    model_config = ConfigDict(str_strip_whitespace=True)

    input_csv: Path = Field(
        ...,
        description="CSV file with a 'site_web' column.",
    )
    output_csv: Path = Field(
        default=Path("Results/contacts.csv"),
        description="Path of the enriched output CSV.",
    )

    @field_validator("input_csv")
    @classmethod
    def contact_input_must_exist(cls, v: Path) -> Path:
        if not v.exists():
            raise ValueError(f"Input CSV not found: '{v}'.")
        return v
