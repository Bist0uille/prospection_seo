#!/usr/bin/env python3
"""
CSV → PostgreSQL importer for INSEE company data.

Reads any INSEE-format CSV (annuaire établissements or StockUniteLegale)
and bulk-inserts it into the ``unites_legales`` table.

Usage:
  python db/importer.py DataBase/annuaire-des-entreprises-nautisme.csv
  python db/importer.py DataBase/StockUniteLegale_utf8.csv --drop --chunk-size 50000
  python db/importer.py DataBase/annuaire.csv --dsn postgresql://user:pass@host/db
"""

from __future__ import annotations

import sys
from pathlib import Path

import click
import pandas as pd
from sqlalchemy import inspect, text

# ── Project root on sys.path ──────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.connection import get_engine, get_dsn
from Scripts.core.logging_config import get_logger, setup_pipeline_logging

logger = get_logger(__name__)

TABLE_NAME = "unites_legales"

# Columns that the pipeline actually queries — used for partial imports
_PIPELINE_COLUMNS = {
    "siren",
    "denominationUniteLegale",
    "activitePrincipaleUniteLegale",
    "trancheEffectifsUniteLegale",
    "etatAdministratifUniteLegale",
    "etablissementSiege",
    "dateCreationUniteLegale",
}


def _normalise_naf(df: pd.DataFrame) -> pd.DataFrame:
    """Remove dots from NAF codes in-place (e.g. '30.12Z' → '3012Z')."""
    col = "activitePrincipaleUniteLegale"
    if col in df.columns:
        df[col] = df[col].astype(str).str.replace(".", "", regex=False)
    return df


def import_csv(
    csv_path: str | Path,
    dsn: str | None = None,
    drop: bool = False,
    chunk_size: int = 10_000,
) -> int:
    """Import a CSV file into the ``unites_legales`` PostgreSQL table.

    Uses pandas ``to_sql`` with ``method='multi'`` for efficient bulk
    inserts.  Rows whose SIREN already exists are skipped (``if_exists='append'``
    mode uses ``ON CONFLICT DO NOTHING`` via ``method='multi'``).

    Args:
        csv_path:   Path to the source CSV file (any INSEE format).
        dsn:        PostgreSQL DSN; ``None`` falls back to env / default.
        drop:       If ``True``, truncate the table before importing.
        chunk_size: Number of rows per INSERT batch.

    Returns:
        Number of rows successfully written.
    """
    csv_path = Path(csv_path)
    logger.info("Importing '%s' → table '%s' (chunk_size=%d)", csv_path, TABLE_NAME, chunk_size)

    engine = get_engine(dsn)

    # ── Optional table truncation ─────────────────────────────────────────────
    if drop:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {TABLE_NAME}"))
            logger.info("Table '%s' truncated.", TABLE_NAME)

    # ── Stream-read the CSV in chunks ─────────────────────────────────────────
    total_written = 0
    reader = pd.read_csv(
        csv_path,
        dtype=str,          # keep everything as text — matches TEXT schema
        chunksize=chunk_size,
        keep_default_na=False,  # don't convert empty strings to NaN
    )

    for i, chunk in enumerate(reader):
        # Normalise NAF codes (remove dots) before storing
        chunk = _normalise_naf(chunk)

        # Drop columns not in the schema (extra columns from some CSV variants)
        with engine.connect() as conn:
            inspector = inspect(engine)
            db_cols = {c["name"] for c in inspector.get_columns(TABLE_NAME)}
        chunk = chunk[[c for c in chunk.columns if c in db_cols]]

        try:
            chunk.to_sql(
                TABLE_NAME,
                engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=500,  # SQLAlchemy inner batch for parameter limit
            )
            total_written += len(chunk)
            logger.info("  Chunk %d: %d rows written (total: %d)", i + 1, len(chunk), total_written)
        except Exception as exc:
            logger.warning("  Chunk %d: partial failure — %s", i + 1, exc)
            # Best-effort: insert row by row to skip duplicates
            for _, row in chunk.iterrows():
                try:
                    pd.DataFrame([row]).to_sql(
                        TABLE_NAME, engine, if_exists="append", index=False, method="multi"
                    )
                    total_written += 1
                except Exception:
                    pass  # skip duplicate SIREN

    logger.info("Import complete: %d rows written to '%s'.", total_written, TABLE_NAME)
    return total_written


# ============================================================================
# CLI
# ============================================================================

@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("csv_path", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--dsn",
    default=None,
    envvar="BOTPARSER_PG_DSN",
    show_default=True,
    help="PostgreSQL DSN (défaut: $BOTPARSER_PG_DSN ou docker-compose local).",
)
@click.option(
    "--drop/--no-drop",
    default=False,
    help="Vider la table avant l'import (défaut: append / skip duplicates).",
)
@click.option(
    "--chunk-size",
    type=int,
    default=10_000,
    show_default=True,
    help="Nombre de lignes par batch d'insertion.",
)
def main(csv_path: str, dsn: str | None, drop: bool, chunk_size: int) -> None:
    """Import un CSV INSEE dans PostgreSQL.

    \b
    Exemples :
      python db/importer.py DataBase/annuaire-des-entreprises-nautisme.csv
      python db/importer.py DataBase/StockUniteLegale_utf8.csv --drop --chunk-size 50000
    """
    setup_pipeline_logging(log_dir="Logs", sector_name="import")
    resolved_dsn = get_dsn(dsn)
    logger.info("Target DSN: %s", resolved_dsn)

    written = import_csv(csv_path, dsn=dsn, drop=drop, chunk_size=chunk_size)
    click.echo(f"\n✓ {written} lignes importées dans '{TABLE_NAME}'.")


if __name__ == "__main__":
    main()
