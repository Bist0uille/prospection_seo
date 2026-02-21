"""
PostgreSQL connection management for the botparser pipeline.

The connection URL can be supplied in three ways (in priority order):
  1. Explicit ``dsn`` argument to :func:`get_engine`.
  2. ``BOTPARSER_PG_DSN`` environment variable.
  3. Default Docker-Compose URL (``postgresql://botparser:botparser@localhost:5432/botparser``).
"""

from __future__ import annotations

import os
from functools import lru_cache

from sqlalchemy import Engine, create_engine, text

DEFAULT_DSN = "postgresql://botparser:botparser@localhost:5432/botparser"


def get_dsn(dsn: str | None = None) -> str:
    """Resolve the PostgreSQL connection URL.

    Args:
        dsn: Explicit DSN override; ``None`` falls back to env var or default.

    Returns:
        Fully-qualified PostgreSQL DSN string.
    """
    return dsn or os.getenv("BOTPARSER_PG_DSN", DEFAULT_DSN)


def get_engine(dsn: str | None = None) -> Engine:
    """Create (or return cached) a SQLAlchemy engine for the given DSN.

    Connection pooling is handled by SQLAlchemy automatically.
    The engine is *not* cached when an explicit ``dsn`` is passed so that
    callers with different DSNs always get an independent engine.

    Args:
        dsn: PostgreSQL connection URL.  ``None`` uses :func:`get_dsn`.

    Returns:
        A :class:`sqlalchemy.Engine` instance.
    """
    resolved = get_dsn(dsn)
    return create_engine(resolved, pool_pre_ping=True)


def check_connection(dsn: str | None = None) -> bool:
    """Test whether the database is reachable.

    Args:
        dsn: PostgreSQL connection URL.

    Returns:
        ``True`` if the connection succeeds, ``False`` otherwise.
    """
    try:
        engine = get_engine(dsn)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
