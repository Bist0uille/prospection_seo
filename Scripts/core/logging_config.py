"""
Centralised logging configuration for the botparser pipeline.

Usage in any module:
    from Scripts.core.logging_config import get_logger
    logger = get_logger(__name__)

Usage in a CLI entrypoint (activates stdout + file handlers):
    from Scripts.core.logging_config import setup_pipeline_logging
    setup_pipeline_logging(log_dir="Logs", sector_name="nautisme")
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

# ── Constants ────────────────────────────────────────────────────────────────
_ROOT_LOGGER = "botparser"
_LOG_FORMAT = "%(asctime)s [%(levelname)-8s] %(name)s — %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


# ── Public API ────────────────────────────────────────────────────────────────

def get_logger(name: str) -> logging.Logger:
    """Return a child logger scoped under the 'botparser' hierarchy.

    Handlers are inherited from the root 'botparser' logger once
    setup_pipeline_logging() has been called.  Works even before that call
    (messages are simply buffered / dropped if no handlers are configured).

    Args:
        name: Typically ``__name__`` of the calling module.

    Returns:
        A named :class:`logging.Logger` instance.
    """
    qualified = f"{_ROOT_LOGGER}.{name}" if not name.startswith(_ROOT_LOGGER) else name
    return logging.getLogger(qualified)


def setup_pipeline_logging(
    log_dir: str | Path = "Logs",
    sector_name: str = "pipeline",
    level: int = logging.INFO,
) -> logging.Logger:
    """Configure the root 'botparser' logger with stdout + file handlers.

    Call this **once** at the start of a CLI entrypoint.  All child loggers
    obtained via :func:`get_logger` will automatically inherit the handlers.

    The file handler is always set to DEBUG so that the log file is verbose
    even when the console level is INFO.

    Args:
        log_dir:     Directory where the log file is written (created if absent).
        sector_name: Used to name the log file, e.g. ``nautisme_20240101_120000.log``.
        level:       Console logging level (default: INFO).

    Returns:
        The configured root ``botparser`` :class:`logging.Logger`.
    """
    root = logging.getLogger(_ROOT_LOGGER)
    root.setLevel(logging.DEBUG)  # capture everything; handlers filter independently

    # Guard against duplicate handlers on re-calls (e.g. during test runs)
    if root.handlers:
        return root

    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

    # ── stdout handler ────────────────────────────────────────────────────────
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(level)
    stdout_handler.setFormatter(formatter)
    root.addHandler(stdout_handler)

    # ── file handler ─────────────────────────────────────────────────────────
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_path / f"{sector_name}_{timestamp}.log"

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    root.info("Logging initialised → %s", log_file)
    return root


def reset_logging() -> None:
    """Remove all handlers from the root botparser logger.

    Intended for use in tests to ensure a clean state between test runs.
    """
    root = logging.getLogger(_ROOT_LOGGER)
    for handler in root.handlers[:]:
        handler.close()
        root.removeHandler(handler)
