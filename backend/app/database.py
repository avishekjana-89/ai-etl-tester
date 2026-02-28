import time
import logging
import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

from app.config import DATABASE_URL

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

logger = logging.getLogger("etl_ai")


def get_db():
    """FastAPI dependency: yields a DB session per request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def run_migrations():
    """Run all pending Alembic migrations with retry logic.

    Works identically for SQLite (local dev) and PostgreSQL (Docker prod).
    Alembic's alembic_version table makes this idempotent — already-applied
    migrations are skipped automatically.

    Auto-stamp handling: if the schema tables already exist (created by an
    older create_all run) but the alembic_version table does not, we stamp
    the DB at the initial revision before upgrading so Alembic doesn't try
    to re-create tables that are already there.
    """
    from alembic.config import Config
    from alembic import command
    from sqlalchemy import inspect, text

    # Locate alembic.ini — works both inside Docker (/app/alembic.ini)
    # and locally (relative to this file's package root).
    candidates = [
        Path("/app/alembic.ini"),                                   # Docker
        Path(__file__).resolve().parent.parent / "alembic.ini",    # local dev
    ]
    alembic_ini = next((p for p in candidates if p.exists()), None)
    if alembic_ini is None:
        raise FileNotFoundError(
            f"alembic.ini not found. Searched: {[str(p) for p in candidates]}"
        )

    cfg = Config(str(alembic_ini))
    # Override sqlalchemy.url from the environment so alembic.ini
    # doesn't need to be edited between dev / prod.
    cfg.set_main_option("sqlalchemy.url", DATABASE_URL)

    # --- Auto-stamp: handle databases that pre-date Alembic tracking ---
    INITIAL_REVISION = "3c5059432bb2"
    try:
        insp = inspect(engine)
        existing_tables = insp.get_table_names()
        has_alembic_version = "alembic_version" in existing_tables
        has_app_tables = "connectors" in existing_tables

        if has_app_tables and not has_alembic_version:
            logger.warning(
                "Detected existing schema without alembic_version. "
                f"Stamping database at initial revision ({INITIAL_REVISION}) "
                "to prevent duplicate-table errors."
            )
            command.stamp(cfg, INITIAL_REVISION)
            logger.info(f"Database stamped at {INITIAL_REVISION}.")
    except Exception as stamp_exc:
        logger.warning(f"Auto-stamp check failed (non-fatal): {stamp_exc}")

    # --- Run pending migrations ---
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Running Alembic migrations (attempt {attempt}/{max_retries})...")
            command.upgrade(cfg, "head")
            logger.info("Alembic migrations applied successfully.")
            return
        except Exception as exc:
            if attempt < max_retries:
                logger.warning(f"Migration attempt {attempt} failed ({exc}). Retrying in 2s...")
                time.sleep(2)
            else:
                logger.error("Could not apply migrations after several attempts.")
                raise
