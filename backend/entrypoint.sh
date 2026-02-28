#!/bin/bash
set -e

echo "=== ETL Backend Entrypoint ==="

INITIAL_REVISION="3c5059432bb2"

# ---------------------------------------------------------------------------
# Wait for PostgreSQL (only when DATABASE_URL points to postgres/postgresql)
# ---------------------------------------------------------------------------
if echo "$DATABASE_URL" | grep -qE "^postgres(ql)?://"; then
    echo "Waiting for PostgreSQL to be ready..."

    DB_HOST=$(echo "$DATABASE_URL" | sed -E 's|.*@([^:/]+).*|\1|')
    DB_PORT=$(echo "$DATABASE_URL" | sed -E 's|.*:([0-9]+)/.*|\1|')
    DB_PORT="${DB_PORT:-5432}"

    MAX_RETRIES=30
    RETRY=0
    until pg_isready -h "$DB_HOST" -p "$DB_PORT" -q 2>/dev/null; do
        RETRY=$((RETRY + 1))
        if [ "$RETRY" -ge "$MAX_RETRIES" ]; then
            echo "ERROR: PostgreSQL not ready after ${MAX_RETRIES} attempts. Exiting."
            exit 1
        fi
        echo "  PostgreSQL not ready yet (attempt $RETRY/$MAX_RETRIES)... retrying in 2s"
        sleep 2
    done
    echo "PostgreSQL is ready."

    # -----------------------------------------------------------------------
    # Auto-stamp: if app tables exist but alembic_version does not, this DB
    # was created by the old create_all path. Stamp it so Alembic won't try
    # to re-create tables that are already there.
    # -----------------------------------------------------------------------
    DB_USER=$(echo "$DATABASE_URL" | sed -E 's|.*://([^:]+):.*|\1|')
    DB_NAME=$(echo "$DATABASE_URL" | sed -E 's|.*/([^?]+).*|\1|')
    DB_PASS=$(echo "$DATABASE_URL" | sed -E 's|.*://[^:]+:([^@]+)@.*|\1|')

    HAS_CONNECTORS=$(PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -tAc \
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='connectors' AND table_schema='public';" 2>/dev/null || echo "0")

    HAS_ALEMBIC=$(PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -tAc \
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='alembic_version' AND table_schema='public';" 2>/dev/null || echo "0")

    if [ "$HAS_CONNECTORS" = "1" ] && [ "$HAS_ALEMBIC" = "0" ]; then
        echo "Existing schema detected without alembic_version — stamping at initial revision ($INITIAL_REVISION)..."
        alembic stamp "$INITIAL_REVISION"
        echo "Stamp applied."
    fi
fi

# ---------------------------------------------------------------------------
# Run Alembic migrations
# ---------------------------------------------------------------------------
echo "Running Alembic migrations..."
alembic upgrade head
echo "Migrations complete."

# ---------------------------------------------------------------------------
# Start the application
# ---------------------------------------------------------------------------
echo "Starting uvicorn..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
