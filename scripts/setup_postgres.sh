#!/usr/bin/env bash
# Create the local sandbox role + database for the pipeline. Idempotent: safe to
# re-run. Uses the postgres superuser via peer auth (the default on Debian/RPi
# OS), so run it as a user with sudo. Override the names via PG* env vars.
set -euo pipefail

PGUSER="${PGUSER:-crypto}"
PGPASSWORD="${PGPASSWORD:-crypto}"
PGDATABASE="${PGDATABASE:-crypto}"

# Create the login role if it doesn't already exist.
sudo -u postgres psql -v ON_ERROR_STOP=1 <<SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${PGUSER}') THEN
    CREATE ROLE ${PGUSER} LOGIN PASSWORD '${PGPASSWORD}';
  END IF;
END
\$\$;
SQL

# Create the database owned by that role if it doesn't already exist.
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname = '${PGDATABASE}'" | grep -q 1; then
  sudo -u postgres createdb -O "${PGUSER}" "${PGDATABASE}"
fi

echo "Postgres ready: role '${PGUSER}' owns database '${PGDATABASE}'."
