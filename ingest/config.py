"""Single source of truth for the Postgres connection.

Both dlt (the loader) and dbt (the transformer) must point at the same database.
dbt reads the standard libpq env vars directly from profiles.yml; dlt gets the
DSN built here. Defaults describe the local sandbox role created by
`scripts/setup_postgres.sh` -- override any piece via the PG* env vars.
"""

import os


def pg_dsn() -> str:
    """Build a libpq connection string from PG* env vars (sandbox defaults)."""
    user = os.environ.get("PGUSER", "crypto")
    password = os.environ.get("PGPASSWORD", "crypto")
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    database = os.environ.get("PGDATABASE", "crypto")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"
