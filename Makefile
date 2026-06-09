.PHONY: install db ingest transform run dagster query clean

# Postgres connection (local sandbox defaults; override via env). These are
# exported so both dbt (profiles.yml) and the dlt loader (ingest/config.py) read
# the same target. dbt also needs DBT_PROFILES_DIR since profiles.yml lives in
# the project dir rather than ~/.dbt.
export PGHOST     ?= localhost
export PGPORT     ?= 5432
export PGUSER     ?= crypto
export PGPASSWORD ?= crypto
export PGDATABASE ?= crypto
export DBT_PROFILES_DIR := $(CURDIR)/transform

install:         ## venv + deps (needs libpq-dev + build tools for psycopg2)
	python3 -m venv .venv && . .venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt

db:              ## create the sandbox role + database (idempotent)
	./scripts/setup_postgres.sh

ingest:          ## run the dlt pipeline only
	python -m ingest.pipeline

transform:       ## run dbt models only
	cd transform && dbt build

run: ingest transform   ## full pipeline, no orchestrator

dagster:         ## launch the Dagster UI (asset graph + schedule)
	dagster dev -m orchestration.definitions

query:           ## peek at the daily mart
	psql "postgresql://$(PGUSER):$(PGPASSWORD)@$(PGHOST):$(PGPORT)/$(PGDATABASE)" \
		-c "select * from analytics.mart_daily_ohlcv order by trade_date desc limit 20;"

clean:           ## drop dbt artifacts (leaves the warehouse intact)
	rm -rf transform/target transform/logs
