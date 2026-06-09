# crypto-data-pipeline

A small, end-to-end data pipeline that runs on a Raspberry Pi: it ingests crypto
market data from a public API, lands it in a local warehouse, and transforms it
into analytics-ready marts — orchestrated as an asset graph.

It's deliberately built to mirror a production cloud stack, so the patterns
transfer directly:

| This project (Pi-local) | Production equivalent | Concept |
|---|---|---|
| **dlt** + incremental cursor | Fivetran / custom Lambda fan-out | extraction w/ pagination + watermark |
| **Postgres** (`binance_raw` + `analytics` schemas) | Snowflake / BigQuery | relational warehouse |
| dlt raw schema | S3 landing + Snowpipe | raw land → managed load |
| **dbt** (`dbt-postgres`) | dbt on Snowflake | transform raw → marts |
| **Dagster** | Airflow / Step Functions | orchestration + lineage |

```
                 ┌────────────┐   ┌──────────────┐   ┌──────────────┐
  Binance API ──▶│   dlt      │──▶│  Postgres    │──▶│    dbt       │──▶ marts
  (klines)       │ extract +  │   │ binance_raw  │   │ staging +    │
                 │ incremental│   │  .klines     │   │ marts        │
                 └────────────┘   └──────────────┘   └──────────────┘
                        └──────────── Dagster (schedule + asset graph) ──────────┘
```

> **Why Postgres, not DuckDB?** This sandbox runs on a 32-bit (armv7l) Raspberry
> Pi OS, and DuckDB has no 32-bit build. Postgres runs natively on 32-bit and is
> an even closer analog to a real cloud warehouse (a managed server you connect
> to, not an embedded file). The architecture is identical — only the warehouse
> engine and a little dialect-specific SQL changed.

## The interesting bit: incremental extraction

`ingest/binance.py` is the part worth reading. It does the two things that
typically get hand-rolled — **pagination** and an **incremental watermark** —
declaratively:

- dlt's `incremental("open_time")` persists the max timestamp loaded across runs.
- We push that watermark into the request (`startTime=...`) so re-runs only fetch
  new candles, then page forward until the API returns a partial batch.
- `write_disposition="merge"` on `["symbol","open_time"]` makes re-runs idempotent
  (a little overlap on the trailing candle is fine — the merge dedupes it).

## Run it

Runs on **64-bit or 32-bit Raspberry Pi OS** with **Python 3.9+**. `psycopg2`
builds from source, so install the build prerequisites once:

```bash
sudo apt update
sudo apt install -y postgresql libpq-dev python3-dev build-essential python3-venv
```

Then:

```bash
make install          # venv + deps
make db               # create the sandbox role + database (one time)
source .venv/bin/activate

# Option A — run the stages directly:
make run              # dlt ingest, then dbt build
make query            # peek at the daily mart

# Option B — run it orchestrated, with the asset graph + hourly schedule:
make dagster          # open http://<pi-ip>:3000
```

No API key needed — `data-api.binance.vision` is a public, key-free,
non-geo-blocked host.

### Connecting to the warehouse

The sandbox defaults are role `crypto` / password `crypto` / database `crypto`
on `localhost:5432`. Both dbt and the dlt loader read the standard libpq env
vars (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`) — the `Makefile`
exports sandbox defaults and `ingest/config.py` builds the dlt DSN from them, so
the two stages always point at the same database. Override any of them in your
environment to point elsewhere.

## Swapping the source

The source is one module. To demonstrate a different cursor style, drop in a
sibling of `ingest/binance.py`:

- **DefiLlama** — `https://api.llama.fi/v2/historicalChainTvl/{chain}`. Keyless;
  returns `[{date, tvl}]`, so the cursor is a date. Good "TVL over time" mart.
- **Etherscan** — `account&action=txlist`. Free API key; paginate by `page` and
  use `startblock` as the cursor. The most on-brand for EVM work — a block-number
  watermark is the canonical on-chain incremental pattern.

The rest of the stack (Postgres, dbt, Dagster) is unchanged — only the dbt source
name and staging model need to match the new table.

## Layout

```
ingest/binance.py        dlt source — pagination + incremental cursor
ingest/config.py         builds the shared Postgres DSN from PG* env vars
ingest/pipeline.py       standalone runner (python -m ingest.pipeline)
transform/               dbt project (staging view -> daily mart table)
orchestration/           Dagster definitions (assets, job, hourly schedule)
scripts/setup_postgres.sh  idempotent sandbox role + database bootstrap
```

## Notes / gotchas

- **dbt + dlt share one database** via the `PG*` env vars. The `Makefile` exports
  them and sets `DBT_PROFILES_DIR` (profiles.yml lives in `transform/`, not
  `~/.dbt`); if you run dbt by hand, export those so it reads the same warehouse.
- **psycopg2 compiles on the Pi.** No `armv7l` wheel ships, so pip builds it from
  source — that's why `libpq-dev` + `build-essential` are prerequisites. It's a
  light build (seconds), unlike DuckDB which won't build on 32-bit at all.
- **Asset linkage.** The dlt asset key is set to `["binance_raw","klines"]` to
  match dbt's source key so lineage connects in the UI; depending on your
  `dagster-dbt` version you may need a custom `DagsterDbtTranslator` to align it.
- **32-bit Pi (Bullseye) grpc pin.** `requirements.txt` pins `grpcio==1.59.3`.
  Newer piwheels `armv7l` wheels are built on Bookworm and need `GLIBCXX_3.4.29`,
  which Bullseye's libstdc++ lacks — without the pin, Dagster fails to import
  `grpc`. The dlt + dbt stages (`make run`) don't use grpc and work regardless.
  Remove the pin on 64-bit / Bookworm.
- **Python 3.9 + dlt.** Resource args are annotated with `typing.Tuple[...]` and
  the bare `incremental` class (not the `[...]`-subscripted generics): newer dlt
  inspects annotations with `issubclass()`, which chokes on PEP 585 builtin
  generics under Python 3.9.
```
