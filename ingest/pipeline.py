"""Run the dlt pipeline standalone:  python -m ingest.pipeline

Dagster calls `run()` directly (see orchestration/definitions.py); this module
just makes the ingestion runnable on its own for quick local iteration.
"""

import dlt

from ingest.binance import binance_source
from ingest.config import pg_dsn


def run():
    pipeline = dlt.pipeline(
        pipeline_name="binance",
        destination=dlt.destinations.postgres(pg_dsn()),
        dataset_name="binance_raw",  # -> schema `binance_raw`, table `klines`
    )
    load_info = pipeline.run(binance_source())
    return load_info


if __name__ == "__main__":
    print(run())
