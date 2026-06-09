"""Dagster orchestration: dlt ingest -> dbt transform, as one asset graph.

Design note: the official `dagster-dlt` multi-asset integration has churned
across versions, so this uses the robust, version-stable pattern -- wrap the
dlt run in a plain @asset and let `@dbt_assets` own the dbt graph. The dlt
asset key is set to ["binance_raw", "klines"] to match dbt's source key, so the
staging model wires up downstream of ingestion automatically in the UI.

Level-up later: replace the plain @asset with dagster_dlt's `@dlt_assets` to get
one Dagster asset per dlt resource for free.

Run:  cd <repo> && dagster dev -m orchestration.definitions
"""

from pathlib import Path

import dlt
from dagster import (
    AssetExecutionContext,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from ingest.binance import binance_source
from ingest.config import pg_dsn

REPO_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = REPO_ROOT / "transform"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,  # profiles.yml lives in the project dir
)
dbt_project.prepare_if_dev()  # auto-generates manifest.json during `dagster dev`


@asset(
    key=["binance_raw", "klines"],  # matches the dbt source key -> auto-links
    group_name="ingestion",
    compute_kind="dlt",
)
def binance_klines(context: AssetExecutionContext):
    pipeline = dlt.pipeline(
        pipeline_name="binance",
        destination=dlt.destinations.postgres(pg_dsn()),
        dataset_name="binance_raw",
    )
    load_info = pipeline.run(binance_source())
    context.add_output_metadata({"load_info": str(load_info)})


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


refresh_job = define_asset_job("refresh_all", selection="*")

hourly_schedule = ScheduleDefinition(
    job=refresh_job,
    cron_schedule="0 * * * *",  # top of every hour
)

defs = Definitions(
    assets=[binance_klines, dbt_models],
    jobs=[refresh_job],
    schedules=[hourly_schedule],
    resources={"dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR)},
)
