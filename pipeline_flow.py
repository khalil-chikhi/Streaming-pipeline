import subprocess
from prefect import flow, task, get_run_logger
from pathlib import Path
from prefect.server.schemas.schedules import IntervalSchedule
from datetime import timedelta, datetime, timezone
import duckdb


DBT_PROJECT_DIR = Path(__file__).parent / "wikimedia_dbt"

@task(name="dbt-run", retries=2, retry_delay_seconds=30)
def run_dbt(command: list[str], step_name: str)-> bool:
    logger = get_run_logger()
    
    full_cmd = ["dbt"] + command + ["--project-dir", str(DBT_PROJECT_DIR)]
    logger.info("[%s] Running: %s", step_name, " ".join(full_cmd))
    result = subprocess.run(
        full_cmd,
        capture_output=True,
        text=True,
        cwd=str(DBT_PROJECT_DIR)
        )
    if result.returncode != 0:
        logger.error("[%s] FAILED:\n%s", step_name, result.stdout[-2000:])
        raise RuntimeError(f"{step_name} failed.")
    logger.info("[%s] SUCCESS:\n%s", step_name, result.stdout[-1000:])
    return True

@task(name="check-delta-freshness")
def check_freshness()->dict:
    logger = get_run_logger()
    con =duckdb.connect("C:/tmp/wikimedia.duckdb")

    result = con.sql("""
        SELECT
            count(*) as row_count,
            max(processed_at) as latest_record ,
            datediff('minute', max(processed_at), now()) as lag_minutes
        FROM delta_scan('C:/tmp/delta/wikimedia_events')
        """).fetchone()
    row_count, latest, lag = result
    logger.info("Freshness: %d rows, latest=%s, lag=%s min", row_count, latest, lag)
    
    if lag and lag>120:
        raise RuntimeError(f"Data is stale. Lag={lag} minutes. Check producer and Spark job")
        # logger.warning(f"Data is stale. Lag={lag} minutes. Check producer and Spark job")
    return {
        "row_count": row_count,
        "latest_record": latest,
        "lag_minutes": lag
    }
@task(name="log-summary")
def log_summary(freshness: dict, dbt_run: bool, dbt_test: bool):
    logger = get_run_logger()
    logger.info(
        "Pipeline run complete, rows=%d, lag=%s min, dbt_run=%s, dbt_test=%s",
        freshness["row_count"],
        freshness['lag_minutes'],
        "OK" if dbt_run else "FAIL",
        "OK" if dbt_test else "FAIL"
    )

@flow(
    name="wikimedia-pipeline-hourly",
    description="Hourly dbt transformation and data quality run for Wikimedia streaming pipeline.",
)
def wikimedia_pipeline_flow():
    logger = get_run_logger()
    logger.info("--Wikimedia Pipeline Starting--")
    
    freshness = check_freshness()
    dbt_run = run_dbt(
        ["run", "--select", "staging marts"],
        step_name="dbt-run",
    )

    dbt_test = run_dbt(
        ["test", "--select", "staging"],
        step_name="dbt-test",
    )

    log_summary(freshness, dbt_run, dbt_test)

    logger.info("--Pipeline Flow complete--")


if __name__ == "__main__":
    wikimedia_pipeline_flow()