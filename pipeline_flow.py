import subprocess
# import os
import time
from pathlib import Path
from prefect import flow, task, get_run_logger
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState

DBT_PROJECT_DIR = Path(__file__).parent / "wikimedia_dbt"

DATABRICKS_HOST = "dbc-16c19e93-6368.cloud.databricks.com"
***REMOVED***
DATABRICKS_JOB_NAME = "wikimedia-kafka-ingestion"

@task(name="trigger-databricks-job", retries=2, retry_delay_seconds=30)
def trigger_databricks_job() -> int:
    
    logger = get_run_logger()

    client = WorkspaceClient(
        host=DATABRICKS_HOST,
        token=DATABRICKS_TOKEN,
    )

    jobs = list(client.jobs.list(name=DATABRICKS_JOB_NAME))
    if not jobs:
        raise RuntimeError(f"Job '{DATABRICKS_JOB_NAME}' not found in Databricks.")

    job_id = jobs[0].job_id
    logger.info("Triggering Databricks job: %s (id=%d)", DATABRICKS_JOB_NAME, job_id)

    run = client.jobs.run_now(job_id=job_id)
    run_id = run.run_id
    logger.info("Job triggered. Run ID: %d", run_id)


    while True:
        run_state = client.jobs.get_run(run_id=run_id)
        state = run_state.state.life_cycle_state

        logger.info("Job state: %s", state)

        if state == RunLifeCycleState.TERMINATED:
            result = run_state.state.result_state
            if result.value != "SUCCESS":
                raise RuntimeError(f"Databricks job failed with result: {result}")
            logger.info("Databricks job completed successfully.")
            break
        elif state in (RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR):
            raise RuntimeError(f"Databricks job ended unexpectedly: {state}")

        time.sleep(30)

    return run_id


@task(name="run-dbt", retries=2, retry_delay_seconds=30)
def run_dbt(command: list[str], step_name: str) -> bool:
    logger = get_run_logger()

    full_cmd = ["dbt"] + command + ["--project-dir", str(DBT_PROJECT_DIR)]
    logger.info("[%s] Running: %s", step_name, " ".join(full_cmd))

    result = subprocess.run(
        full_cmd,
        capture_output=True,
        text=True,
        cwd=str(DBT_PROJECT_DIR),
    )

    if result.returncode != 0:
        logger.error("[%s] FAILED:\n%s", step_name, result.stdout[-2000:])
        raise RuntimeError(f"{step_name} failed.")

    logger.info("[%s] SUCCESS", step_name)
    return True


@task(name="log-summary")
def log_summary(run_id: int, dbt_run: bool, dbt_test: bool):
    logger = get_run_logger()
    logger.info(
        "Pipeline complete | databricks_run_id=%d | dbt_run=%s | dbt_test=%s",
        run_id,
        "OK" if dbt_run else "FAIL",
        "OK" if dbt_test else "FAIL",
    )


@flow(
    name="wikimedia-pipeline",
    description="Every 30min: Databricks ingestion → dbt transforms → dbt tests",
)
def wikimedia_pipeline_flow():
    logger = get_run_logger()
    logger.info("--- Wikimedia Pipeline Flow starting ---")

    
    run_id = trigger_databricks_job()

    dbt_run = run_dbt(
        ["run", "--select", "staging marts"],
        step_name="dbt-run",
    )

    dbt_test = run_dbt(
        ["test", "--select", "staging"],
        step_name="dbt-test",
    )

    
    log_summary(run_id, dbt_run, dbt_test)

    logger.info("--- Pipeline Flow complete ---")


if __name__ == "__main__":
    wikimedia_pipeline_flow()