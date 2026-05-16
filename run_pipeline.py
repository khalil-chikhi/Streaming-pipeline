import os
import sys
import time
import subprocess
import logging
import threading
import signal
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).parent
PRODUCER_SCRIPT = BASE_DIR / "producer.py"
SPARK_SCRIPT = BASE_DIR / "spark_streaming.py"
FLOW_SCRIPT = BASE_DIR / "pipeline_flow.py"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

PYTHON = sys.executable
RESTART_DELAY = 10       
DBT_RUN_INTERVAL = 3600  

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "supervisor.log"),
    ]
)
logger = logging.getLogger("supervisor")

processes = {}
shutdown_event = threading.Event()

def start_process(name: str, script: Path, extra_env: dict = None) -> subprocess.Popen:
    log_file = open(LOG_DIR / f"{name}.log", "a")
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    proc = subprocess.Popen(
        [PYTHON, str(script)],
        stdout=log_file,
        stderr=log_file,
        env=env,
    )
    logger.info("Started %s (PID %d) -> logs/%s.log", name, proc.pid, name)
    return proc

def monitor_process(name: str, script: Path, extra_env: dict = None):
    while not shutdown_event.is_set():
        proc = start_process(name, script, extra_env)
        processes[name] = proc

        while not shutdown_event.is_set():
            try:
                proc.wait(timeout=5)
                if shutdown_event.is_set():
                    break
                logger.warning(
                    "%s exited with code %d. Restarting in %ds...",
                    name, proc.returncode, RESTART_DELAY
                )
                time.sleep(RESTART_DELAY)
                break
            except subprocess.TimeoutExpired:
                continue
            

def run_dbt_scheduler():
    
    logger.info("dbt scheduler: waiting 2 minutes before first run...")
    shutdown_event.wait(timeout=120)

    while not shutdown_event.is_set():
        logger.info("Running Prefect flow (dbt run + test)...")
        try:
            result = subprocess.run(
                [PYTHON, str(FLOW_SCRIPT)],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                logger.info("Prefect flow completed successfully.")
            else:
                logger.error("Prefect flow failed:\n%s", result.stderr[-1000:])
        except Exception as e:
            logger.error("Error running Prefect flow: %s", e)

        next_run = datetime.now() + timedelta(seconds=DBT_RUN_INTERVAL)
        logger.info("Next dbt run at: %s", next_run.strftime("%H:%M:%S"))
        shutdown_event.wait(timeout=DBT_RUN_INTERVAL)



def shutdown(signum, frame):
    logger.info("Shutting down pipeline...")
    shutdown_event.set()

    for name, proc in processes.items():
        logger.info("Terminating %s (PID %d)...", name, proc.pid)
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

    logger.info("All processes stopped.")
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def main():
    logger.info("--- Wikimedia Streaming Pipeline Starting ---")
    logger.info("Logs directory: %s", LOG_DIR)

    producer_thread = threading.Thread(
        target=monitor_process,
        args=("producer", PRODUCER_SCRIPT),
        daemon=True,
    )

    spark_thread = threading.Thread(
        target=monitor_process,
        args=("spark", SPARK_SCRIPT),
        daemon=True,
    )
    dbt_thread = threading.Thread(
        target=run_dbt_scheduler,
        daemon=True,
    )

    producer_thread.start()
    time.sleep(5) 
    spark_thread.start()
    dbt_thread.start()

    logger.info("All components started.")
    logger.info("Monitoring logs in: %s", LOG_DIR)
    
    while not shutdown_event.is_set():
        time.sleep(1)


if __name__ == "__main__":
    main()