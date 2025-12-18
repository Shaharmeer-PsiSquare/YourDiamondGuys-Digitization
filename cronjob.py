import json
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path


logger = logging.getLogger(__name__)


def run_command(description: str, script_path: Path, project_root: Path) -> None:
    """
    Run a Python script using the current interpreter and wait until it finishes.
    Raises CalledProcessError if the command fails.
    """
    full_path = project_root / script_path

    if not full_path.exists():
        msg = f"Script not found: {full_path}"
        print(f"❌ {msg}")
        logger.error(msg)
        raise FileNotFoundError(msg)

    msg_start = f"Starting: {description} ({script_path})"
    print(f"\n=== {msg_start} ===")
    logger.info(msg_start)

    result = subprocess.run(
        [sys.executable, str(full_path)],
        cwd=project_root,
        check=False,
    )

    msg_end = f"Finished: {description} (exit code {result.returncode})"
    print(f"=== {msg_end} ===\n")
    if result.returncode == 0:
        logger.info(msg_end)
    else:
        logger.error(msg_end)
        raise subprocess.CalledProcessError(result.returncode, result.args)


def fetch_batch_has_work(project_root: Path) -> bool:
    """
    Check whether the last fetch run produced any diamonds.
    Returns True if diamond_records.json exists and has at least one entry.
    """
    records_path = project_root / "1.FetchFromDB" / "diamond_records.json"
    if not records_path.exists():
        # If the file doesn't exist, treat as no work available.
        msg = f"diamond_records.json not found at {records_path}; treating as no more work."
        print(f"⚠ {msg}")
        logger.warning(msg)
        return False

    try:
        with records_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list) and len(data) > 0:
            msg = f"diamond_records.json contains {len(data)} records."
            print(msg)
            logger.info(msg)
            return True
        else:
            msg = "diamond_records.json is empty; no more diamonds to process."
            print(msg)
            logger.info(msg)
            return False
    except Exception as e:
        msg = f"Failed to inspect diamond_records.json: {e}"
        print(f"⚠ {msg}")
        logger.warning(msg)
        # On error, be conservative and stop looping.
        return False


def main():
    project_root = Path(__file__).resolve().parent

    # Setup logging (local configuration)
    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"cronjob_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )

    logger.info("Cronjob started.")

    # Commands to run sequentially for each batch
    steps = [
        ("Fetch from DB", Path("1.FetchFromDB") / "run.py"),
        ("Create OpenAI batch (concurrent)", Path("2..CallOpenAI") / "scripts" / "create_batch_concurrent.py"),
        ("Submit OpenAI batch (concurrent)", Path("2..CallOpenAI") / "scripts" / "submit_batch_concurrent.py"),
        ("Check OpenAI batch (concurrent)", Path("2..CallOpenAI") / "scripts" / "check_batch_concurrent.py"),
        ("Run scoring and DB ops", Path("3.ScoringAndDBOps") / "run.py"),
    ]

    iteration = 0
    while True:
        iteration += 1
        banner = f"================ CRONJOB ITERATION {iteration} ================"
        print(f"\n{banner}")
        logger.info(banner)

        # Step 1: Fetch from DB
        try:
            run_command(steps[0][0], steps[0][1], project_root)
        except Exception as e:
            logger.error(f"Fetch step failed: {e}")
            print("❌ Fetch step failed; stopping cronjob loop.")
            break

        # Check if there is any work to do
        if not fetch_batch_has_work(project_root):
            msg = "No more diamonds to process. Stopping cronjob loop."
            print(msg)
            logger.info(msg)
            break

        # Run the remaining steps for this batch
        for description, script in steps[1:]:
            try:
                run_command(description, script, project_root)
            except Exception as e:
                logger.error(f"Step '{description}' failed: {e}")
                print(f"❌ Step '{description}' failed; stopping cronjob loop.")
                return

        msg = f"✅ Completed full pipeline iteration {iteration}. Looping again..."
        print(f"{msg}\n")
        logger.info(msg)

    print("All iterations completed; cronjob exiting.")
    logger.info("Cronjob finished; exiting.")


if __name__ == "__main__":
    main()


