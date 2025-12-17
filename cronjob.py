import subprocess
import sys
from pathlib import Path


def run_command(description, script_path):
    """
    Run a Python script using the current interpreter and wait until it finishes.
    Raises CalledProcessError if the command fails.
    """
    print(f"\n=== Starting: {description} ({script_path}) ===")
    # Ensure we run relative to the project root (location of this file)
    project_root = Path(__file__).resolve().parent
    full_path = project_root / script_path

    if not full_path.exists():
        raise FileNotFoundError(f"Script not found: {full_path}")

    result = subprocess.run(
        [sys.executable, str(full_path)],
        cwd=project_root,
        check=True,
    )
    print(f"=== Finished: {description} (exit code {result.returncode}) ===\n")


def main():
    # Commands to run sequentially
    steps = [
        ("Fetch from DB", Path("1.FetchFromDB") / "run.py"),
        ("Create OpenAI batch (concurrent)", Path("2..CallOpenAI") / "scripts" / "create_batch_concurrent.py"),
        ("Submit OpenAI batch (concurrent)", Path("2..CallOpenAI") / "scripts" / "submit_batch_concurrent.py"),
        ("Check OpenAI batch (concurrent)", Path("2..CallOpenAI") / "scripts" / "check_batch_concurrent.py"),
        ("Run scoring and DB ops", Path("3.ScoringAndDBOps") / "run_scoring.py"),
    ]

    for description, script in steps:
        run_command(description, script)

    print("All steps completed successfully.")


if __name__ == "__main__":
    main()


