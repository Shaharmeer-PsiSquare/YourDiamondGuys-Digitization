import logging
import sys
import argparse
import json
import os
from pathlib import Path
from typing import List, Dict, Any
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent directory to path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config, get_project_root
from src.logger import setup_logging, suppress_third_party_logs, create_log_filename
from src.batch_client import upload_batch_file, create_batch_job, monitor_batch_job
from openai import OpenAI

logger = logging.getLogger(__name__)

def submit_single_job(
    client: OpenAI,
    input_file: str,
    job_index: int,
    batch_dir: Path,
) -> Dict[str, Any]:
    """Submit a single batch job."""
    try:
        input_path = batch_dir / input_file
        
        logger.info(f"Job {job_index}: Uploading {input_file}...")
        file_id = upload_batch_file(client, str(input_path))
        
        logger.info(f"Job {job_index}: Creating batch job...")
        job_id = create_batch_job(client, file_id)
        
        logger.info(f"Job {job_index}: ✓ Created successfully (Job ID: {job_id})")
        
        return {
            "job_index": job_index,
            "job_id": job_id,
            "file_id": file_id,
            "input_file": input_file,
            "status": "success"
        }
    except Exception as e:
        logger.error(f"Job {job_index}: ✗ Failed - {e}")
        return {
            "job_index": job_index,
            "job_id": None,
            "file_id": None,
            "input_file": input_file,
            "status": "failed",
            "error": str(e)
        }

def monitor_all_jobs(client: OpenAI, successful_jobs: List[Dict[str, Any]], poll_interval: int):
    """Wait for all submitted jobs to finish."""
    logger.info("=" * 70)
    logger.info(f"Monitoring {len(successful_jobs)} jobs until completion...")
    logger.info("=" * 70)

    results = []
    with ThreadPoolExecutor(max_workers=len(successful_jobs)) as executor:
        future_to_job = {
            executor.submit(
                monitor_batch_job, 
                client, 
                job["job_id"], 
                poll_interval
            ): job["job_index"]
            for job in successful_jobs
        }

        for future in as_completed(future_to_job):
            job_idx = future_to_job[future]
            try:
                status_data = future.result()
                logger.info(f"Job {job_idx} finished with status: {status_data['status']}")
                results.append(status_data)
            except Exception as e:
                logger.error(f"Error monitoring Job {job_idx}: {e}")
    return results

def main():
    """Submit multiple batch jobs and wait for completion."""
    parser = argparse.ArgumentParser(description="Submit and monitor batch jobs")
    parser.add_argument("--manifest", type=str, default="batch_input_manifest.json")
    parser.add_argument("--poll-interval", type=int, default=60, help="Seconds between status checks")
    parser.add_argument("--job-ids-file", type=str, default="batch_job_ids.json")
    
    args = parser.parse_args()
    
    project_root = get_project_root()
    batch_dir = project_root / "batchfiles"
    setup_logging(level="INFO", log_file=str(project_root / "logs" / create_log_filename("submit_and_wait")))
    suppress_third_party_logs()
    
    try:
        config = load_config()
        manifest_path = batch_dir / args.manifest
        
        if not manifest_path.exists():
            logger.error(f"Manifest not found: {manifest_path}")
            sys.exit(1)
        
        with open(manifest_path, "r") as f:
            manifest = json.load(f)
        
        client = OpenAI(api_key=config.openai_api_key)
        jobs = manifest["jobs"]
        total_jobs = len(jobs)

        # 1. Sequential Submission
        job_results: List[Dict[str, Any]] = []
        for idx, job in enumerate(jobs):
            result = submit_single_job(client, job["file"], job["job_index"], batch_dir)
            job_results.append(result)

            # 5-minute delay between submissions to avoid rate limits
            if idx < total_jobs - 1:
                logger.info("Waiting 5 minutes before next submission...")
                time.sleep(5 * 60)
        
        # # 2. Save Initial Job IDs
        job_ids_path = batch_dir / args.job_ids_file
        with open(job_ids_path, "w") as f:
            json.dump({"jobs": job_results}, f, indent=2)

        # 3. Wait for all "success" submissions to reach terminal state
        successful_submissions = [r for r in job_results if r["status"] == "success"]
        if successful_submissions:
            monitor_all_jobs(client, successful_submissions, args.poll_interval)
        
        logger.info("=" * 70)
        logger.info("✓ All submitted jobs have reached a terminal state (Completed/Failed).")
        logger.info(f"✓ Final Job IDs saved to: {job_ids_path}")
        logger.info("Next step: Run the check_batch_concurrent.py script to download results.")
        logger.info("=" * 70)
        
    except KeyboardInterrupt:
        logger.info("\nStopped by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal Error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()