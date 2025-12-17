"""
Submit Multiple Batch Jobs Concurrently

This script uploads multiple batch input files and creates batch jobs concurrently.

Usage:
    python 2..CallOpenAI/scripts/submit_batch_concurrent.py [--manifest batch_input_manifest.json] [--max-workers 5]
"""

import logging
import sys
import argparse
import json
import os
from pathlib import Path
from typing import List, Dict, Any
import time

# Add parent directory to path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config, get_project_root
from src.logger import setup_logging, suppress_third_party_logs, create_log_filename
from src.batch_client import upload_batch_file, create_batch_job
from openai import OpenAI

logger = logging.getLogger(__name__)


def submit_single_job(
    client: OpenAI,
    input_file: str,
    job_index: int,
    batch_dir: Path,
) -> Dict[str, Any]:
    """
    Submit a single batch job.
    
    Args:
        client: OpenAI client
        input_file: Path to batch input file
        job_index: Job index
        project_root: Project root directory
        
    Returns:
        Dict with job_id, file_id, and job_index
    """
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


def main():
    """Submit multiple batch jobs concurrently."""
    
    parser = argparse.ArgumentParser(description="Submit multiple batch jobs concurrently")
    parser.add_argument(
        "--manifest",
        type=str,
        default="batch_input_manifest.json",
        help="Path to manifest file (default: batch_input_manifest.json)"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=5,
        help="Maximum number of concurrent workers (default: 5)"
    )
    parser.add_argument(
        "--job-ids-file",
        type=str,
        default="batch_job_ids.json",
        help="Path to file where job IDs will be saved (default: batch_job_ids.json)"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    project_root = get_project_root()
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / create_log_filename("submit_batch_concurrent")
    setup_logging(level="INFO", log_file=str(log_file))
    suppress_third_party_logs()
    batch_dir = project_root / "batchfiles"
    batch_dir.mkdir(exist_ok=True)
    
    logger.info("=" * 70)
    logger.info("Submit Concurrent Batch Jobs")
    logger.info("=" * 70)
    
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = load_config()
        
        # Load manifest from batchfiles directory
        manifest_path = batch_dir / args.manifest
        if not manifest_path.exists():
            logger.error(f"Manifest file not found: {manifest_path}")
            logger.error("Please run 'python 2..CallOpenAI/scripts/create_batch_concurrent.py' first.")
            sys.exit(1)
        
        with open(manifest_path, "r") as f:
            manifest = json.load(f)
        
        logger.info(f"Loaded manifest: {manifest['total_jobs']} jobs, {manifest['total_urls']} total URLs")
        
        # Initialize OpenAI client
        logger.info("Initializing OpenAI client...")
        client = OpenAI(api_key=config.openai_api_key)
        
        # Prepare jobs
        jobs = manifest["jobs"]
        total_jobs = len(jobs)
        logger.info(f"Submitting {total_jobs} batch jobs sequentially with a 10-minute wait between each job...")

        # Submit jobs sequentially with a 10-minute delay between submissions
        job_results: List[Dict[str, Any]] = []
        for idx, job in enumerate(jobs):
            logger.info(f"Starting submission for job {idx + 1}/{total_jobs} (file: {job['file']})")
            result = submit_single_job(
                client=client,
                input_file=job["file"],
                job_index=job["job_index"],
                batch_dir=batch_dir,
            )
            job_results.append(result)

            # Wait 10 minutes before submitting the next job, except after the last one
            if idx < total_jobs - 1:
                logger.info("Waiting 10 minutes before submitting the next batch job...")
                time.sleep(5 * 60)
        
        # Sort results by job_index
        job_results.sort(key=lambda x: x["job_index"])
        
        # Save job IDs
        job_ids_data = {
            "total_jobs": len(job_results),
            "successful_jobs": sum(1 for r in job_results if r["status"] == "success"),
            "failed_jobs": sum(1 for r in job_results if r["status"] == "failed"),
            "jobs": job_results
        }
        
        job_ids_path = batch_dir / args.job_ids_file
        with open(job_ids_path, "w") as f:
            json.dump(job_ids_data, f, indent=2)
        
        # Summary
        successful = [r for r in job_results if r["status"] == "success"]
        failed = [r for r in job_results if r["status"] == "failed"]
        
        logger.info("=" * 70)
        logger.info("✓ Batch job submission completed!")
        logger.info(f"✓ Successful: {len(successful)}/{len(job_results)}")
        if failed:
            logger.warning(f"✗ Failed: {len(failed)}/{len(job_results)}")
        logger.info(f"✓ Job IDs saved to: {job_ids_path}")
        logger.info("=" * 70)
        logger.info("")
        logger.info("Job IDs:")
        for result in successful:
            logger.info(f"  Job {result['job_index']}: {result['job_id']}")
        if failed:
            logger.info("")
            logger.warning("Failed jobs:")
            for result in failed:
                logger.warning(f"  Job {result['job_index']}: {result.get('error', 'Unknown error')}")
        logger.info("")
        logger.info("Next step: Run 'python 2..CallOpenAI/scripts/check_batch_concurrent.py' to monitor and download results")
        
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error submitting batch jobs: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

