"""
Check Multiple Batch Jobs Concurrently and Download Results

This script monitors multiple batch jobs concurrently and downloads results when completed.

Usage:
    python 2..CallOpenAI/scripts/check_batch_concurrent.py [--job-ids-file batch_job_ids.json] [--poll-interval 60]
"""

import logging
import sys
import argparse
import json
import math
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional

# Add parent directory to path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config, get_project_root
from src.logger import setup_logging, suppress_third_party_logs, create_log_filename
from src.batch_client import monitor_batch_job, download_batch_results, parse_batch_response
from openai import OpenAI

logger = logging.getLogger(__name__)


def check_single_job(
    client: OpenAI,
    job_id: str,
    job_index: int,
    poll_interval: int = 60,
    max_wait_time: Optional[int] = None,
    no_wait: bool = False
) -> Dict[str, Any]:
    """
    Check status of a single batch job.
    """
    try:
        # Helper to extract errors if they exist
        def get_error_details(batch_obj):
            if hasattr(batch_obj, 'errors') and batch_obj.errors:
                return "; ".join([f"{e.code}: {e.message}" for e in batch_obj.errors.data])
            return None

        if no_wait:
            batch_job = client.batches.retrieve(job_id)
            status = batch_job.status
            completed = getattr(batch_job.request_counts, 'completed', 0)
            total = getattr(batch_job.request_counts, 'total', 0)
            
            logger.info(f"Job {job_index}: Status={status}, Completed={completed}/{total}")
            
            if status == "completed":
                return {
                    "job_index": job_index,
                    "job_id": job_id,
                    "status": status,
                    "output_file_id": batch_job.output_file_id,
                    "completed": completed,
                    "total": total
                }
            elif status in ["failed", "cancelled", "expired"]:
                error_details = get_error_details(batch_job)
                error_msg = f"Job finished with status: {status}"
                if error_details:
                    error_msg += f" | Details: {error_details}"
                
                logger.error(f"Job {job_index} FAILED: {error_msg}")

                return {
                    "job_index": job_index,
                    "job_id": job_id,
                    "status": status,
                    "output_file_id": None,
                    "completed": completed,
                    "total": total,
                    "error": error_msg
                }
            else:
                return {
                    "job_index": job_index,
                    "job_id": job_id,
                    "status": status,
                    "output_file_id": None,
                    "completed": completed,
                    "total": total
                }
        else:
            # If we are waiting, we use the monitor function
            # Note: This blocks the thread until completion or timeout
            job_status = monitor_batch_job(
                client, job_id, poll_interval, max_wait_time
            )
            
            job_obj = job_status.get("job")
            request_counts = getattr(job_obj, 'request_counts', None) if job_obj else None
            
            return {
                "job_index": job_index,
                "job_id": job_id,
                "status": job_status["status"],
                "output_file_id": job_status.get("output_file_id"),
                "completed": getattr(request_counts, 'completed', 0) if request_counts else 0,
                "total": getattr(request_counts, 'total', 0) if request_counts else 0
            }
    except Exception as e:
        logger.error(f"Job {job_index}: Error checking status - {e}")
        return {
            "job_index": job_index,
            "job_id": job_id,
            "status": "error",
            "output_file_id": None,
            "error": str(e)
        }


def download_and_parse_results(
    client: OpenAI,
    output_file_id: str,
    job_index: int,
    list_of_url: List[str],
    batch_size: int,
    start_url_idx: int,
    end_url_idx: int
) -> List[Dict[str, Any]]:
    """
    Download and parse results for a single job.
    """
    try:
        logger.info(f"Job {job_index}: Downloading results...")
        results = download_batch_results(client, output_file_id)
        logger.info(f"Job {job_index}: Downloaded {len(results)} results")
        
        # Get URLs for this job
        job_urls = list_of_url[start_url_idx:end_url_idx + 1]
        
        # Parse results
        all_parsed_results = []
        for result in results:
            custom_id = result.get('custom_id', '')
            try:
                # Custom ID format: request-{batch_idx}-{request_idx} or similar
                # Assuming format creates a link to the batch index
                if '-' in custom_id:
                    parts = custom_id.split('-')
                    # This logic depends on your specific custom_id format from create_batch.py
                    # Assuming basic sequential mapping for now
                    batch_idx = int(parts[1]) 
                    start_idx = batch_idx * batch_size
                    end_idx = min(start_idx + batch_size, len(job_urls))
                    url_batch = job_urls[start_idx:end_idx]
                else:
                    url_batch = job_urls
            except (ValueError, IndexError):
                url_batch = job_urls
            
            parsed = parse_batch_response(result, url_batch)
            if parsed:
                all_parsed_results.extend(parsed)
        
        logger.info(f"Job {job_index}: Parsed {len(all_parsed_results)} results")
        return all_parsed_results
        
    except Exception as e:
        logger.error(f"Job {job_index}: Error downloading/parsing results - {e}")
        return []


def main():
    """Check multiple batch jobs concurrently and download results."""
    
    parser = argparse.ArgumentParser(description="Check multiple batch jobs concurrently")
    parser.add_argument(
        "--job-ids-file",
        type=str,
        default="batch_job_ids.json",
        help="Path to file containing job IDs (default: batch_job_ids.json)"
    )
    parser.add_argument(
        "--manifest-file",
        type=str,
        default="batch_input_manifest.json",
        help="Path to manifest file (default: batch_input_manifest.json)"
    )
    parser.add_argument(
        "--urls-file",
        type=str,
        default="prompts/input/test.json",
        help="Path to original URLs file (default: prompts/input/test.json)"
    )
    parser.add_argument(
        "--output-file",
        type=str,
        default="batch_results_concurrent.json",
        help="Path to output file for parsed results (default: batch_results_concurrent.json)"
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=60,
        help="Seconds to wait between status checks (default: 60)"
    )
    parser.add_argument(
        "--max-wait-time",
        type=int,
        default=None,
        help="Maximum time to wait in seconds (default: unlimited)"
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Don't wait for completion, just check current status and exit"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Maximum number of concurrent workers (default: matches job count)"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    project_root = get_project_root()
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / create_log_filename("check_batch_concurrent")
    setup_logging(level="INFO", log_file=str(log_file))
    suppress_third_party_logs()
    batch_dir = project_root / "batchfiles"
    batch_dir.mkdir(exist_ok=True)
    
    logger.info("=" * 70)
    logger.info("Check Concurrent Batch Jobs")
    logger.info("=" * 70)
    
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = load_config()
        
        # Load job IDs from batchfiles directory
        job_ids_path = batch_dir / args.job_ids_file
        if not job_ids_path.exists():
            logger.error(f"Job IDs file not found: {job_ids_path}")
            logger.error("Please run 'python 2..CallOpenAI/scripts/submit_batch_concurrent.py' first.")
            sys.exit(1)
        
        with open(job_ids_path, "r") as f:
            job_ids_data = json.load(f)
        
        successful_jobs = [j for j in job_ids_data["jobs"] if j["status"] == "success"]
        logger.info(f"Loaded {len(successful_jobs)} successful jobs")
        
        # Load URLs
        urls_path = project_root / args.urls_file
        list_of_url = []
        if urls_path.exists():
            with open(urls_path, "r") as f:
                list_of_url = json.load(f)
            logger.info(f"Loaded {len(list_of_url)} URLs from original file")
        else:
            logger.warning(f"URLs file not found: {urls_path}")

        # Load manifest from batchfiles directory
        manifest_path = batch_dir / args.manifest_file
        if manifest_path.exists():
            with open(manifest_path, "r") as f:
                manifest = json.load(f)
            batch_size = manifest["batch_size"]
            job_url_ranges = {
                job["job_index"]: (job["start_url_idx"], job["end_url_idx"])
                for job in manifest["jobs"]
            }
        else:
            # Fallback: Calculate ranges if manifest is missing
            logger.warning(f"Manifest file not found: {manifest_path}. Calculating ranges manually.")
            batch_size = 5 # Default assumption
            job_url_ranges = {}
            if list_of_url and successful_jobs:
                total_urls = len(list_of_url)
                urls_per_job = math.ceil(total_urls / len(successful_jobs))
                for i, job in enumerate(successful_jobs):
                    start = i * urls_per_job
                    end = min((i + 1) * urls_per_job - 1, total_urls - 1)
                    job_url_ranges[job["job_index"]] = (start, end)

        # Initialize OpenAI client
        logger.info("Initializing OpenAI client...")
        client = OpenAI(api_key=config.openai_api_key)
        
        # Determine max workers
        # If not waiting, 5 is fine. If waiting, we want 1 worker per job to avoid blocking.
        if args.max_workers is None:
            if args.no_wait:
                max_workers = 5
            else:
                max_workers = min(20, len(successful_jobs))
        else:
            max_workers = args.max_workers

        # Check all jobs concurrently
        logger.info(f"Checking {len(successful_jobs)} jobs with {max_workers} concurrent workers...")
        if args.no_wait:
            logger.info("Mode: Check status only (not waiting for completion)")
        else:
            logger.info(f"Mode: Monitor until completion (polling every {args.poll_interval}s)")
        
        job_statuses = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_job = {
                executor.submit(
                    check_single_job,
                    client,
                    job["job_id"],
                    job["job_index"],
                    args.poll_interval,
                    args.max_wait_time,
                    args.no_wait
                ): job["job_index"]
                for job in successful_jobs
            }
            
            for future in as_completed(future_to_job):
                status = future.result()
                job_statuses.append(status)
        
        # Sort by job_index
        job_statuses.sort(key=lambda x: x["job_index"])
        
        # Check completion status
        completed_jobs = [j for j in job_statuses if j["status"] == "completed"]
        pending_jobs = [j for j in job_statuses if j["status"] not in ["completed", "failed", "cancelled", "expired", "error"]]
        failed_jobs = [j for j in job_statuses if j["status"] in ["failed", "cancelled", "expired", "error"]]
        
        logger.info("=" * 70)
        logger.info(f"Status Summary:")
        logger.info(f"  Completed: {len(completed_jobs)}/{len(job_statuses)}")
        if pending_jobs:
            logger.info(f"  Pending: {len(pending_jobs)}/{len(job_statuses)}")
        if failed_jobs:
            logger.warning(f"  Failed: {len(failed_jobs)}/{len(job_statuses)}")
        logger.info("=" * 70)
        
        if args.no_wait and pending_jobs:
            logger.info("")
            logger.info("Some jobs are still pending. Run without --no-wait to monitor until completion.")
            sys.exit(0)
        
        if not completed_jobs:
            logger.error("No jobs have completed yet.")
            sys.exit(1)
        
        # Download and parse results from completed jobs
        logger.info("")
        logger.info(f"Downloading and parsing results from {len(completed_jobs)} completed jobs...")
        
        all_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_job = {}
            for job_status in completed_jobs:
                if job_status.get("output_file_id"):
                    start_idx, end_idx = job_url_ranges.get(
                        job_status["job_index"],
                        (0, len(list_of_url) - 1)
                    )
                    
                    future = executor.submit(
                        download_and_parse_results,
                        client,
                        job_status["output_file_id"],
                        job_status["job_index"],
                        list_of_url,
                        batch_size,
                        start_idx,
                        end_idx
                    )
                    future_to_job[future] = job_status["job_index"]
            
            for future in as_completed(future_to_job):
                results = future.result()
                all_results.extend(results)
        
        # Save results inside batchfiles directory
        output_path = batch_dir / args.output_file
        with open(output_path, "w") as f:
            json.dump(all_results, f, indent=2)
        
        logger.info("=" * 70)
        logger.info("✓ All results downloaded and parsed successfully!")
        logger.info(f"✓ Total results: {len(all_results)}")
        logger.info(f"✓ Output file: {output_path}")
        logger.info("=" * 70)
        
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error checking batch jobs: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()