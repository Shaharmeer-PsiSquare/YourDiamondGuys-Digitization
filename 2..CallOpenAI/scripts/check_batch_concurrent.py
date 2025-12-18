"""
Download and Parse Completed Batch Results

This script checks the current status of batch jobs and downloads results 
for those that are already completed. It does not wait/poll for pending jobs.

Usage:
    python 2..CallOpenAI/scripts/check_batch_concurrent.py [--job-ids-file batch_job_ids.json]
"""

import logging
import sys
import argparse
import json
import math
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
import os

# Add parent directory to path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config, get_project_root
from src.logger import setup_logging, suppress_third_party_logs, create_log_filename
from src.batch_client import download_batch_results, parse_batch_response
from openai import OpenAI

logger = logging.getLogger(__name__)


def get_job_status_simple(client: OpenAI, job_id: str, job_index: int) -> Dict[str, Any]:
    """Retrieves current status of a job without waiting."""
    try:
        batch_job = client.batches.retrieve(job_id)
        return {
            "job_index": job_index,
            "job_id": job_id,
            "status": batch_job.status,
            "output_file_id": batch_job.output_file_id,
            "completed_count": getattr(batch_job.request_counts, 'completed', 0),
            "total_count": getattr(batch_job.request_counts, 'total', 0)
        }
    except Exception as e:
        logger.error(f"Job {job_index}: Error retrieving status - {e}")
        return {"job_index": job_index, "job_id": job_id, "status": "error", "error": str(e)}


def download_and_parse_results(
    client: OpenAI,
    output_file_id: str,
    job_index: int,
    list_of_url: List[str],
    batch_size: int,
    start_url_idx: int,
    end_url_idx: int
) -> List[Dict[str, Any]]:
    """Download and parse results for a single completed job."""
    try:
        logger.info(f"Job {job_index}: Downloading results from file {output_file_id}...")
        results = download_batch_results(client, output_file_id)
        
        # Get specific URLs assigned to this job slice
        job_urls = list_of_url[start_url_idx:end_url_idx + 1]
        
        all_parsed_results = []
        for result in results:
            custom_id = result.get('custom_id', '')
            try:
                # Extract local request index from "job-X-request-Y"
                parts = custom_id.split('-')
                numeric_parts = [int(p) for p in parts if p.isdigit()]
                if numeric_parts:
                    request_idx = numeric_parts[-1]
                    start_idx = request_idx * batch_size
                    end_idx = min(start_idx + batch_size, len(job_urls))
                    url_batch = job_urls[start_idx:end_idx]
                else:
                    url_batch = job_urls
            except (ValueError, IndexError):
                url_batch = job_urls
            
            parsed = parse_batch_response(result, url_batch)
            if parsed:
                all_parsed_results.extend(parsed)
        
        return all_parsed_results
    except Exception as e:
        logger.error(f"Job {job_index}: Error during download/parse - {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Download completed batch results")
    parser.add_argument("--job-ids-file", type=str, default="batch_job_ids.json")
    parser.add_argument("--manifest-file", type=str, default="batch_input_manifest.json")
    parser.add_argument("--urls-file", type=str, default="../1.FetchFromDB/diamond_records.json")
    parser.add_argument("--output-file", type=str, default="batch_results_concurrent.json")
    
    args = parser.parse_args()
    
    project_root = get_project_root()
    batch_dir = project_root / "batchfiles"
    setup_logging(level="INFO", log_file=str(project_root / "logs" / create_log_filename("download_results")))
    suppress_third_party_logs()
    
    # Clean up old results
    scoring_path = project_root.parent / "3.ScoringAndDBOps" / "OpenAIresults.json"
    if scoring_path.exists():
        scoring_path.unlink()

    try:
        config = load_config()
        client = OpenAI(api_key=config.openai_api_key)

        # Load Job IDs
        with open(batch_dir / args.job_ids_file, "r") as f:
            job_data = json.load(f)
        
        # Load URLs
        with open(project_root / args.urls_file, "r") as f:
            raw_urls = json.load(f)
            list_of_url = [item["certificate_link"] for item in raw_urls] if isinstance(raw_urls[0], dict) else raw_urls

        # Load Manifest for indexing logic
        with open(batch_dir / args.manifest_file, "r") as f:
            manifest = json.load(f)
            batch_size = manifest.get("batch_size", 5)
            job_map = {j["job_index"]: (j["start_url_idx"], j["end_url_idx"]) for j in manifest["jobs"]}

        # 1. Quick Check Status
        logger.info(f"Checking status for {len(job_data['jobs'])} jobs...")
        completed_jobs = []
        
        for job in job_data["jobs"]:
            if job["status"] != "success": continue
            
            status_info = get_job_status_simple(client, job["job_id"], job["job_index"])
            if status_info["status"] == "completed":
                completed_jobs.append(status_info)
            else:
                logger.info(f"Job {job['job_index']} is still {status_info['status']} ({status_info['completed_count']}/{status_info['total_count']})")

        if not completed_jobs:
            logger.warning("No jobs are completed yet. Exiting.")
            sys.exit(0)

        # 2. Download and Parse
        logger.info(f"Downloading results for {len(completed_jobs)} jobs...")
        all_results = []
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for info in completed_jobs:
                s_idx, e_idx = job_map.get(info["job_index"], (0, 0))
                futures.append(executor.submit(
                    download_and_parse_results, client, info["output_file_id"], 
                    info["job_index"], list_of_url, batch_size, s_idx, e_idx
                ))
            
            for future in as_completed(futures):
                all_results.extend(future.result())

        # 3. Save Output
        final_output = batch_dir / args.output_file
        with open(final_output, "w") as f:
            json.dump(all_results, f, indent=2)
        
        with open(scoring_path, "w") as f:
            json.dump(all_results, f, indent=2)

        logger.info("=" * 70)
        logger.info(f"✓ Success! Parsed {len(all_results)} total items.")
        logger.info(f"✓ Saved to: {final_output}")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()