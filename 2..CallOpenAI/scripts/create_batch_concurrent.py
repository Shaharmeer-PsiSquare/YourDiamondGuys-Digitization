"""
Create Multiple Batch Input Files for Concurrent Processing

This script creates multiple JSONL input files. It fills each batch to a 
maximum number of requests (e.g., 20) rather than dividing them equally.

Usage:
    python 2..CallOpenAI/scripts/create_batch_concurrent.py --max-requests-per-job 20 --batch-size 5
"""

import json
import logging
import sys
import argparse
from pathlib import Path
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List

# Add parent directory to path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config, get_project_root
from src.logger import setup_logging, suppress_third_party_logs, create_log_filename
from src.prompt_loader import load_prompt
from src.batch_client import create_batch_requests, create_batch_input_file

logger = logging.getLogger(__name__)


def main():
    """Create batch input files filled to a specific request capacity."""

    parser = argparse.ArgumentParser(description="Create batch files with a fixed request capacity")
    parser.add_argument(
        "--input-file",
        type=str,
        default="../1.FetchFromDB/diamond_records.json",
        help="Path to JSON file containing URLs."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Number of URLs per single request (max: 5)"
    )
    parser.add_argument(
        "--max-requests-per-job",
        type=int,
        default=20,
        help="Maximum number of requests allowed per batch file (default: 20)"
    )
    parser.add_argument(
        "--output-prefix",
        type=str,
        default="batch_input",
        help="Prefix for output files"
    )
    parser.add_argument(
        "--prompt-file",
        type=str,
        default="prompt.txt",
        help="Path to prompt file"
    )
    
    args = parser.parse_args()
    
    # Setup paths and logging
    project_root = get_project_root()
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / create_log_filename("create_batch_concurrent")
    setup_logging(level="INFO", log_file=str(log_file))
    suppress_third_party_logs()
    
    batch_dir = project_root / "batchfiles"
    batch_dir.mkdir(exist_ok=True)

    # Clear existing files
    for path in batch_dir.iterdir():
        if path.is_file():
            try:
                path.unlink()
            except Exception as e:
                logger.warning(f"Could not delete {path}: {e}")
    
    logger.info("=" * 70)
    logger.info("Create Capacity-Based Batch Input Files")
    logger.info("=" * 70)
    start_time = time.time()
    
    try:
        config = load_config()
        input_path = project_root / args.input_file
        
        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            sys.exit(1)
        
        with open(input_path, "r") as file:
            raw_data = json.load(file)

        # Extract URLs
        if isinstance(raw_data, list) and raw_data and isinstance(raw_data[0], dict):
            list_of_url = [item["certificate_link"] for item in raw_data if item.get("certificate_link")]
        else:
            list_of_url = raw_data

        total_urls = len(list_of_url)
        prompt = load_prompt(args.prompt_file)
        
        # Logic for filling batches to capacity
        effective_batch_size = min(max(args.batch_size, 1), 5)
        urls_per_batch_limit = args.max_requests_per_job * effective_batch_size
        
        # Determine total jobs needed (e.g., 34 requests / 20 capacity = 2 jobs)
        num_jobs = (total_urls + urls_per_batch_limit - 1) // urls_per_batch_limit

        logger.info(f"Configuration:")
        logger.info(f"  - Total URLs: {total_urls}")
        logger.info(f"  - URLs per Request: {effective_batch_size}")
        logger.info(f"  - Max Requests per Job: {args.max_requests_per_job}")
        logger.info(f"  - Max URLs per Job: {urls_per_batch_limit}")
        logger.info(f"  - Calculated Total Jobs: {num_jobs}")

        def process_single_job(job_idx: int) -> Dict[str, Any]:
            start_idx = job_idx * urls_per_batch_limit
            end_idx = min(start_idx + urls_per_batch_limit, total_urls)

            if start_idx >= total_urls:
                return {}

            job_urls = list_of_url[start_idx:end_idx]
            
            # Create requests for this specific slice
            batch_requests = create_batch_requests(
                list_of_url=job_urls,
                prompt=prompt,
                config=config,
                few_shot_examples=None,
                markdown="",
                urls_per_request=effective_batch_size,
                job_prefix=f"job-{job_idx}-",
            )

            output_file = f"{args.output_prefix}_{job_idx}.jsonl"
            output_path = batch_dir / output_file
            create_batch_input_file(batch_requests, str(output_path))
            
            return {
                "job_index": job_idx,
                "file": output_file,
                "urls": len(job_urls),
                "requests": len(batch_requests),
                "start_url_idx": start_idx,
                "end_url_idx": end_idx - 1,
            }

        job_info: List[Dict[str, Any]] = []
        # Use ThreadPool to process the calculated number of jobs
        with ThreadPoolExecutor(max_workers=min(num_jobs, 10)) as executor:
            futures = {executor.submit(process_single_job, i): i for i in range(num_jobs)}
            for future in as_completed(futures):
                info = future.result()
                if info:
                    job_info.append(info)

        # Sort job info by index for the manifest
        job_info.sort(key=lambda x: x["job_index"])

        # Save manifest
        manifest_file = batch_dir / f"{args.output_prefix}_manifest.json"
        with open(manifest_file, "w") as f:
            json.dump({
                "total_urls": total_urls,
                "total_jobs": len(job_info),
                "max_requests_per_job": args.max_requests_per_job,
                "jobs": job_info
            }, f, indent=2)
        
        logger.info("=" * 70)
        logger.info(f"✓ Created {len(job_info)} batch files.")
        for job in job_info:
            logger.info(f"  - {job['file']}: {job['requests']} requests ({job['urls']} URLs)")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()