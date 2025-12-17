"""
Create Multiple Batch Input Files for Concurrent Processing

This script creates multiple JSONL input files for concurrent batch processing.
It splits URLs into multiple batch jobs that can be processed in parallel.

Default input:
    Uses `1.FetchFromDB/diamond_records.json` produced by the fetch step.
    That file is a JSON list of objects with:
        { "diamond_id": "...", "certificate_link": "https://..." }

Usage:
    python 2..CallOpenAI/scripts/create_batch_concurrent.py [--input-file ../1.FetchFromDB/diamond_records.json] [--batch-size 5] [--jobs 5]
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
    """Create multiple batch input files for concurrent processing."""

    parser = argparse.ArgumentParser(description="Create multiple batch input files for concurrent processing")
    parser.add_argument(
        "--input-file",
        type=str,
        default="../1.FetchFromDB/diamond_records.json",
        help=(
            "Path to JSON file containing URLs. "
            "Defaults to ../1.FetchFromDB/diamond_records.json "
            "(list of {diamond_id, certificate_link})."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Number of URLs per request (max: 5, default: 5)"
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=10,
        help="Number of concurrent batch jobs to create (default: 5)"
    )
    parser.add_argument(
        "--output-prefix",
        type=str,
        default="batch_input",
        help="Prefix for output files (default: batch_input, creates batch_input_0.jsonl, batch_input_1.jsonl, etc.)"
    )
    parser.add_argument(
        "--prompt-file",
        type=str,
        default="prompt.txt",
        help="Path to prompt file (default: prompt.txt)"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    project_root = get_project_root()
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / create_log_filename("create_batch_concurrent")
    setup_logging(level="INFO", log_file=str(log_file))
    suppress_third_party_logs()
    batch_dir = project_root / "batchfiles"
    batch_dir.mkdir(exist_ok=True)

    # Clear any existing files in the batchfiles directory
    for path in batch_dir.iterdir():
        if path.is_file():
            try:
                path.unlink()
                logger.info(f"Deleted existing batch file: {path.name}")
            except Exception as e:
                logger.warning(f"Could not delete file {path}: {e}")
    
    logger.info("=" * 70)
    logger.info("Create Concurrent Batch Input Files")
    logger.info("=" * 70)
    start_time = time.time()
    
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = load_config()
        
        # Load URLs from input file
        input_path = project_root / args.input_file
        logger.info(f"Loading records from: {input_path}")
        
        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            sys.exit(1)
        
        with open(input_path, "r") as file:
            raw_data = json.load(file)

        # Support both:
        #   - list of URL strings
        #   - list of { diamond_id, certificate_link }
        if isinstance(raw_data, list) and raw_data and isinstance(raw_data[0], dict):
            list_of_url = [
                item["certificate_link"]
                for item in raw_data
                if isinstance(item, dict) and item.get("certificate_link")
            ]
            logger.info(
                f"Loaded {len(raw_data)} records from input file, "
                f"extracted {len(list_of_url)} certificate_link URLs"
            )
        else:
            list_of_url = raw_data
            logger.info(f"Loaded {len(list_of_url)} URLs from input file")

        total_urls = len(list_of_url)
        
        # Load prompt (no few-shot examples)
        logger.info("Loading prompt...")
        prompt = load_prompt(args.prompt_file)
        logger.debug(f"Prompt loaded: {len(prompt)} characters")
        # Enforce max batch size of 5
        effective_batch_size = min(max(args.batch_size, 1), 5)
        if effective_batch_size != args.batch_size:
            logger.warning(f"Batch size {args.batch_size} is out of allowed range; using {effective_batch_size} instead.")

        # Calculate URLs per job
        urls_per_job = total_urls // args.jobs
        if total_urls % args.jobs != 0:
            urls_per_job += 1
        
        logger.info(f"Configuration:")
        logger.info(f"  - Total URLs: {total_urls}")
        logger.info(f"  - Number of jobs: {args.jobs}")
        logger.info(f"  - URLs per job: ~{urls_per_job}")
        logger.info(f"  - URLs per request: {effective_batch_size}")
        logger.info(f"  - Requests per job: ~{urls_per_job // effective_batch_size}")
        
        # Helper to process a single job (runs in a thread)
        def process_single_job(job_idx: int) -> Dict[str, Any]:
            start_idx_local = job_idx * urls_per_job
            end_idx_local = min(start_idx_local + urls_per_job, total_urls)

            if start_idx_local >= total_urls:
                # Nothing to do for this job index
                return {}

            job_urls_local = list_of_url[start_idx_local:end_idx_local]
            logger.info(
                f"Job {job_idx + 1}/{args.jobs}: "
                f"Processing URLs {start_idx_local} to {end_idx_local - 1} "
                f"({len(job_urls_local)} URLs)"
            )

            # Create batch requests for this job (no few-shot examples)
            batch_requests_local = create_batch_requests(
                list_of_url=job_urls_local,
                prompt=prompt,
                config=config,
                few_shot_examples=None,
                markdown="",
                urls_per_request=effective_batch_size,
                # Prefix ensures custom_id values are unique across jobs/files
                job_prefix=f"job-{job_idx}-",
            )

            # Create output file path inside batchfiles directory
            output_file_local = f"{args.output_prefix}_{job_idx}.jsonl"
            output_path_local = batch_dir / output_file_local

            # Create batch input file
            create_batch_input_file(batch_requests_local, str(output_path_local))
            logger.info(
                f"  ✓ Created {output_file_local} with {len(batch_requests_local)} requests"
            )

            return {
                "job_index": job_idx,
                "file": output_file_local,
                "urls": len(job_urls_local),
                "requests": len(batch_requests_local),
                "start_url_idx": start_idx_local,
                "end_url_idx": end_idx_local - 1,
            }

        # Create batch files for each job concurrently
        batch_files: List[str] = []
        job_info: List[Dict[str, Any]] = []

        max_workers = args.jobs if args.jobs > 0 else 1
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_single_job, job_idx): job_idx
                for job_idx in range(args.jobs)
            }

            for future in as_completed(futures):
                info = future.result()
                if not info:
                    continue
                batch_files.append(info["file"])
                job_info.append(info)
        
        # Save job manifest inside batchfiles directory
        manifest_file = batch_dir / f"{args.output_prefix}_manifest.json"
        with open(manifest_file, "w") as f:
            json.dump({
                "total_urls": total_urls,
                "total_jobs": len(batch_files),
                "batch_size": args.batch_size,
                "jobs": job_info
            }, f, indent=2)
        
        logger.info("=" * 70)
        logger.info("✓ All batch input files created successfully!")
        logger.info(f"✓ Total jobs: {len(batch_files)}")
        logger.info(f"✓ Manifest file: {manifest_file}")
        logger.info("=" * 70)
        logger.info("")
        logger.info("Created files:")
        for job in job_info:
            logger.info(f"  - {job['file']} ({job['requests']} requests, {job['urls']} URLs)")
        logger.info("")
        logger.info("Next step: Run 'python 2..CallOpenAI/scripts/submit_batch_concurrent.py' to submit all jobs concurrently")
        elapsed = time.time() - start_time
        logger.info(f"Total time to create batch files: {elapsed:.2f} seconds")
        logger.info(f"Execution completed at: {datetime.now().isoformat(timespec='seconds')}")
        
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error creating batch files: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

