"""
Check Batch Job Status and Download Results

This script monitors a batch job and downloads results when completed.

Usage:
    python 2..CallOpenAI/scripts/check_batch.py [--job-id-file batch_job_id.txt] [--output-file batch_results.jsonl] [--poll-interval 60]
"""

import logging
import sys
import argparse
import os
import json
from pathlib import Path

# Add parent directory to path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config, get_project_root
from src.logger import setup_logging, suppress_third_party_logs, create_log_filename
from src.batch_client import monitor_batch_job, download_batch_results, parse_batch_response
from openai import OpenAI

logger = logging.getLogger(__name__)


def process_results(results: list, list_of_url: list, urls_per_request: int = 5) -> list:
    """
    Process batch results and extract parsed data.
    
    Args:
        results: List of batch result dictionaries
        list_of_url: Original list of URLs (for enrichment)
        urls_per_request: Number of URLs per request
        
    Returns:
        List of parsed result dictionaries
    """
    all_parsed_results = []
    
    for result in results:
        custom_id = result.get('custom_id', '')
        # Extract batch index from custom_id (e.g., "request-0" -> 0)
        try:
            batch_idx = int(custom_id.split('-')[1]) if '-' in custom_id else 0
            start_idx = batch_idx * urls_per_request
            end_idx = min(start_idx + urls_per_request, len(list_of_url))
            url_batch = list_of_url[start_idx:end_idx]
        except (ValueError, IndexError):
            # Fallback: use all URLs if we can't parse the index
            url_batch = list_of_url
        
        # Parse this result with its corresponding URL batch
        parsed = parse_batch_response(result, url_batch)
        if parsed:
            all_parsed_results.extend(parsed)
    
    return all_parsed_results


def main():
    """Check batch job status and download results."""
    
    parser = argparse.ArgumentParser(description="Check batch job status and download results")
    parser.add_argument(
        "--job-id",
        type=str,
        default=None,
        help="Batch job ID (if not provided, will read from job-id-file)"
    )
    parser.add_argument(
        "--job-id-file",
        type=str,
        default="batch_job_id.txt",
        help="Path to file containing job ID (default: batch_job_id.txt)"
    )
    parser.add_argument(
        "--output-file",
        type=str,
        default="batch_results.jsonl",
        help="Path to output file for raw results (default: batch_results.jsonl)"
    )
    parser.add_argument(
        "--parsed-output-file",
        type=str,
        default="batch_results_parsed.json",
        help="Path to output file for parsed results (default: batch_results_parsed.json)"
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
        "--urls-file",
        type=str,
        default="prompts/input/test.json",
        help="Path to original URLs file (for result enrichment, default: prompts/input/test.json)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Number of URLs per request (default: 5)"
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Don't wait for completion, just check current status and exit"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    project_root = get_project_root()
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / create_log_filename("check_batch")
    setup_logging(level="INFO", log_file=str(log_file))
    suppress_third_party_logs()
    batch_dir = project_root / "batchfiles"
    batch_dir.mkdir(exist_ok=True)
    
    logger.info("=" * 70)
    logger.info("Check Batch Job Status")
    logger.info("=" * 70)
    
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = load_config()
        
        # Get job ID
        if args.job_id:
            job_id = args.job_id
            logger.info(f"Using provided job ID: {job_id}")
        else:
            job_id_path = batch_dir / args.job_id_file
            
            if not job_id_path.exists():
                logger.error(f"Job ID file not found: {job_id_path}")
                logger.error("Please run 'python 2..CallOpenAI/scripts/submit_batch.py' first to create a batch job.")
                sys.exit(1)
            
            with open(job_id_path, "r") as f:
                job_id = f.read().strip()
            
            logger.info(f"Loaded job ID from file: {job_id}")
        
        # Initialize OpenAI client
        logger.info("Initializing OpenAI client...")
        client = OpenAI(api_key=config.openai_api_key)
        
        # Monitor batch job
        output_file_id = None
        if args.no_wait:
            logger.info("Checking current status (not waiting for completion)...")
            batch_job = client.batches.retrieve(job_id)
            status = batch_job.status
            completed = getattr(batch_job.request_counts, 'completed', 0)
            total = getattr(batch_job.request_counts, 'total', 0)
            
            logger.info(f"Current Status: {status}")
            logger.info(f"Completed: {completed}/{total}")
            
            if status == "completed":
                logger.info("Job is completed! Downloading results...")
                output_file_id = batch_job.output_file_id
            elif status in ["failed", "cancelled", "expired"]:
                logger.error(f"Job finished with status: {status}")
                sys.exit(1)
            else:
                logger.info(f"Job is still {status}. Run without --no-wait to monitor until completion.")
                sys.exit(0)
        else:
            logger.info(f"Monitoring batch job (polling every {args.poll_interval} seconds)...")
            job_status = monitor_batch_job(
                client, job_id, args.poll_interval, args.max_wait_time
            )
            
            if job_status["status"] != "completed":
                logger.error(f"Batch job did not complete successfully: {job_status['status']}")
                sys.exit(1)
            
            output_file_id = job_status.get("output_file_id")
        
        # Download results
        if not output_file_id:
            logger.error("No output file ID available. Cannot download results.")
            sys.exit(1)
        
        logger.info("Downloading batch results...")
        results = download_batch_results(client, output_file_id)
        logger.info(f"✓ Downloaded {len(results)} results")
        
        # Save raw results inside batchfiles directory
        output_path = batch_dir / args.output_file
        with open(output_path, "w") as f:
            for result in results:
                f.write(json.dumps(result) + "\n")
        
        logger.info(f"✓ Raw results saved to: {output_path}")
        
        # Load original URLs for enrichment
        urls_path = project_root / args.urls_file
        list_of_url = []
        if urls_path.exists():
            with open(urls_path, "r") as f:
                list_of_url = json.load(f)
            logger.info(f"Loaded {len(list_of_url)} URLs from original file for enrichment")
        else:
            logger.warning(f"URLs file not found: {urls_path}. Results will not be enriched with image URLs.")
        
        # Process and parse results
        logger.info("Processing and parsing results...")
        parsed_results = process_results(results, list_of_url, args.batch_size)
        
        # Save parsed results inside batchfiles directory
        parsed_output_path = batch_dir / args.parsed_output_file
        with open(parsed_output_path, "w") as f:
            json.dump(parsed_results, f, indent=2)
        
        logger.info("=" * 70)
        logger.info("✓ Batch processing completed successfully!")
        logger.info(f"✓ Raw results: {output_path}")
        logger.info(f"✓ Parsed results: {parsed_output_path}")
        logger.info(f"✓ Total parsed results: {len(parsed_results)}")
        logger.info("=" * 70)
        
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error checking batch job: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

