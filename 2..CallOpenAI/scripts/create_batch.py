"""
Create Batch Input File

This script creates a JSONL input file for OpenAI batch processing.
By default it reads from the shared `1.FetchFromDB/diamond_records.json`
file produced by the fetch step.

That file is a JSON list of objects:
    { "diamond_id": "...", "certificate_link": "https://..." }

We extract `certificate_link` values as the image URLs for batch requests.

Usage:
    python 2..CallOpenAI/scripts/create_batch.py [--input-file ../1.FetchFromDB/diamond_records.json] [--output-file batch_input.jsonl] [--batch-size 5]
"""

import json
import logging
import sys
import argparse
from pathlib import Path
import os
import time
from datetime import datetime

# Add parent directory to path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config, get_project_root
from src.logger import setup_logging, suppress_third_party_logs, create_log_filename
from src.prompt_loader import load_prompt
from src.batch_client import create_batch_requests, create_batch_input_file

logger = logging.getLogger(__name__)


def main():
    """Create batch input file from URLs."""

    parser = argparse.ArgumentParser(description="Create batch input file for OpenAI batch processing")
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
        "--output-file",
        type=str,
        default="batch_input.jsonl",
        help="Path to output JSONL file (default: batch_input.jsonl)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Number of URLs per request (max: 5, default: 5)"
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
    log_file = log_dir / create_log_filename("create_batch")
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
    logger.info("Create Batch Input File")
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
        
        # Load prompt
        logger.info("Loading prompt...")
        prompt = load_prompt(args.prompt_file)
        logger.debug(f"Prompt loaded: {len(prompt)} characters")
        
        # Enforce max batch size of 5
        effective_batch_size = min(max(args.batch_size, 1), 5)
        if effective_batch_size != args.batch_size:
            logger.warning(f"Batch size {args.batch_size} is out of allowed range; using {effective_batch_size} instead.")

        # Create batch requests
        logger.info(f"Creating batch requests (grouping by {effective_batch_size} URLs per request)...")
        batch_requests = create_batch_requests(
            list_of_url=list_of_url,
            prompt=prompt,
            config=config,
            few_shot_examples=None,
            markdown="",
            urls_per_request=effective_batch_size,
            job_prefix="",  # Single file, no per-job prefix needed
        )
        
        logger.info(f"Created {len(batch_requests)} batch requests")
        
        # Create output file path inside batchfiles directory
        output_path = batch_dir / args.output_file
        
        # Create batch input file
        logger.info(f"Creating batch input file: {output_path}")
        create_batch_input_file(batch_requests, str(output_path))
        
        logger.info("=" * 70)
        logger.info("✓ Batch input file created successfully!")
        logger.info(f"✓ File: {output_path}")
        logger.info(f"✓ Total requests: {len(batch_requests)}")
        logger.info(f"✓ URLs per request: {args.batch_size}")
        logger.info("=" * 70)
        logger.info("")
        logger.info("Next step: Run 'python 2..CallOpenAI/scripts/submit_batch.py --input-file batch_input.jsonl'")
        elapsed = time.time() - start_time
        logger.info(f"Total time to create batch file: {elapsed:.2f} seconds")
        logger.info(f"Execution completed at: {datetime.now().isoformat(timespec='seconds')}")
        
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error creating batch file: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

