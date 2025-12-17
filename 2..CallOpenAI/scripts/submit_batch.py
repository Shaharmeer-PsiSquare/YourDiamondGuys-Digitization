"""
Submit Batch Job to OpenAI

This script uploads a batch input file to OpenAI and creates a batch job.

Usage:
    python 2..CallOpenAI/scripts/submit_batch.py [--input-file batch_input.jsonl] [--job-id-file batch_job_id.txt]
"""

import logging
import sys
import argparse
import os
from pathlib import Path

# Add parent directory to path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config, get_project_root
from src.logger import setup_logging, suppress_third_party_logs, create_log_filename
from src.batch_client import upload_batch_file, create_batch_job
from openai import OpenAI

logger = logging.getLogger(__name__)


def main():
    """Submit batch job to OpenAI."""
    
    parser = argparse.ArgumentParser(description="Submit batch job to OpenAI")
    parser.add_argument(
        "--input-file",
        type=str,
        # default="batch_input.jsonl",
        default="batch_input_0.jsonl",
        help="Path to batch input JSONL file (default: batch_input.jsonl)"
    )
    parser.add_argument(
        "--job-id-file",
        type=str,
        default="batch_job_id.txt",
        help="Path to file where job ID will be saved (default: batch_job_id.txt)"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    project_root = get_project_root()
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / create_log_filename("submit_batch")
    setup_logging(level="INFO", log_file=str(log_file))
    suppress_third_party_logs()
    batch_dir = project_root / "batchfiles"
    batch_dir.mkdir(exist_ok=True)
    
    logger.info("=" * 70)
    logger.info("Submit Batch Job to OpenAI")
    logger.info("=" * 70)
    
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = load_config()
        
        # Check if input file exists (inside batchfiles directory)
        input_path = batch_dir / args.input_file
        
        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            logger.error("Please run 'python 2..CallOpenAI/scripts/create_batch.py' first to create the batch input file.")
            sys.exit(1)
        
        logger.info(f"Using batch input file: {input_path}")
        
        # Initialize OpenAI client
        logger.info("Initializing OpenAI client...")
        client = OpenAI(api_key=config.openai_api_key)
        
        # Upload file
        logger.info("Uploading batch input file to OpenAI...")
        file_id = upload_batch_file(client, str(input_path))
        logger.info(f"✓ File uploaded successfully. File ID: {file_id}")
        
        # Create batch job
        logger.info("Creating batch job...")
        job_id = create_batch_job(client, file_id)
        logger.info(f"✓ Batch job created successfully. Job ID: {job_id}")
        
        # Save job ID to file inside batchfiles directory
        job_id_path = batch_dir / args.job_id_file
        with open(job_id_path, "w") as f:
            f.write(job_id)
        
        logger.info("=" * 70)
        logger.info("✓ Batch job submitted successfully!")
        logger.info(f"✓ Job ID: {job_id}")
        logger.info(f"✓ Job ID saved to: {job_id_path}")
        logger.info("=" * 70)
        logger.info("")
        logger.info("Next step: Run 'python 2..CallOpenAI/scripts/check_batch.py' to monitor and download results")
        logger.info("")
        logger.info(f"Or check status manually with:")
        logger.info(f"  python 2..CallOpenAI/scripts/check_batch.py --job-id {job_id}")
        
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error submitting batch job: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

