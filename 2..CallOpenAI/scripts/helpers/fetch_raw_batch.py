import logging
import sys
import os
import json
from pathlib import Path
from openai import OpenAI

# Add parent directory to path to import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config, get_project_root

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def fetch_raw_output(job_id: str):
    """
    Retrieves the raw .jsonl content from an OpenAI Batch Job 
    and saves it to the local batchfiles directory.
    """
    try:
        # 1. Setup paths
        project_root = get_project_root()
        batch_dir = project_root / "batchfiles"
        batch_dir.mkdir(exist_ok=True)
        
        # Define output path
        output_path = batch_dir / f"raw_output_{job_id}.jsonl"

        # 2. Initialize Client
        config = load_config()
        client = OpenAI(api_key=config.openai_api_key)

        # 3. Retrieve Batch Info
        logger.info(f"Retrieving info for batch: {job_id}")
        batch_job = client.batches.retrieve(job_id)

        if batch_job.status != "completed":
            logger.error(f"Batch is not completed (Status: {batch_job.status}). Cannot download content.")
            return

        file_id = batch_job.output_file_id
        if not file_id:
            logger.error("No output file ID found for this batch.")
            return

        # 4. Download Content
        logger.info(f"Downloading raw content from file: {file_id}")
        content = client.files.content(file_id)
        
        # 5. Save to File
        # content.text contains the raw JSONL lines
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content.text)

        logger.info("=" * 60)
        logger.info(f"✓ Raw results saved successfully!")
        logger.info(f"✓ Location: {output_path}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    # Use the ID provided in your logs
    TARGET_JOB_ID = "batch_6943d7a051c481909a857719f5b94410"
    fetch_raw_output(TARGET_JOB_ID)