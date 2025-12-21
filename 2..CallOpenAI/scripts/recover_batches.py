import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config, get_project_root
from src.logger import setup_logging
from openai import OpenAI

logger = logging.getLogger(__name__)

def main():
    """Recover lost Batch IDs from OpenAI API."""
    
    # Setup basics
    project_root = get_project_root()
    batch_dir = project_root / "batchfiles"
    setup_logging(level="INFO")
    
    config = load_config()
    client = OpenAI(api_key=config.openai_api_key)

    logger.info("=" * 70)
    logger.info("SCANNING OPENAI FOR RECENT BATCHES")
    logger.info("=" * 70)

    # 1. Fetch recent batches (last 50)
    # We fetch a bit more than needed to ensure we catch them all
    recent_batches = client.batches.list(limit=50)
    
    recovered_jobs = []
    
    # Time window: Only look for batches created in the last 24 hours
    cutoff_time = datetime.now().timestamp() - (24 * 3600) 

    logger.info("Analyzing recent batches...")

    for batch in recent_batches:
        # Convert CreatedAt (unix timestamp) to check recency
        if batch.created_at < cutoff_time:
            continue
            
        # We need to find out WHICH file this batch belongs to.
        # We do this by asking OpenAI for the filename of the input_file_id
        try:
            file_obj = client.files.retrieve(batch.input_file_id)
            filename = file_obj.filename
            
            # Simple filter: Only include files that match your naming convention
            # (Assumes your files are named like 'batch_input_0.jsonl' etc)
            if "batch_input" in filename and filename.endswith(".jsonl"):
                
                # Try to extract the job index from the filename
                # Example: batch_input_3.jsonl -> index 3
                try:
                    # Remove extension
                    name_part = filename.rsplit('.', 1)[0]
                    # Get the number after the last underscore
                    job_index = int(name_part.split('_')[-1])
                except ValueError:
                    job_index = -1 # Could not parse index

                logger.info(f"✓ FOUND: Job {job_index} | ID: {batch.id} | Status: {batch.status} | File: {filename}")
                
                recovered_jobs.append({
                    "job_index": job_index,
                    "job_id": batch.id,
                    "file_id": batch.input_file_id,
                    "input_file": filename,
                    "status": batch.status
                })
        except Exception as e:
            logger.warning(f"Could not retrieve details for batch {batch.id}: {e}")

    # Sort them by index so they look nice
    recovered_jobs.sort(key=lambda x: x["job_index"])

    if not recovered_jobs:
        logger.error("No recent matching batches found!")
        return

    logger.info("-" * 70)
    logger.info(f"Successfully identified {len(recovered_jobs)} batches from OpenAI history.")
    
    # 2. Re-create the missing JSON file
    job_ids_path = batch_dir / "batch_job_ids.json"
    
    # Interactive check
    confirm = input(f"\nSave these {len(recovered_jobs)} IDs to {job_ids_path}? (y/n): ")
    if confirm.lower() != 'y':
        logger.info("Operation cancelled.")
        return

    with open(job_ids_path, "w") as f:
        json.dump({"jobs": recovered_jobs}, f, indent=2)

    logger.info("=" * 70)
    logger.info("✓ RECOVERY COMPLETE")
    logger.info(f"File saved: {job_ids_path}")
    logger.info("You can now run your 'check_batch_concurrent.py' script.")
    logger.info("=" * 70)

if __name__ == "__main__":
    main()