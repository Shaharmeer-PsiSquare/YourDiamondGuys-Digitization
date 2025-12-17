import logging
import sys
import json
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config, get_project_root
from src.batch_client import upload_batch_file, create_batch_job
from openai import OpenAI

# SETUP LOGGING
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def main():
    config = load_config()
    client = OpenAI(api_key=config.openai_api_key)
    project_root = get_project_root()
    batch_dir = project_root / "batchfiles"
    batch_dir.mkdir(exist_ok=True)
    
    # Load the manifest from batchfiles directory
    manifest_path = batch_dir / "batch_input_manifest.json"
    with open(manifest_path, 'r') as f:
        manifest = json.load(f)
        
    # JOBS TO RETRY
    target_indices = [7, 8, 9]
    new_job_data = {"jobs": []}

    print(f"Resubmitting Jobs: {target_indices}")

    for i in target_indices:
        filename = f"batch_input_{i}.jsonl"
        file_path = batch_dir / filename
        
        if not file_path.exists():
            print(f"Error: File {filename} not found!")
            continue

        print(f"\nProcessing Job {i}...")
        
        try:
            # 1. Upload File
            file_id = upload_batch_file(client, str(file_path))
            print(f"  Uploaded file: {file_id}")
            
            # 2. Create Batch 
            # FIX: create_batch_job returns the ID string directly, not an object
            batch_job_id = create_batch_job(client, file_id)
            
            print(f"  Created Batch: {batch_job_id}")
            
            new_job_data["jobs"].append({
                "job_index": i,
                "job_id": batch_job_id,  # Use the ID string directly
                "file_id": file_id,
                "status": "success"
            })
            
        except Exception as e:
            print(f"  Failed to submit Job {i}: {e}")

    # Save to new file inside batchfiles directory
    output_file = batch_dir / "batch_job_ids_remaining.json"
    with open(output_file, 'w') as f:
        json.dump(new_job_data, f, indent=2)
    
    print(f"\nSaved new job IDs to: {output_file}")
    print("Run the check script with this new file to monitor progress.")

if __name__ == "__main__":
    main()