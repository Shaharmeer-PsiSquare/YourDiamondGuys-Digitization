import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path to find src.config
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config
from openai import OpenAI

def list_active_batches():
    print("Loading config...")
    config = load_config()
    client = OpenAI(api_key=config.openai_api_key)

    print("\n--- FETCHING RECENT BATCHES ---")
    # Fetch the last 20 batches
    batches = client.batches.list(limit=100)
    
    active_count = 0
    
    print(f"{'BATCH ID':<35} | {'STATUS':<12} | {'CREATED (UTC)':<20} | {'PROGRESS'}")
    print("-" * 90)

    for batch in batches:
        # specific formatting for readability
        created_at = datetime.fromtimestamp(batch.created_at).strftime('%Y-%m-%d %H:%M:%S')
        
        # Check if it's "active" (clogging the queue)
        is_active = batch.status in ['validating', 'in_progress', 'finalizing']
        status_symbol = "🔄" if is_active else "•"
        
        if is_active:
            active_count += 1
            
        counts = batch.request_counts
        progress = "N/A"
        if counts:
             progress = f"{counts.completed} / {counts.total} ({counts.failed} failed)"

        print(f"{status_symbol} {batch.id:<33} | {batch.status:<12} | {created_at:<20} | {progress}")

    print("-" * 90)
    print(f"Total Active Batches: {active_count}")
    
    # Optional: Suggest cancellation
    if active_count > 0:
        print("\nTo fix 'token_limit_exceeded', you likely need to CANCEL these active batches.")
        print("Run this python command to cancel a specific batch:")
        print("client.batches.cancel('batch_id_here')")

if __name__ == "__main__":
    list_active_batches()