import sys
import time
from pathlib import Path
from datetime import datetime

# --- CONFIGURATION SETUP ---
# Add parent directory to path to find src.config, just like check_queue.py
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from src.config import load_config
    from openai import OpenAI
except ImportError as e:
    print("❌ Error importing project modules. Make sure you are running this from the correct directory.")
    print(f"Details: {e}")
    sys.exit(1)

def find_and_kill_zombies():
    print("Loading config...")
    config = load_config()
    
    # Initialize client with the loaded key
    client = OpenAI(api_key=config.openai_api_key)

    print("\n🔍 STARTING DEEP SEARCH: Scanning full batch history for STUCK/ACTIVE batches...")
    print("Note: This will page through older history that check_queue.py might miss.")
    print("-" * 90)
    print(f"{'BATCH ID':<35} | {'STATUS':<12} | {'CREATED (UTC)':<20} | {'PROGRESS'}")
    print("-" * 90)
    
    # Pagination Setup
    has_more = True
    last_id = None
    total_scanned = 0
    active_batches = []

    try:
        while has_more:
            # Fetch pages of 100 to go deep into history
            # 'after' cursor is used to get the next page
            if last_id:
                page = client.batches.list(limit=100, after=last_id)
            else:
                page = client.batches.list(limit=100)
            
            # Check if page is empty (end of history)
            if not page.data:
                break

            for batch in page.data:
                total_scanned += 1
                
                # Check for "Quota Consuming" states
                is_active = batch.status in ['validating', 'in_progress', 'finalizing']
                
                if is_active:
                    active_batches.append(batch)
                    
                    # Formatting
                    created_at = datetime.fromtimestamp(batch.created_at).strftime('%Y-%m-%d %H:%M:%S')
                    counts = batch.request_counts
                    progress = "N/A"
                    if counts:
                        progress = f"{counts.completed} / {counts.total} ({counts.failed} failed)"
                    
                    print(f"🚨 {batch.id:<33} | {batch.status:<12} | {created_at:<20} | {progress}")

            if page.has_more:
                last_id = page.data[-1].id
                # Small sleep to be nice to the API rate limits
                time.sleep(0.1)
            else:
                has_more = False
                
    except Exception as e:
        print(f"\n❌ API Error during scan: {e}")
        return

    print("-" * 90)
    print(f"\n✅ Deep Scan Complete. Checked {total_scanned} historic batches.")

    if not active_batches:
        print("🤷 No active batches found in your entire history.")
        print("👉 Diagnosis: If you are still blocked, another user in your Org is running batches.")
    else:
        print(f"\n🔥 Found {len(active_batches)} ACTIVE batch(es) consuming your quota.")
        print("These are likely 'Zombies' - old jobs that got stuck or are still running slowly.")
        
        confirm = input("\n⚠️  Do you want to CANCEL all these active batches to free up your 2M token limit? (y/n): ")
        
        if confirm.lower() == 'y':
            for batch in active_batches:
                print(f"💥 Cancelling {batch.id}...")
                try:
                    client.batches.cancel(batch.id)
                    print("   ✅ Cancelled.")
                except Exception as e:
                    print(f"   ❌ Error cancelling: {e}")
            
            print("\n⏳ Please wait 2-5 minutes for OpenAI to release the token quota.")
        else:
            print("❌ Operation cancelled. Quota remains locked.")

if __name__ == "__main__":
    find_and_kill_zombies()