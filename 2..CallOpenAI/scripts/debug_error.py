import sys
import json
import os
from pathlib import Path
from openai import OpenAI

# Add parent directory to path to find src.config if needed
sys.path.insert(0, str(Path(__file__).parent.parent))

# Try to load config, or just use ENV if that fails
try:
    from src.config import load_config
    config = load_config()
    api_key = config.openai_api_key
except ImportError:
    api_key = os.environ.get("OPENAI_API_KEY")

if not api_key:
    print("❌ Error: Could not find API Key in config or environment.")
    sys.exit(1)

client = OpenAI(api_key=api_key)

def check_detailed_errors():
    # Allow passing ID via command line, default to the hardcoded one if missing
    if len(sys.argv) > 1:
        job_id = sys.argv[1]
    else:
        # Default ID (Replace this if you want to check a specific default)
        job_id = "batch_69428ecaf3dc8190978a40885b5c5d64"

    print(f"\n🔍 Inspecting Job ID: {job_id}")
    
    try:
        batch = client.batches.retrieve(job_id)

        # --- 1. SAVE METADATA TO DEBUG.TXT ---
        try:
            debug_file_path = Path(__file__).parent / "debug.txt"
            with open(debug_file_path, "w", encoding="utf-8") as f:
                f.write(batch.model_dump_json(indent=4))
            print(f"📄 Batch Metadata saved to: {debug_file_path}")
        except Exception as file_err:
            print(f"⚠️ Could not write debug.txt: {file_err}")

        print(f"Status: {batch.status}")
        print(f"Counts: {batch.request_counts}")

        # --- 2. NEW: DOWNLOAD SUCCESSFUL RESULTS ---
        if batch.output_file_id:
            print(f"\n🎁 Batch Complete! Found Output File: {batch.output_file_id}")
            print("   Downloading full results...")
            
            try:
                file_response = client.files.content(batch.output_file_id)
                output_content = file_response.content
                
                # Save to a separate JSONL file for clarity
                output_path = Path(__file__).parent / "batch_output.jsonl"
                with open(output_path, "wb") as f:
                    f.write(output_content)
                
                print(f"   ✅ FULL OUTPUT SAVED TO: {output_path}")
                
                # Optional: Print the first result as a preview
                print("\n   --- Preview of First Result ---")
                first_line = output_content.decode('utf-8').split('\n')[0]
                if first_line:
                    preview = json.loads(first_line)
                    # Try to show just the content content for readability
                    try:
                        print(f"   ID: {preview.get('custom_id')}")
                        print(f"   Content: {preview['response']['body']['choices'][0]['message']['content'][:100]}...")
                    except:
                        print(f"   {first_line[:100]}...")
                print("   -------------------------------\n")

            except Exception as e:
                print(f"   ❌ Failed to download output file: {e}")
        # -------------------------------------------

        # --- 3. CHECK FOR ERRORS (Batch Level) ---
        if hasattr(batch, 'errors') and batch.errors:
            print(f"\n🚨 BATCH LEVEL ERRORS (CRITICAL FAILURE):")
            error_list = batch.errors.data if hasattr(batch.errors, 'data') else batch.errors
            for error in error_list:
                code = getattr(error, 'code', error.get('code') if isinstance(error, dict) else 'N/A')
                message = getattr(error, 'message', error.get('message') if isinstance(error, dict) else 'N/A')
                print(f"  ❌ Code: {code} | Message: {message}")
            print("-" * 50)

        # --- 4. CHECK FOR ERRORS (Item Level) ---
        if batch.error_file_id:
            print(f"\n--- ⚠️ ITEM LEVEL ERRORS (From File: {batch.error_file_id}) ---")
            print("Downloading error details...\n")
            
            content = client.files.content(batch.error_file_id).content
            
            for i, line in enumerate(content.decode('utf-8').splitlines()):
                err_obj = json.loads(line)
                custom_id = err_obj.get('custom_id', 'N/A')
                response = err_obj.get('response', {})
                status_code = response.get('status_code', 'N/A')
                body = response.get('body', {})
                error_details = body.get('error', body)
                err_msg = error_details.get('message', 'No message provided')
                
                print(f"🆔 ID: {custom_id}")
                print(f"❌ Status: {status_code} | Message: {err_msg}")
                print("-" * 50)
                
                if i >= 4:
                    print("... (Stopping after 5 errors)")
                    break
        elif not batch.output_file_id:
            # Only print this if we didn't get an output file either
            print("ℹ️ No Item-Level Error File found (and no Output file yet).")

    except Exception as e:
        print(f"❌ Fatal Error: {e}")

if __name__ == "__main__":
    check_detailed_errors()