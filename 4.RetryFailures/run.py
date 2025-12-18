import json
import os
from pathlib import Path
from typing import Dict, Any, List, Tuple
import subprocess
import sys

import psycopg2
from dotenv import load_dotenv

# --- DATABASE UTILITIES ---

def _load_db_config() -> dict:
    """Load database configuration from .env file or environment."""
    # Project root is the parent of this '4.RetryFailures' folder
    project_root = Path(__file__).resolve().parent.parent
    env_path = project_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()

    return {
        "dbname": os.getenv("DB_NAME", "postgres"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", ""),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
    }

def fetch_diamond_details(diamond_ids: List[str]) -> List[Dict[str, Any]]:
    """Fetch diamond_id and certificate_link from DB for the missing IDs."""
    if not diamond_ids:
        return []

    db_config = _load_db_config()
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        query = """
            SELECT diamond_id, certificate_link
            FROM "Affiliate_app_productinfo"
            WHERE diamond_id = ANY(%s);
        """
        cur.execute(query, (diamond_ids,))
        rows = cur.fetchall()
        return [{"diamond_id": str(row[0]), "certificate_link": row[1]} for row in rows]
    except Exception as e:
        print(f"❌ Database error: {e}")
        return []
    finally:
        if conn is not None:
            conn.close()

# --- JSON UTILITIES ---

def load_json(path: Path) -> Any:
    """Safely load JSON from a path with basic error handling."""
    try:
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError as e:
        print(f"❌ {e}")
        raise
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse JSON from {path}: {e}")
        raise
    except Exception as e:
        print(f"❌ Unexpected error reading {path}: {e}")
        raise

def build_diamond_records_index(records: List[Dict[str, Any]]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Build lookup dictionaries: url_to_id and id_to_url."""
    url_to_id: Dict[str, str] = {}
    id_to_url: Dict[str, str] = {}
    for rec in records:
        if isinstance(rec, dict):
            d_id = rec.get("diamond_id")
            url = rec.get("certificate_link")
            if d_id and url:
                url_to_id[str(url)] = str(d_id)
                id_to_url[str(d_id)] = str(url)
    return url_to_id, id_to_url

# --- MAIN LOGIC ---

def run_integrity_and_recovery() -> List[str]:
    # 1. Setup Paths (relative to Digitization root)
    project_root = Path(__file__).resolve().parent.parent
    diamond_records_path = project_root / "1.FetchFromDB" / "diamond_records.json"
    insert_to_db_path = project_root / "3.ScoringAndDBOps" / "InsertToDb.json"
    # Write the retry/missing file in this folder (4.RetryFailures), not the parent
    missing_output_path = Path(__file__).resolve().parent / "failed_diamonds.json"

    # Ensure we start with a clean file each run
    try:
        if missing_output_path.exists():
            missing_output_path.unlink()
            print(f"Deleted existing {missing_output_path}")
    except Exception as e:
        print(f"⚠ Warning: could not delete existing {missing_output_path}: {e}")

    print(f"--- Step 1: Integrity Check ---")
    print(f"Ground Truth: {diamond_records_path.name}")
    print(f"Test File:    {insert_to_db_path.name}")

    # 2. Load Data with error handling
    try:
        diamond_records = load_json(diamond_records_path)
        insert_records = load_json(insert_to_db_path)
    except Exception:
        print("❌ Aborting: failed to load one or more required JSON files.")
        return []
    
    url_to_id, id_to_url = build_diamond_records_index(diamond_records)
    truth_ids = set(id_to_url.keys())

    # 3. Analyze Consistency
    ok, missing_id_count, id_mismatch, url_mismatch = 0, 0, 0, 0
    mismatches = []

    for idx, rec in enumerate(insert_records):
        d_id = rec.get("diamond_id")
        img = rec.get("image_url") or rec.get("certificate_link")

        if not d_id:
            missing_id_count += 1
            continue

        d_id_str = str(d_id)
        img_str = str(img) if img else None

        if d_id_str and img_str:
            truth_id = url_to_id.get(img_str)
            truth_url = id_to_url.get(d_id_str)

            is_dirty = False
            if truth_id and truth_id != d_id_str:
                id_mismatch += 1
                is_dirty = True
            if truth_url and truth_url != img_str:
                url_mismatch += 1
                is_dirty = True
            
            if not is_dirty:
                ok += 1
            elif len(mismatches) < 10:
                mismatches.append(f"Index {idx}: ID {d_id_str} does not match expected URL data.")

    # 4. Print Summary
    print(f"Processed: {len(insert_records)} | Consistent: {ok} | Mismatches: {id_mismatch + url_mismatch}")
    if mismatches:
        for m in mismatches: print(f" - {m}")

    # 5. Identify Missing
    insert_ids = {str(r.get("diamond_id")) for r in insert_records if r.get("diamond_id")}
    missing_ids = sorted(list(truth_ids - insert_ids))
    
    print(f"\n--- Step 2: Recovery from diamond_records.json (no DB) ---")
    print(f"Missing Diamonds detected: {len(missing_ids)}")

    if missing_ids:
        print("Populating missing records from diamond_records.json...")
        # We already have id_to_url from build_diamond_records_index, so we can
        # reconstruct the same structure as diamond_records.json without hitting DB.
        enriched_missing_records: List[Dict[str, Any]] = []
        for d_id in missing_ids:
            url = id_to_url.get(d_id)
            if url:
                enriched_missing_records.append(
                    {
                        "diamond_id": d_id,
                        "certificate_link": url,
                    }
                )

        with missing_output_path.open("w", encoding="utf-8") as f:
            json.dump(enriched_missing_records, f, indent=2)

        print(
            f"✅ Success! Saved {len(enriched_missing_records)} enriched records "
            f"to {missing_output_path.name} using diamond_records.json"
        )
    else:
        print("✅ No missing diamonds found. Nothing to fetch.")

    # Return the list of missing IDs so caller can decide what to do next
    return missing_ids


def run_downstream_steps(project_root: Path) -> None:
    """
    After building failed_diamonds.json, run the downstream pipeline in order:
      1. python 2..CallOpenAI/scripts/create_batch_concurrent.py --input-file 4.RetryFailures/failed_diamonds.json
      2. python 2..CallOpenAI/scripts/submit_batch_concurrent.py
      3. python 2..CallOpenAI/scripts/check_batch_concurrent.py
      4. python 3.ScoringAndDBOps/run.py
    """
    commands = [
        # Note: create_batch_concurrent resolves paths relative to its own
        # project root (the 2..CallOpenAI folder), so we pass a path that is
        # correct from there: "../4.RetryFailures/failed_diamonds.json".
        ["python", "2..CallOpenAI/scripts/create_batch_concurrent.py", "--input-file", "../4.RetryFailures/failed_diamonds.json"],
        ["python", "2..CallOpenAI/scripts/submit_batch_concurrent.py"],
        ["python", "2..CallOpenAI/scripts/check_batch_concurrent.py"],
        ["python", "3.ScoringAndDBOps/run.py"],
    ]

    for idx, cmd in enumerate(commands, start=1):
        print(f"\n▶ Running step {idx}: {' '.join(cmd)}")
        try:
            result = subprocess.run(
                cmd,
                cwd=str(project_root),
                check=False,
            )
        except Exception as e:
            print(f"❌ Failed to start command: {' '.join(cmd)}")
            print(f"   Error: {e}")
            sys.exit(1)

        if result.returncode != 0:
            print(f"❌ Command failed with exit code {result.returncode}: {' '.join(cmd)}")
            sys.exit(result.returncode)
        else:
            print(f"✅ Step {idx} completed successfully.")


def write_final_failures(project_root: Path) -> None:
    """
    After the second pass (retry pipeline), re-check for any diamonds that are still
    missing from InsertToDb.json and write their IDs to final_failure.json.
    """
    diamond_records_path = project_root / "1.FetchFromDB" / "diamond_records.json"
    insert_to_db_path = project_root / "3.ScoringAndDBOps" / "InsertToDb.json"
    final_failure_path = project_root / "4.RetryFailures" / "final_failure.json"

    try:
        diamond_records = load_json(diamond_records_path)
        insert_records = load_json(insert_to_db_path)
    except Exception:
        print("⚠ Skipping final failure check: could not load required JSON files.")
        return

    _, id_to_url = build_diamond_records_index(diamond_records)
    truth_ids = set(id_to_url.keys())
    insert_ids = {str(r.get("diamond_id")) for r in insert_records if r.get("diamond_id")}
    still_missing = sorted(list(truth_ids - insert_ids))

    try:
        # If file exists, load existing failures and merge; otherwise start fresh.
        existing: List[str] = []
        if final_failure_path.exists():
            try:
                existing_data = load_json(final_failure_path)
                if isinstance(existing_data, list):
                    existing = [str(x) for x in existing_data]
            except Exception:
                # If the existing file is corrupt, we just overwrite with the new set.
                existing = []

        combined = sorted(set(existing).union(still_missing), key=str)

        with final_failure_path.open("w", encoding="utf-8") as f:
            json.dump(combined, f, indent=2)
        print(
            f"✅ Final failure check complete. Remaining missing diamonds: "
            f"{len(still_missing)} (total unique in file: {len(combined)}) "
            f"(written/appended to {final_failure_path})"
        )
    except Exception as e:
        print(f"⚠ Failed to write final_failure.json: {e}")


if __name__ == "__main__":
    # Run integrity + recovery for failed diamonds
    missing_ids = run_integrity_and_recovery()
    print("✅ Integrity and recovery step complete.")

    # If nothing is missing, we can safely stop here
    if not missing_ids:
        print("✅ No missing diamonds detected; skipping downstream retry pipeline.")
        sys.exit(0)

    # Otherwise, run the downstream OpenAI + scoring pipeline
    project_root = Path(__file__).resolve().parent.parent
    run_downstream_steps(project_root)

    # After the retry pipeline, perform a final failure check
    write_final_failures(project_root)
