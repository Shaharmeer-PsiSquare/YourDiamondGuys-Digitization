import psycopg2
import os
import json
from pathlib import Path

from dotenv import load_dotenv


def _get_parent_env_path() -> Path:
    """
    Get the path to the parent .env file (Digitization root).
    This file is in 1.FetchFromDB/, so we go up 1 level.
    """
    return Path(__file__).parent.parent / ".env"


def _load_db_config() -> dict:
    """
    Load database configuration from parent .env file or environment.
    """
    env_path = _get_parent_env_path()
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


def _get_last_id_path() -> Path:
    """Path to the file storing the last processed Primary Key ID."""
    return Path(__file__).parent / "last_processed_id.txt"


def _get_lifetime_log_path() -> Path:
    """Path to the file storing ALL processed diamond IDs over time."""
    return Path(__file__).parent / "lifetime_processed_diamonds.txt"


def _read_last_id() -> int:
    """Read the last processed Primary Key ID. Returns 0 if not found."""
    path = _get_last_id_path()
    if not path.exists():
        return 0

    content = path.read_text(encoding="utf-8").strip()
    try:
        return int(content)
    except ValueError:
        return 0


def _write_last_id(pk_id: int) -> None:
    """Write the last processed Primary Key ID."""
    path = _get_last_id_path()
    path.write_text(str(pk_id), encoding="utf-8")


def _append_lifetime_diamonds(diamond_ids: list[str]) -> None:
    """Append a list of diamond IDs to the lifetime log file."""
    path = _get_lifetime_log_path()
    if not diamond_ids:
        return
    
    # Open in 'a' (append) mode. Create if not exists.
    try:
        with open(path, "a", encoding="utf-8") as f:
            for d_id in diamond_ids:
                f.write(f"{d_id}\n")
        print(f"Appended {len(diamond_ids)} IDs to {path.name}")
    except Exception as e:
        print(f"Warning: Failed to append to lifetime log: {e}")


# 1. Database Configuration
DB_CONFIG = _load_db_config()

# 2. The Output Filename for the current batch
output_filename = "1.FetchFromDB/diamond_records.json"

# Remove previous output file at the start of each run
try:
    if os.path.exists(output_filename):
        os.remove(output_filename)
        print(f"Deleted existing file: {os.path.abspath(output_filename)}")
except Exception as e:
    print(f"Warning: could not delete existing output file: {e}")

print("Connecting to database...")

try:
    # 4. Connect to Database
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # 3. Read the last Primary Key ID to resume pagination (fix for random diamond_id issue)
    last_id = _read_last_id()
    print(f"Resuming from Primary Key ID: {last_id}")

    # Query uses 'id' for stable sorting/pagination
    base_query = """
        SELECT id, diamond_id, certificate_link
        FROM "Affiliate_app_productinfo" app 
        WHERE digitization_id IS NULL 
          AND sell_status IS FALSE
          AND id > %s
        ORDER BY id ASC
        LIMIT 1000;
    """

    # 5. Execute Query
    print("Executing query...")
    cursor.execute(base_query, (last_id,))

    # 6. Fetch Data
    if cursor.description:
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        print(f"Fetched {len(rows)} rows. Processing...")

        try:
            pk_index = columns.index("id")
            d_id_index = columns.index("diamond_id")
            link_index = columns.index("certificate_link")
            
            output_data = []
            fetched_diamond_ids = []  # List to store IDs for the lifetime file
            max_pk_id = last_id 

            for row in rows:
                current_pk = row[pk_index]
                d_id = row[d_id_index]
                link = row[link_index]
                
                # Track max ID for pagination
                if current_pk > max_pk_id:
                    max_pk_id = current_pk
                
                if link:
                    # Add to JSON output list
                    output_data.append({
                        "diamond_id": d_id,
                        "certificate_link": link
                    })
                    # Add to lifetime tracking list
                    fetched_diamond_ids.append(str(d_id))

            # Write JSON output for this specific run
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2)
            print(f"Success! {len(output_data)} records saved to: {os.path.abspath(output_filename)}")

            # --- NEW: Append to lifetime file ---
            if fetched_diamond_ids:
                _append_lifetime_diamonds(fetched_diamond_ids)

            # Update pagination pointer with the highest ID seen
            if rows:
                _write_last_id(max_pk_id)
                print(f"Updated last_processed_id.txt to: {max_pk_id}")
            
        except ValueError as e:
            print(f"Error: Column missing in results. Available columns: {columns}")
            print(f"Details: {e}")

    else:
        print("Query executed but returned no results/description.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    print("Database connection closed.")