import os
import json
import logging
import traceback

from utility.db_operations import (
    get_db_connection,
    insert_digitisation_data,
    insert_digisation_score_data,
    update_product_info,
    update_ProductCharacteristics,
    ai_time_update,
)



# 2. Import Logic
try:
    from utility.extract_pdf_data import (
        fetch_data,
        fetch_data_round,
        data as config_data,
    )

    reject_ls_fluorescence = ["yellow", "Strong Blue", "Very Strong Blue"]
    reject_ls_symbol = ["Cavity", "Etched Channel", "Knot", "Laser Drill"]

    print("✅ Successfully imported dependencies")

except ImportError as e:
    # ... (Same fallbacks as before) ...
    print(f"⚠️ Warning: Using default lists/mocks. Import Error: {e}")

# 3. Logging Setup
LOG_FILE_PATH = os.path.join(os.path.dirname(__file__), "run_scoring_results.log")

# Configure logging to both console and file in append mode so it works
# even while the file is open in an editor.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE_PATH, mode="a", encoding="utf-8"),
    ],
)

logger = logging.getLogger(__name__)


def log_line(line: str) -> None:
    """
    Helper to log a single formatted line to both stdout and the log file.
    """
    print(line)
    logger.info(line)


# 4. Load Data
# Read results produced by the OpenAI processing step
data_file_path = os.path.join(os.path.dirname(__file__), "OpenAIresults.json")
if not os.path.exists(data_file_path):
    msg = f"⚠️ JSON file not found at {data_file_path}."
    print(msg)
    logger.warning(msg)
    openai_response = []
else:
    logger.info("Loading OpenAI results from %s", data_file_path)
    with open(data_file_path, "r", encoding="utf-8") as f:
        openai_response = json.load(f)

    logger.info("Loaded %d result objects from OpenAIresults.json", len(openai_response))

    # 4.a Attach diamond_id to each result using diamond_records.json as reference
    try:
        project_root = os.path.dirname(os.path.dirname(__file__))  # Digitization root
        records_path = os.path.join(project_root, "1.FetchFromDB", "diamond_records.json")
        logger.info("Attempting to enrich results with diamond_id from %s", records_path)

        if os.path.exists(records_path):
            with open(records_path, "r", encoding="utf-8") as rf:
                diamond_records = json.load(rf)

            logger.info("Loaded %d diamond_records entries", len(diamond_records))

            # Build lookup from certificate_link -> diamond_id
            link_to_id = {
                rec["certificate_link"]: rec["diamond_id"]
                for rec in diamond_records
                if isinstance(rec, dict)
                and rec.get("certificate_link")
                and rec.get("diamond_id")
            }
            logger.info("Built lookup for %d certificate_link entries", len(link_to_id))

            updated_count = 0
            for item in openai_response:
                if not isinstance(item, dict):
                    continue

                # Only set if missing / empty
                if item.get("diamond_id"):
                    continue

                url = item.get("image_url") or item.get("certificate_link")
                if url and url in link_to_id:
                    item["diamond_id"] = link_to_id[url]
                    updated_count += 1

            logger.info(
                "Attached diamond_id to %d results using diamond_records.json", updated_count
            )
            print(f"✅ Attached diamond_id to {updated_count} results using diamond_records.json")
        else:
            msg = f"⚠️ diamond_records.json not found at: {records_path}"
            print(msg)
            logger.warning(msg)
    except Exception:
        msg = "⚠️ Failed to attach diamond_id from diamond_records.json"
        print(msg)
        logger.exception("Failed enriching results with diamond_id")



# 5. Run Scoring Loop
separator = "-" * 160
log_line(separator)
log_line(
    f"{'SHAPE':<35} | {'SCORE':<8} | {'REJ FLU?':<10} | "
    f"{'REJ SYM?':<10} | {'COLOR':<6} | {'CULET':<10} | {'NOTES'}"
)
log_line(separator)

# Create a single DB connection for this scoring run
conn = None
try:
    logger.info("Attempting to connect to database for scoring run...")
    conn = get_db_connection()
    logger.info("Database connection established successfully.")
except Exception as e:
    msg = f"⚠️ Could not connect to database: {e}"
    print(msg)
    logger.exception("Database connection failed")

for idx, diamond in enumerate(openai_response, start=1):
    try:
        # --- A. PRE-PROCESSING (FIXED) ---

        # 1. Safe Shape Access & Normalization
        shape_raw = str(diamond.get("shape") or "ND").strip()
        diamond_id_for_log = diamond.get("diamond_id") or "N/A"
        logger.debug("Processing diamond %d (diamond_id=%s, raw shape=%s)", idx, diamond_id_for_log, shape_raw)

        # 2. Map Unknown Shapes (Fix legacy naming issues)
        shape_upper = shape_raw.upper()

        if "SQUARE EMERALD" in shape_upper:
            diamond["shape"] = "Square Emerald"
        elif "OCTAGONAL MODIFIED" in shape_upper:
            diamond["shape"] = "Radiant"  # Standard mapping
        elif "CUT CORNERED SQUARE" in shape_upper or "CUT-CORNERED SQUARE" in shape_upper:
            diamond["shape"] = "Radiant"  # Maps to Radiant Square logic
        elif "CUT CORNERED RECTANGULAR" in shape_upper:
            diamond["shape"] = "Radiant"  # Maps to Radiant Rect logic
        else:
            diamond["shape"] = shape_raw

        # 3. Normalize Key to Symbols
        key_symbols_raw = diamond.get("key_to_symbols") or diamond.get("key_to_symbol")
        if key_symbols_raw is None:
            value = ""
        elif isinstance(key_symbols_raw, list):
            value = ",".join(key_symbols_raw)
        else:
            value = str(key_symbols_raw)
        diamond["key_to_symbol"] = value

        # 4. Cleanup Culet
        culet_raw = str(diamond.get("culet")).lower()
        if culet_raw in ["false", "none", "null", "none"]:
            diamond["culet"] = "None"

        # 5. Typo Correction
        if "flouroscence" in diamond:
            diamond["fluorescence"] = diamond.pop("flouroscence")

        # --- B. SCORING ENGINE ---

        is_round = "round" in str(diamond.get("shape", "")).lower()
        reprocess_data = None

        if is_round:
            final_data, result_dc, updated_pdf_dic = fetch_data_round(
                config_data, diamond, reprocess_data, None
            )
        else:
            final_data, result_dc, updated_pdf_dic = fetch_data(
                config_data, diamond, reprocess_data, None
            )

        logger.debug(
            "Scoring complete for diamond_id=%s: digisation_score=%s",
            diamond.get("diamond_id"),
            final_data.get("digisation_score"),
        )

        # --- C. POST-PROCESSING ---

        # 1. Fluorescence Rejection
        fluorescence_rejection = False
        if "fluorescence" in result_dc and result_dc["fluorescence"]:
            try:
                fl_val = str(result_dc["fluorescence"]).lower()
                if "not" not in fl_val and fl_val in [
                    x.lower() for x in reject_ls_fluorescence
                ]:
                    fluorescence_rejection = True
            except Exception:
                pass
        result_dc["fluorescence_rejection"] = fluorescence_rejection

        # 2. Key to Symbol Rejection
        symbol_rejection = False
        sym_keys_to_check = [
            k for k in ["key_to_symbol", "key_symbols"] if k in result_dc and result_dc[k]
        ]

        for key in sym_keys_to_check:
            sym_val = str(result_dc[key])
            if sym_val and "not" not in sym_val.lower() and sym_val.lower() != "none":
                data_ls = sym_val.split(",")
                if any(
                    k.strip().lower() in [x.lower() for x in reject_ls_symbol]
                    for k in data_ls
                ):
                    symbol_rejection = True
                    break
        result_dc["key_to_symbol_rejection"] = symbol_rejection

        # 3. Renaming Color
        if "color_grade" in result_dc:
            result_dc["color"] = result_dc.pop("color_grade")

        # 4. Similar Data Access (currently unused placeholder)
        similar_data = []
        color = result_dc.get("color")

        # --- D. LOG OUTPUT (unchanged) ---
        score = final_data.get("digisation_score") or "N/A"
        shape_name = result_dc.get("shape") or "Unknown"
        color_final = result_dc.get("color") or "N/A"
        culet_final = result_dc.get("culet") or "N/A"

        syms_print = result_dc.get("key_to_symbol")
        if not syms_print:
            syms_print = "None"
        if len(syms_print) > 30:
            syms_print = syms_print[:30] + "..."

        line = (
            f"{shape_name:<35} | {score:<8} | {str(fluorescence_rejection):<10} | "
            f"{str(symbol_rejection):<10} | {color_final:<6} | {culet_final:<10} | "
            f"Syms: {syms_print}"
        )
        log_line(line)

        # --- E. DATABASE UPDATES (new flow mirroring legacy Celery task) ---

        if conn is not None:
            # Prefer explicit diamond_id from the payload; otherwise try to derive from URL
            diamond_id = diamond.get("diamond_id")
            if not diamond_id:
                img_url = diamond.get("image_url") or diamond.get("source_link")
                if img_url and isinstance(img_url, str):
                    # Legacy pattern: /diamond/<id> or sku-<id>?
                    import re

                    m = re.search(r"diamond/(\d+)", img_url)
                    if m:
                        diamond_id = m.group(1)
                    else:
                        m = re.search(r"sku-(\d+)\?", img_url)
                        if m:
                            diamond_id = m.group(1)

            if diamond_id:
                try:
                    logger.debug(
                        "Persisting results to DB for diamond_id=%s (score=%s)",
                        diamond_id,
                        final_data.get("digisation_score"),
                    )
                    # 1) Upsert digitisation data (use result_dc as the normalized digitized_data)
                    digi_id = insert_digitisation_data(conn, result_dc, str(diamond_id))

                    # 2) Upsert score data (final_data corresponds to digitized_score_data)
                    score_id = insert_digisation_score_data(
                        conn, final_data, str(diamond_id)
                    )

                    # 3) One-time AI timestamp
                    ai_time_update(conn, str(diamond_id))

                    # 4) Update product characteristics with scores
                    update_ProductCharacteristics(
                        conn,
                        final_data.get("digisation_score"),
                        str(diamond_id),
                        final_data.get("key_to_symbol_score"),
                    )

                    # 5) Mark product info as digitized and attach FKs
                    update_product_info(conn, str(diamond_id), digi_id, score_id)
                    conn.commit()
                except Exception as db_exc:
                    # Log but continue processing other diamonds
                    logger.exception("DB error for diamond_id=%s", diamond_id)
                    log_line(f"❌ DB error for diamond_id={diamond_id}: {db_exc}")
                    conn.rollback()
                    
    except Exception as e:
        # Use .get with a default to avoid 'NoneType' error during the error logging itself!
        s_name = diamond.get("shape") if diamond.get("shape") else "Unknown/None"
        tb = traceback.format_exc()
        logger.exception("Error processing diamond with shape=%s", s_name)
        log_line(f"❌ Error processing {s_name}: {e}")
        log_line(tb)

if conn is not None:
    try:
        logger.info("Closing database connection.")
        conn.close()
    except Exception:
        logger.exception("Error while closing database connection")