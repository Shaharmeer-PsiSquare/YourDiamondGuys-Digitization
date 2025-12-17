import os
import logging
import psycopg2
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pytz
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def _get_parent_env_path() -> Path:
    """
    Get the path to the parent .env file (Digitization root).
    This file is in 3.Scoring/utility/, so we go up 2 levels.
    """
    return Path(__file__).parent.parent.parent / ".env"


def _load_db_config() -> dict:
    """
    Load database configuration from parent .env file.
    """
    env_path = _get_parent_env_path()
    if env_path.exists():
        logger.debug("Loading DB configuration from %s", env_path)
        load_dotenv(env_path)
    else:
        # Fallback: try loading from current directory or environment
        logger.debug("Parent .env not found, falling back to default environment")
        load_dotenv()

    config = {
        "dbname": os.getenv("DB_NAME", "postgres"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", ""),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
    }
    logger.info(
        "DB config loaded (host=%s, dbname=%s, user=%s, port=%s)",
        config["host"],
        config["dbname"],
        config["user"],
        config["port"],
    )
    return config


def get_db_connection():
    """
    Create a new PostgreSQL connection using configuration from parent .env file.
    """
    db_config = _load_db_config()
    try:
        logger.info(
            "Opening PostgreSQL connection to %s@%s/%s",
            db_config["user"],
            db_config["host"],
            db_config["dbname"],
        )
        conn = psycopg2.connect(**db_config)
        logger.info("PostgreSQL connection established successfully")
        return conn
    except Exception:
        logger.exception("Failed to establish PostgreSQL connection")
        raise


def insert_digitisation_data(connection, data: Dict[str, Any], diamond_id: str) -> int:
    """
    Insert or update the main digitization record for a diamond.

    This mirrors the legacy behaviour from legacy_context.tasks.insert_digitisation_data.
    """
    if data.get("culet") == "none":
        data["culet"] = "None"

    # Base columns common to all shapes
    columns = [
        "diamond_id",
        "culet",
        "depth",
        "flouroscence",
        "key_to_symbol",
        "measurement",
        "table_size",
        "girdle",
        "clarity",
        "color",
        "shape",
        "symmetry",
        "polish",
        "carat",
        "reprocess_status",
        "value_check",
    ]

    is_round = data.get("shape") and "round" in str(data["shape"]).lower()
    if is_round:
        columns.extend(
            [
                "crown_angle",
                "crown_height",
                "pavilion_height",
                "pavilion_angle",
                "cut",
                "lower_half_length",
                "star_length",
            ]
        )

    values = []
    for col in columns:
        if col == "diamond_id":
            values.append(diamond_id)
        elif col == "reprocess_status":
            values.append("pending")
        elif col == "value_check":
            values.append("DF")
        elif col == "flouroscence":
            # Map correct data key to misspelled DB column
            values.append(data.get("fluorescence"))
        else:
            values.append(data.get(col))

    cols_str = ", ".join([f'"{c}"' for c in columns])
    placeholders = ", ".join(["%s"] * len(values))

    update_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns if col != "diamond_id"])

    insert_query = f"""
        INSERT INTO public."Affiliate_app_digitization" ({cols_str})
        VALUES ({placeholders})
        ON CONFLICT (diamond_id) DO UPDATE SET
        {update_clause}
        RETURNING id;
    """

    try:
        logger.debug(
            "Inserting/updating digitisation data for diamond_id=%s with columns=%s",
            diamond_id,
            columns,
        )
        with connection.cursor() as cursor:
            cursor.execute(insert_query, tuple(values))
            worker_id = cursor.fetchone()[0]
        
        logger.info(
            "Digitisation data upserted for diamond_id=%s (id=%s)", diamond_id, worker_id
        )
        return worker_id
    except Exception:
        logger.exception("Failed to insert/update digitisation data for diamond_id=%s", diamond_id)
        connection.rollback()
        raise


def insert_digisation_score_data(
    connection, data: Dict[str, Any], diamond_id: str
) -> int:
    """
    Insert or update the scoring record for a diamond.
    """
    columns = [
        "diamond_id",
        "culet_score",
        "depth_score",
        "flouroscence",
        "gridle_score",
        "polish_score",
        "symmetry_score",
        "table_size_score",
        "key_to_symbol_score",
        "digisation_score",
    ]

    if "cut_score" in data:
        columns.extend(
            [
                "crown_angle_score",
                "cut_score",
                "measurement_score",
                "pavilion_angle_score",
                "pavilior_height_score", 
            ]
        )

    values = []
    for col in columns:
        if col == "diamond_id":
            values.append(diamond_id)
        elif col == "flouroscence":
            values.append(data.get("fluorescence_score"))
        elif col == "gridle_score":
            values.append(data.get("girdle_score"))
        elif col == "pavilior_height_score":
            values.append(data.get("pavilion_height_score"))
        elif col == "digisation_score":
            val = data.get("digisation_score")
            values.append(val.replace("%", "") if isinstance(val, str) else val)
        else:
            values.append(data.get(col))

    cols_str = ", ".join([f'"{c}"' for c in columns])
    placeholders = ", ".join(["%s"] * len(values))

    # --- NEW UPDATE LOGIC ADDED HERE ---
    update_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns if col != "diamond_id"])

    insert_query = f"""
        INSERT INTO public."Affiliate_app_scoreinfo" ({cols_str})
        VALUES ({placeholders})
        ON CONFLICT (diamond_id) DO UPDATE SET
        {update_clause}
        RETURNING id;
    """

    try:
        logger.debug(
            "Inserting/updating score data for diamond_id=%s with columns=%s",
            diamond_id,
            columns,
        )
        with connection.cursor() as cursor:
            cursor.execute(insert_query, tuple(values))
            worker_id = cursor.fetchone()[0]
        
        logger.info(
            "Score data upserted for diamond_id=%s (id=%s)", diamond_id, worker_id
        )
        return worker_id
    except Exception:
        logger.exception("Failed to insert/update score data for diamond_id=%s", diamond_id)
        connection.rollback()
        raise

def update_product_info(
    connection, diamond_id: str, digisation_id: Optional[int], score_id: Optional[int]
) -> None:
    """
    Update the main product info row with status and FK references.
    """
    update_query = """
        UPDATE public."Affiliate_app_productinfo"
        SET status=%s, digitization_id=%s, score_info_id=%s
        WHERE diamond_id = %s;
    """
    try:
        logger.debug(
            "Updating product info for diamond_id=%s (digitisation_id=%s, score_id=%s)",
            diamond_id,
            digisation_id,
            score_id,
        )
        with connection.cursor() as cursor:
            cursor.execute(update_query, (True, digisation_id, score_id, diamond_id))
        
        logger.info("Product info updated for diamond_id=%s", diamond_id)
    except Exception:
        logger.exception("Failed to update product info for diamond_id=%s", diamond_id)
        connection.rollback()
        raise


def update_ProductCharacteristics(
    connection, digitisation_score: Any, diamond_id: str, key_to_symbol_score: Any
) -> None:
    """
    Update digitization_score and key_to_symbol_score for the product characteristics row.
    """
    if str(digitisation_score) == "None":
        digitisation_score = 0
    else:
        digitisation_score = str(digitisation_score).replace("%", "")

    update_query = """
        UPDATE public."Affiliate_app_productcharacteristics"
        SET digitization_score=%s, key_to_symbol_score=%s
        WHERE diamond_id = %s;
    """

    try:
        d_score = float(digitisation_score)
    except Exception:
        d_score = 0.0

    try:
        k_score = float(key_to_symbol_score) if key_to_symbol_score else 0.0
    except Exception:
        k_score = 0.0

    try:
        logger.debug(
            "Updating product characteristics for diamond_id=%s (digitisation_score=%s, key_to_symbol_score=%s)",
            diamond_id,
            d_score,
            k_score,
        )
        with connection.cursor() as cursor:
            cursor.execute(update_query, (d_score, k_score, diamond_id))
        
        logger.info("Product characteristics updated for diamond_id=%s", diamond_id)
    except Exception:
        logger.exception(
            "Failed to update product characteristics for diamond_id=%s", diamond_id
        )
        connection.rollback()
        raise


def ai_time_update(connection, diamond_id: str) -> None:
    """
    Insert a one-time timestamp row into authen_app_aidatetimerecord for this diamond.
    """
    utc_time = datetime.now(pytz.utc)

    check_query = (
        "SELECT 1 FROM authen_app_aidatetimerecord WHERE diamond_id = %s LIMIT 1;"
    )
    insert_query = """
        INSERT INTO authen_app_aidatetimerecord (diamond_id, type, created_at)
        VALUES (%s, %s, %s);
    """

    try:
        logger.debug(
            "Ensuring AI datetime record exists for diamond_id=%s at %s", diamond_id, utc_time
        )
        with connection.cursor() as cursor:
            cursor.execute(check_query, (str(diamond_id),))
            if not cursor.fetchone():
                cursor.execute(insert_query, (diamond_id, "digitization", utc_time))
        
        logger.info("AI datetime record ensured for diamond_id=%s", diamond_id)
    except Exception:
        logger.exception(
            "Failed to insert/update AI datetime record for diamond_id=%s", diamond_id
        )
        connection.rollback()
        raise

