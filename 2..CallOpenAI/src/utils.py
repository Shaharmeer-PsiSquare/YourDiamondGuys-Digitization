"""
Utility functions module.

This module provides utility functions for image processing,
file operations, and data handling.
"""
import os
from openai import OpenAI

import base64
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List

from src.config import get_absolute_path
from src.models import DiamondGradingReport, ProcessingResult

logger = logging.getLogger(__name__)


def image_to_base64(image_path: str) -> Optional[str]:
    """
    Convert an image file to base64-encoded string.

    Args:
        image_path: Path to the image file (can be relative or absolute)

    Returns:
        Optional[str]: Base64-encoded string, or None if conversion failed
    """
    try:
        # Handle both relative and absolute paths
        if not Path(image_path).is_absolute():
            full_path = get_absolute_path(image_path)
        else:
            full_path = Path(image_path)

        if not full_path.exists():
            logger.error(f"Image file not found: {full_path}")
            return None

        with open(full_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode("utf-8")

        logger.debug(f"Successfully encoded image from {full_path}")
        logger.debug(f"Encoded string length: {len(encoded_string)} characters")

        return encoded_string

    except FileNotFoundError:
        logger.error(f"File not found: {image_path}")
        return None

    except Exception as e:
        logger.error(f"Error encoding image {image_path}: {e}")
        return None


def save_result_to_json(
    result: ProcessingResult, output_dir: str, filename: Optional[str] = None
) -> Optional[Path]:
    """
    Save processing result to a JSON file.

    Args:
        result: ProcessingResult object to save
        output_dir: Directory to save the file in
        filename: Optional custom filename. If None, generates timestamp-based name

    Returns:
        Optional[Path]: Path to the saved file, or None if save failed
    """
    try:
        # Ensure output directory exists
        output_path = get_absolute_path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"diamond_report_{timestamp}.json"

        # Ensure .json extension
        if not filename.endswith(".json"):
            filename += ".json"

        file_path = output_path / filename


        # Write to file
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved result to {file_path}")
        return file_path

    except Exception as e:
        logger.error(f"Error saving result to JSON: {e}")
        return None


def save_diamond_report_to_json(
    report: DiamondGradingReport, output_dir: str, filename: Optional[str] = None
) -> Optional[Path]:
    """
    Save diamond grading report to a JSON file.

    Args:
        report: DiamondGradingReport object to save
        output_dir: Directory to save the file in
        filename: Optional custom filename. If None, generates timestamp-based name

    Returns:
        Optional[Path]: Path to the saved file, or None if save failed
    """
    try:
        # Ensure output directory exists
        output_path = get_absolute_path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_num = report.report_number or "unknown"
            filename = f"diamond_{report_num}_{timestamp}.json"

        # Ensure .json extension
        if not filename.endswith(".json"):
            filename += ".json"

        file_path = output_path / filename

        # Convert report to dict for JSON serialization
        report_dict = report.model_dump(mode="json")

        # Write to file
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(report_dict, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved diamond report to {file_path}")
        return file_path

    except Exception as e:
        logger.error(f"Error saving diamond report to JSON: {e}")
        return None


def validate_image_path(image_path: str) -> bool:
    """
    Validate that an image path exists and is a valid image file.

    Args:
        image_path: Path to the image file

    Returns:
        bool: True if valid, False otherwise
    """
    try:
        # Handle both relative and absolute paths
        if not Path(image_path).is_absolute():
            full_path = get_absolute_path(image_path)
        else:
            full_path = Path(image_path)

        if not full_path.exists():
            logger.error(f"Image path does not exist: {full_path}")
            return False

        if not full_path.is_file():
            logger.error(f"Image path is not a file: {full_path}")
            return False

        # Check file extension
        valid_extensions = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp"}
        if full_path.suffix.lower() not in valid_extensions:
            logger.warning(f"Image file has unusual extension: {full_path.suffix}")

        logger.debug(f"Image path validated: {full_path}")
        return True

    except Exception as e:
        logger.error(f"Error validating image path: {e}")
        return False


def format_diamond_report_summary(report: DiamondGradingReport) -> str:
    """
    Format a diamond grading report as a human-readable summary.

    Args:
        report: DiamondGradingReport object

    Returns:
        str: Formatted summary string
    """
    summary_lines = ["=" * 60, "DIAMOND GRADING REPORT SUMMARY", "=" * 60, ""]

    # Basic information
    if report.report_number:
        summary_lines.append(f"Report Number: {report.report_number}")
    if report.name:
        summary_lines.append(f"Laboratory: {report.name}")

    summary_lines.append("")
    summary_lines.append("DIAMOND CHARACTERISTICS:")
    summary_lines.append("-" * 60)

    # The 4 Cs
    if report.shape:
        summary_lines.append(f"Shape: {report.shape}")
    if report.carat:
        summary_lines.append(f"Carat Weight: {report.carat}")
    if report.color_grade:
        summary_lines.append(f"Color Grade: {report.color_grade}")
    if report.clarity:
        summary_lines.append(f"Clarity: {report.clarity}")
    if report.cut:
        summary_lines.append(f"Cut Grade: {report.cut}")

    # Additional grading
    summary_lines.append("")
    summary_lines.append("ADDITIONAL GRADING:")
    summary_lines.append("-" * 60)

    if report.polish:
        summary_lines.append(f"Polish: {report.polish}")
    if report.symmetry:
        summary_lines.append(f"Symmetry: {report.symmetry}")
    if report.fluorescence:
        summary_lines.append(f"Fluorescence: {report.fluorescence}")

    # Measurements
    if report.measurement:
        summary_lines.append("")
        summary_lines.append("MEASUREMENTS:")
        summary_lines.append("-" * 60)
        summary_lines.append(f"Dimensions: {report.measurement}")

    summary_lines.append("=" * 60)

    return "\n".join(summary_lines)

def markdown_extraction(urls: List[str], max_retries: int = 3) -> Optional[str]:
    """
    Extract markdown from a URL using OpenAI GPT-4o.
    
    Requires OPENAI_API_KEY to be set in environment variables.

    Args:
        urls: List of image URLs to extract markdown from
        max_retries: Number of times to retry on failure before giving up

    Returns:
        Optional[str]: Markdown string, or None if extraction failed
    """
    for attempt in range(1, max_retries + 1):
        try:
            # Initialize client - it will automatically use os.environ.get("OPENAI_API_KEY")
            client = OpenAI()

            logger.debug(
                f"Starting markdown extraction for: {urls} "
                f"(attempt {attempt}/{max_retries})"
            )

            prompt_text = """
            You are a high-precision OCR engine. Your only goal is to transcribe text visible in the image into Markdown.

            RULES:
            1. **Transcribe Everything:** Output every single piece of text visible, including small print, report numbers, symbols, percentages, and dates.
            2. **Key-Value Formatting:** For labeled fields (like "Shape and Cutting Style: Round Brilliant"), use the format: `**Label:** Value`.
            3. **Tables:** If data is arranged in columns (like Grading Results or Grading Scales), strictly use Markdown Tables.
            4. **Structure:** Use `##` for section headers (e.g., ## GRADING RESULTS).
            5. **No Commentary:** Do not start with "Here is the transcription" or "The image shows". Start directly with the text.
            6. **Visuals:** If there is a diagram with numbers (like angles/percentages), list them as a bulleted list: `* Crown Angle: 34.5°`.
            """

            # Build message content with one text block plus one image per URL
            content = [
                {"type": "text", "text": prompt_text},
            ] + [
                {"type": "image_url", "image_url": {"url": url}}
                for url in urls
            ]

            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "user",
                        "content": content,
                    }
                ],
                temperature=0,  # Deterministic output
            )

            content = response.choices[0].message.content
            logger.info(f"Successfully extracted markdown from {urls}")
            return content

        except Exception as e:
            logger.error(
                f"Error extracting markdown from {urls} "
                f"(attempt {attempt}/{max_retries}): {e}"
            )

    # All attempts failed
    logger.error(
        f"Failed to extract markdown from {urls} after {max_retries} attempts"
    )
    return None