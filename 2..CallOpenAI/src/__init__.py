"""
Diamond Grading Report Extraction Package.

This package provides modular components for extracting diamond grading
information from certificate images using OpenAI's vision API.
"""

__version__ = "1.0.0"
__author__ = "PsiSquare"


from src.config import load_config, get_project_root
from src.logger import setup_logging, get_logger
from src.models import DiamondGradingReport, Config, ProcessingResult, FewShotExample
from src.openai_client import extract_with_retries
from src.prompt_loader import load_prompt, load_few_shot_examples
from src.utils import image_to_base64, save_result_to_json

__all__ = [
    "DiamondGradingReport",
    "Config",
    "ProcessingResult",
    "FewShotExample",
    "load_config",
    "get_project_root",
    "setup_logging",
    "get_logger",
    "image_to_base64",
    "save_result_to_json",
    "extract_with_retries",
    "load_prompt",
    "load_few_shot_examples",
]
