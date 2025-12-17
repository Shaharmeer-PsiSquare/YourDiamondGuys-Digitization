"""
Configuration management module.

This module handles loading configuration from environment variables
and configuration files, providing a centralized configuration object.
"""

import logging
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from src.models import Config

logger = logging.getLogger(__name__)

def get_project_root() -> Path:
    """
    Get the project root directory (2..CallOpenAI folder).
    
    Returns:
        Path: Path to the project root directory
    """
    return Path(__file__).parent.parent


def get_parent_root() -> Path:
    """
    Get the parent Digitization root directory (where the main .env file is).
    
    Returns:
        Path: Path to the parent Digitization directory
    """
    return Path(__file__).parent.parent.parent


def load_config(env_file: Optional[str] = None) -> Config:
    """
    Load configuration from environment variables and defaults.
    
    Args:
        env_file: Optional path to .env file. If None, looks for .env in project root
        
    Returns:
        Config: Configuration object with all settings
        
    Raises:
        ValueError: If required configuration is missing
    """
    # Load environment variables from .env file
    if env_file:
        load_dotenv(env_file)
    else:
        # First try parent root (Digitization/.env) - this is the main config file
        parent_root = get_parent_root()
        parent_env_path = parent_root / ".env"
        if parent_env_path.exists():
            load_dotenv(parent_env_path)
            logger.debug(f"Loaded environment variables from parent .env: {parent_env_path}")
        else:
            # Fallback to local .env if parent doesn't exist
            project_root = get_project_root()
            env_path = project_root / ".env"
            if env_path.exists():
                load_dotenv(env_path)
                logger.debug(f"Loaded environment variables from local .env: {env_path}")
    
    # Get API key from environment
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError(
            "OPENAI_API_KEY not found in environment variables. "
            "Please set it in your .env file or environment."
        )
    
    # Build configuration
    config = Config(
        openai_api_key=api_key,
        model_name=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        temperature=float(os.getenv("OPENAI_TEMPERATURE", "0")),
        max_tokens=int(os.getenv("OPENAI_MAX_TOKENS", "2000")),
        max_retries=int(os.getenv("MAX_RETRIES", "4")),
        prompt_file=os.getenv(
            "PROMPT_FILE",
            "prompt.txt"
        ),
        few_shot_file=os.getenv(
            "FEW_SHOT_FILE",
            "prompts/shots/examples.yaml"
        ),
        output_dir=os.getenv("OUTPUT_DIR", "output")
    )
    
    logger.info(f"Configuration loaded successfully")
    logger.debug(f"Model: {config.model_name}, Temperature: {config.temperature}")
    
    return config


def ensure_output_directory(config: Config) -> Path:
    """
    Ensure the output directory exists.
    
    Args:
        config: Configuration object
        
    Returns:
        Path: Path to the output directory
    """
    project_root = get_project_root()
    output_path = project_root / config.output_dir
    output_path.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Output directory ensured at {output_path}")
    return output_path


def get_absolute_path(relative_path: str) -> Path:
    """
    Convert a relative path to an absolute path from project root.
    
    Args:
        relative_path: Path relative to project root
        
    Returns:
        Path: Absolute path
    """
    project_root = get_project_root()
    return project_root / relative_path