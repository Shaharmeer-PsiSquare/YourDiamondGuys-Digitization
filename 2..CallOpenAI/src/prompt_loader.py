"""
Prompt and few-shot example loader module.

This module handles loading prompts from text files or Python modules and
few-shot examples from YAML files, providing them in a format ready for
use with the OpenAI API.
"""

import logging
import importlib.util
import sys
from pathlib import Path
from typing import List, Dict, Any
import yaml
from src.models import FewShotExample
from src.config import get_absolute_path

logger = logging.getLogger(__name__)


def load_prompt(prompt_file: str) -> str:
    """
    Load prompt text from a file (supports .txt and .py files).

    For .py files, it imports the module and calls get_prompt() function
    or uses DIAMOND_GRADING_EXTRACTION_PROMPT constant.

    Args:
        prompt_file: Path to the prompt file (relative to project root)

    Returns:
        str: Content of the prompt

    Raises:
        FileNotFoundError: If the prompt file doesn't exist
        IOError: If there's an error reading the file
    """
    prompt_path = get_absolute_path(prompt_file)

    if not prompt_path.exists():
        logger.error(f"Prompt file not found: {prompt_path}")
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")

    try:
        # Check if it's a Python file
        if prompt_path.suffix == '.py':
            logger.debug(f"Loading prompt from Python module: {prompt_path}")

            # Load the module dynamically
            spec = importlib.util.spec_from_file_location("prompt_module", prompt_path)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not load module from {prompt_path}")

            module = importlib.util.module_from_spec(spec)
            sys.modules["prompt_module"] = module
            spec.loader.exec_module(module)

            # Try to get prompt from get_prompt() function first
            if hasattr(module, 'get_prompt'):
                prompt_content = module.get_prompt()
                logger.debug(f"Loaded prompt using get_prompt() function")
            # Otherwise try to get DIAMOND_GRADING_EXTRACTION_PROMPT constant
            elif hasattr(module, 'DIAMOND_GRADING_EXTRACTION_PROMPT'):
                prompt_content = module.DIAMOND_GRADING_EXTRACTION_PROMPT
                logger.debug(f"Loaded prompt from DIAMOND_GRADING_EXTRACTION_PROMPT constant")
            else:
                raise AttributeError(
                    f"Python module must have either get_prompt() function "
                    f"or DIAMOND_GRADING_EXTRACTION_PROMPT constant"
                )
        else:
            # Load as text file
            logger.debug(f"Loading prompt from text file: {prompt_path}")
            with open(prompt_path, "r", encoding="utf-8") as file:
                prompt_content = file.read()

        logger.debug(f"Loaded prompt from {prompt_path} ({len(prompt_content)} characters)")
        return prompt_content

    except Exception as e:
        logger.error(f"Error reading prompt file {prompt_path}: {e}")
        raise IOError(f"Error reading prompt file: {e}")


def load_few_shot_examples(few_shot_file: str) -> List[FewShotExample]:
    """
    Load few-shot examples from a YAML file.
    
    Args:
        few_shot_file: Path to the YAML file containing few-shot examples
        
    Returns:
        List[FewShotExample]: List of few-shot examples
        
    Raises:
        FileNotFoundError: If the few-shot file doesn't exist
        ValueError: If the YAML file is malformed or invalid
    """
    few_shot_path = get_absolute_path(few_shot_file)
    
    if not few_shot_path.exists():
        logger.warning(f"Few-shot examples file not found: {few_shot_path}")
        logger.info("Continuing without few-shot examples")
        return []
    
    try:
        with open(few_shot_path, "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)
        
        if not data or "examples" not in data:
            logger.warning(f"No 'examples' key found in {few_shot_path}")
            return []
        
        examples = []
        for example_data in data["examples"]:
            try:
                example = FewShotExample(**example_data)
                examples.append(example)
            except Exception as e:
                logger.warning(f"Skipping invalid example: {e}")
                continue
        
        logger.info(f"Loaded {len(examples)} few-shot examples from {few_shot_path}")
        return examples
    
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file {few_shot_path}: {e}")
        raise ValueError(f"Invalid YAML file: {e}")
    
    except Exception as e:
        logger.error(f"Error reading few-shot file {few_shot_path}: {e}")
        raise IOError(f"Error reading few-shot file: {e}")


def format_few_shot_for_api(examples: List[FewShotExample]) -> List[Dict[str, Any]]:
    """
    Format few-shot examples for use in OpenAI API messages.
    
    Args:
        examples: List of FewShotExample objects
        
    Returns:
        List[Dict[str, Any]]: List of message dictionaries formatted for OpenAI API
    """
    formatted_messages = []
    
    for example in examples:
        message = {
            "role": example.role,
            "content": example.content
        }
        formatted_messages.append(message)
    
    logger.debug(f"Formatted {len(formatted_messages)} few-shot examples for API")
    return formatted_messages


def build_prompt_with_examples(
    prompt: str,
    few_shot_examples: List[FewShotExample]
) -> str:
    """
    Build a complete prompt by combining the base prompt with few-shot examples.
    
    Args:
        prompt: Base prompt text
        few_shot_examples: List of few-shot examples
        
    Returns:
        str: Combined prompt with examples
    """
    if not few_shot_examples:
        return prompt
    
    # Build examples section
    examples_text = "\n\n## EXAMPLES\n\n"
    for i, example in enumerate(few_shot_examples, 1):
        examples_text += f"### Example {i}\n"
        examples_text += f"**{example.role.upper()}**: {example.content}\n\n"
    
    # Combine prompt with examples
    combined_prompt = prompt + examples_text
    
    logger.debug(f"Built combined prompt with {len(few_shot_examples)} examples")
    return combined_prompt

