"""
OpenAI API client module.

This module provides functions for interacting with the OpenAI API,
including sending requests and processing responses.
"""

import json
import logging
from typing import Optional, Dict, Any, List, Union
import base64
import requests

from src.models import (
    Config,
    DiamondGradingReport,
    OpenAIResponse,
    ProcessingResult,
    FewShotExample
)

logger = logging.getLogger(__name__)

# Per‑1K token pricing (USD) for supported models
MODEL_PRICING = {
    "gpt-4.1": {"input": 0.002, "output": 0.008},
}


def create_api_headers(api_key: str) -> Dict[str, str]:
    """
    Create headers for OpenAI API requests.
    
    Args:
        api_key: OpenAI API key
        
    Returns:
        Dict[str, str]: Headers dictionary
    """
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

def url_to_base64(url):
    """
    Download an image from a URL and convert it to a base64 data URI.

    NOTE:
        This function is a potential bottleneck when called many times
        because it performs network I/O. We enforce a timeout to avoid
        hanging indefinitely on bad URLs.
    """
    try:
        # Download image from URL with a reasonable timeout
        response = requests.get(url, timeout=120)
        # response.raise_for_status()  # Raise error if failed

        # Encode image to Base64
        base64_str = base64.b64encode(response.content).decode("utf-8")
        data_uri = f"data:image/jpeg;base64,{base64_str}"
        return data_uri

    except Exception as e:
        logger.warning(f"Failed to download or encode image from URL '{url}': {e}")
        return None

def create_message_content(prompt: str, all_url: [], markdown: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Create message content with text, image and markdown for OpenAI API.
    
    Args:
        prompt: Text prompt to send
        base64_image: Base64-encoded image string
        markdown: Markdown string
    Returns:
        List[Dict[str, Any]]: Message content list
    """
    result = [
                 {"type": "text", "text": prompt}
             ] + [
                 {"type": "image_url", "image_url": {"url": url_to_base64(url)}}
                #  {"type": "image_url", "image_url": {"url": url}}
                 for url in all_url
             ]
    if markdown:
        result.append({"type": "text", "text": markdown})
    return result


def build_api_payload(
    prompt: str,
    base64_image: str,
    config: Config,
    few_shot_examples: Optional[List[FewShotExample]] = None,
    markdown: Optional[str] = None
) -> Dict[str, Any]:
    """
    Build the complete payload for OpenAI API request.
    
    Args:
        prompt: Text prompt
        base64_image: Base64-encoded image
        config: Configuration object
        few_shot_examples: Optional list of few-shot examples
        
    Returns:
        Dict[str, Any]: Complete API payload
    """
    messages = []
    
    # Add few-shot examples if provided
    if few_shot_examples:
        for example in few_shot_examples:
            messages.append({
                "role": example.role,
                "content": example.content
            })
        logger.debug(f"Added {len(few_shot_examples)} few-shot examples to payload")
    
    # Add the main user message with prompt and image
    messages.append({
        "role": "user",
        "content": create_message_content(prompt, base64_image, markdown)
    })
    
    payload = {
        "model": config.model_name,
        "temperature": config.temperature,
        "messages": messages,
        "max_tokens": config.max_tokens
    }
    
    return payload
import re
def clean_girdle_value(girdle):
    try:
        # Replace hyphen ranges with " to "
        girdle = re.sub(r'\s*-\s*', ' to ', girdle)

        # Replace commas with " to "
        girdle = re.sub(r'\s*,\s*', ' to ', girdle)

        # Replace 'on.' or 'on' as a standalone word with 'to'
        girdle = re.sub(r'\bon\.?\b', 'to', girdle, flags=re.IGNORECASE)

        # Remove parentheses and their content
        cleaned = re.sub(r'\(.*?\)', '', girdle)

        # Remove any leftover "(" or ")"
        cleaned = re.sub(r'[()]', '', cleaned)

        # Remove numbers, floats (even with spaces), and % signs
        cleaned = re.sub(r'\d+(?:\s*\.\s*\d*)?%?', '', cleaned)

        # Remove extra % signs that are not attached to numbers
        cleaned = re.sub(r'%', '', cleaned)

        # Remove extra spaces
        cleaned = ' '.join(cleaned.split())
        return True, cleaned
    except:
        return False, girdle

def send_openai_request(
    base64_image: str,
    prompt: str,
    config: Config,
    few_shot_examples: Optional[List[FewShotExample]] = None,
    markdown: Optional[str] = None
) -> Optional[OpenAIResponse]:
    """
    Send a request to OpenAI API.
    
    Args:
        base64_image: Base64-encoded image string
        prompt: Text prompt
        config: Configuration object
        few_shot_examples: Optional list of few-shot examples
        
    Returns:
        Optional[OpenAIResponse]: Response from OpenAI API, or None if request failed
    """
    headers = create_api_headers(config.openai_api_key)
    payload = build_api_payload(prompt, base64_image, config, few_shot_examples, markdown)
    
    try:
        logger.debug("Sending request to OpenAI API")
        logger.debug(f"Headers: {headers}")
        logger.debug(f"Payload: {payload}")

        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=60
        )

        if response.status_code == 200:
            response_data = response.json()
            usage = response_data.get("usage", {})
            model_used = response_data.get("model", config.model_name)

            # Log token usage and rough cost
            if usage:
                in_tok = usage.get("prompt_tokens", 0)
                out_tok = usage.get("completion_tokens", 0)
                pricing = MODEL_PRICING.get(model_used, MODEL_PRICING.get(config.model_name))
                if pricing:
                    cost = (in_tok / 1000 * pricing["input"]) + (out_tok / 1000 * pricing["output"])
                    logger.info(
                        "OpenAI usage — model=%s, prompt_tokens=%s, completion_tokens=%s, cost≈$%.4f",
                        model_used,
                        in_tok,
                        out_tok,
                        cost,
                    )
                else:
                    logger.info(
                        "OpenAI usage — model=%s, prompt_tokens=%s, completion_tokens=%s (no pricing table match)",
                        model_used,
                        in_tok,
                        out_tok,
                    )

            raw_content = response_data['choices'][0]['message']['content']
            if raw_content.startswith("```json"):
                raw_content = raw_content[7:]
            if raw_content.endswith("```"):
                raw_content = raw_content[:-3]
            raw_content = raw_content.strip()

            try:
                parsed = json.loads(raw_content)
            except Exception as exc:
                logger.error("Failed to parse OpenAI JSON content: %s", exc)
                logger.debug("Raw content snippet: %s", raw_content[:400])
                return None

            if not isinstance(parsed, list):
                logger.error("Unexpected response format (expected list of records)")
                logger.debug("Parsed content: %s", parsed)
                return None

            # Enrich parsed items and attach image URL if available
            for idx, data in enumerate(parsed):
                if isinstance(data, dict):
                    girdle_val = data.get('girdle')
                    st, cleaned = clean_girdle_value(girdle_val) if girdle_val else (False, girdle_val)
                    if st:
                        data['girdle'] = cleaned
                    data['image_url'] = base64_image[idx] if idx < len(base64_image) else None
                else:
                    logger.debug("Skipping non-dict item at index %s: %s", idx, data)

            logger.info("Successfully received response from OpenAI API")
            logger.debug(f"Parsed Response: {parsed}")
            logger.debug("Parsed %d records from response", len(parsed))
            return parsed
        else:
            logger.error(f"OpenAI API request failed with status {response.status_code}")
            logger.error(f"Response: {response.text}")
            return None
    
    except requests.exceptions.Timeout:
        logger.error("OpenAI API request timed out")
        return None
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {e}")
        return None
    
    except Exception as e:
        logger.error(f"Unexpected error during API request: {e}")
        return None


def parse_json_response(response_content: str) -> Optional[Dict[str, Any]]:
    """
    Parse JSON response from OpenAI, handling markdown code blocks.
    
    Args:
        response_content: Raw response content from OpenAI
        
    Returns:
        Optional[Dict[str, Any]]: Parsed JSON data, or None if parsing failed
    """
    # Clean up markdown code blocks if present
    cleaned_content = response_content.strip()
    
    if cleaned_content.startswith("```json"):
        cleaned_content = cleaned_content[7:]
    elif cleaned_content.startswith("```"):
        cleaned_content = cleaned_content[3:]
    
    if cleaned_content.endswith("```"):
        cleaned_content = cleaned_content[:-3]
    
    cleaned_content = cleaned_content.strip()
    
    try:
        data = json.loads(cleaned_content)
        logger.debug("Successfully parsed JSON response")
        return data
    
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response: {e}")
        logger.debug(f"Response content: {cleaned_content[:200]}...")
        return None


def extract_diamond_data(
    base64_image: str,
    prompt: str,
    config: Config,
    few_shot_examples: Optional[List[FewShotExample]] = None,
    markdown: Optional[str] = None
) -> Optional[DiamondGradingReport]:
    """
    Extract diamond grading data from an image.
    
    Args:
        base64_image: Base64-encoded image string
        prompt: Extraction prompt
        config: Configuration object
        few_shot_examples: Optional list of few-shot examples
        
    Returns:
        Optional[DiamondGradingReport]: Extracted diamond data, or None if extraction failed
    """
    response = send_openai_request(base64_image, prompt, config, few_shot_examples, markdown)
    
    # if not response:
    return response
    
    # Parse the JSON response
    # data_dict = parse_json_response(response.content)
    #
    # if not data_dict:
    #     return None
    #
    # # Validate that we have meaningful data (at least shape should be present)
    # shape_value = data_dict.get('shape', '')
    # if not shape_value:
    #     logger.warning("No shape value found in response, considering extraction failed")
    #     return None
    #
    # try:
    #     # Create DiamondGradingReport from the parsed data
    #     diamond_report = DiamondGradingReport(**data_dict)
    #     logger.info(f"Successfully extracted diamond data: {diamond_report.shape}, {diamond_report.carat} carat")
    #     return diamond_report
    #
    # except Exception as e:
    #     logger.error(f"Error creating DiamondGradingReport: {e}")
    #     return None


def extract_with_retries(
    list_of_url: [],
    prompt: str,
    config: Config,
    few_shot_examples: Optional[List[FewShotExample]] = None,
    markdown: Optional[str] = None,
) -> Union[List[Dict[str, Any]], ProcessingResult]:
    """
    Extract diamond data with automatic retries.
    
    Args:
        base64_image: Base64-encoded image string
        prompt: Extraction prompt
        config: Configuration object
        few_shot_examples: Optional list of few-shot examples
        
    Returns:
        ProcessingResult: Result object containing success status and data or error
    """
    logger.info(f"Starting extraction with up to {config.max_retries} retries")
    
    for attempt in range(config.max_retries):
        logger.debug(f"Attempt {attempt + 1}/{config.max_retries}")
        
        diamond_data = extract_diamond_data(list_of_url, prompt, config, few_shot_examples, markdown)
        
        if diamond_data:
           return diamond_data
    logger.error(f"Extraction failed after {config.max_retries} attempts")
    return ProcessingResult(
        success=False,
        data=None,
        error=f"Failed to extract data after {config.max_retries} attempts",
        retries=config.max_retries
    )

