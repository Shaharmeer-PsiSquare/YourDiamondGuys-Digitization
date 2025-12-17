"""
OpenAI Batch API client module.

This module provides functions for batch processing with OpenAI API,
allowing multiple requests to be processed asynchronously.
"""

import json
import logging
import os
import time
import tempfile
from typing import Optional, Dict, Any, List, Union
from openai import OpenAI
from src.models import Config, FewShotExample

logger = logging.getLogger(__name__)

# OpenAI Batch API endpoint
BATCH_ENDPOINT = "/v1/chat/completions"
COMPLETION_WINDOW = "24h"


def create_batch_input_file(
    requests: List[Dict[str, Any]],
    output_file: Optional[str] = None
) -> str:
    """
    Create a JSONL input file for batch processing.
    
    Args:
        requests: List of request dictionaries, each with custom_id, method, url, and body
        output_file: Optional path to output file. If None, creates a temporary file.
        
    Returns:
        str: Path to the created JSONL file
    """
    if output_file is None:
        # Create a temporary file
        fd, output_file = tempfile.mkstemp(suffix='.jsonl', prefix='batch_input_')
        os.close(fd)
    
    logger.info(f"Creating batch input file: {output_file}")
    
    with open(output_file, "w") as f:
        for req in requests:
            f.write(json.dumps(req) + "\n")
    
    logger.info(f"Created batch input file with {len(requests)} requests")
    return output_file


def upload_batch_file(client: OpenAI, file_path: str) -> str:
    """
    Upload a JSONL file to OpenAI's file storage for batch processing.
    
    Args:
        client: OpenAI client instance
        file_path: Path to the JSONL file to upload
        
    Returns:
        str: File ID of the uploaded file
    """
    logger.info(f"Uploading batch file: {file_path}")
    
    with open(file_path, "rb") as f:
        uploaded_file = client.files.create(
            file=f,
            purpose="batch"
        )
    
    logger.info(f"File uploaded successfully. File ID: {uploaded_file.id}")
    return uploaded_file.id


def create_batch_job(
    client: OpenAI,
    input_file_id: str,
    completion_window: str = COMPLETION_WINDOW
) -> str:
    """
    Create and start a batch job.
    
    Args:
        client: OpenAI client instance
        input_file_id: File ID of the uploaded input file
        completion_window: Time window for batch completion (default: 24h)
        
    Returns:
        str: Batch job ID
    """
    logger.info("Creating batch job...")
    
    batch_job = client.batches.create(
        input_file_id=input_file_id,
        endpoint=BATCH_ENDPOINT,
        completion_window=completion_window
    )
    
    logger.info(f"Batch job created successfully. Job ID: {batch_job.id}")
    return batch_job.id


def monitor_batch_job(
    client: OpenAI,
    job_id: str,
    poll_interval: int = 60,
    max_wait_time: Optional[int] = None
) -> Dict[str, Any]:
    """
    Monitor a batch job until completion.
    
    Args:
        client: OpenAI client instance
        job_id: Batch job ID to monitor
        poll_interval: Seconds to wait between status checks (default: 60)
        max_wait_time: Maximum time to wait in seconds (None for unlimited)
        
    Returns:
        Dict[str, Any]: Batch job status information
    """
    logger.info(f"Monitoring batch job: {job_id}")
    start_time = time.time()
    
    while True:
        try:
            batch_job = client.batches.retrieve(job_id)
            status = batch_job.status
            
            completed = getattr(batch_job.request_counts, 'completed', 0)
            total = getattr(batch_job.request_counts, 'total', 0)
            
            logger.info(
                f"Batch job status: {status}. "
                f"Completed: {completed}/{total}"
            )
            
            if status == "completed":
                logger.info("✅ Batch job completed successfully!")
                return {
                    "status": status,
                    "job": batch_job,
                    "output_file_id": batch_job.output_file_id
                }
            
            if status in ["failed", "cancelled", "expired"]:
                logger.error(f"❌ Batch job finished with status: {status}")
                return {
                    "status": status,
                    "job": batch_job,
                    "output_file_id": None
                }
            
            # Check max wait time
            if max_wait_time and (time.time() - start_time) > max_wait_time:
                logger.warning(f"Max wait time ({max_wait_time}s) exceeded")
                return {
                    "status": "timeout",
                    "job": batch_job,
                    "output_file_id": None
                }
            
            logger.debug(f"Waiting {poll_interval} seconds before next check...")
            time.sleep(poll_interval)
            
        except Exception as e:
            logger.error(f"Error monitoring batch job: {e}")
            raise


def download_batch_results(
    client: OpenAI,
    output_file_id: str
) -> List[Dict[str, Any]]:
    """
    Download and parse batch results from OpenAI.
    
    Args:
        client: OpenAI client instance
        output_file_id: File ID of the output file
        
    Returns:
        List[Dict[str, Any]]: List of parsed result dictionaries
    """
    logger.info(f"Downloading batch results from file: {output_file_id}")
    
    result_file_content = client.files.content(output_file_id).content.decode('utf-8')
    
    # Parse JSONL content
    results = []
    for line in result_file_content.strip().split('\n'):
        if line.strip():
            results.append(json.loads(line))
    
    logger.info(f"Downloaded {len(results)} results")
    return results


def parse_batch_response(
    batch_result: Dict[str, Any],
    base64_images: List[str]
) -> Optional[List[Dict[str, Any]]]:
    """
    Parse a single batch response result, similar to send_openai_request parsing.
    
    Args:
        batch_result: Single result dictionary from batch output
        base64_images: List of base64 image URLs for enrichment
        
    Returns:
        Optional[List[Dict[str, Any]]]: Parsed response data, or None if parsing failed
    """
    from src.openai_client import clean_girdle_value
    
    try:
        # Extract response body
        response = batch_result.get('response', {})
        
        if response.get('status_code') != 200:
            logger.error(
                f"Batch request failed with status {response.get('status_code')}. "
                f"Error: {response.get('body', {}).get('error', 'Unknown error')}"
            )
            return None
        
        response_body = response.get('body', {})
        
        # Extract content from response
        choices = response_body.get('choices', [])
        if not choices:
            logger.error("No choices in batch response")
            return None
        
        raw_content = choices[0].get('message', {}).get('content', '')
        
        if not raw_content:
            logger.error("No content in batch response")
            return None
        
        # Clean markdown code blocks
        if raw_content.startswith("```json"):
            raw_content = raw_content[7:]
        if raw_content.endswith("```"):
            raw_content = raw_content[:-3]
        raw_content = raw_content.strip()
        
        # Parse JSON
        try:
            parsed = json.loads(raw_content)
        except json.JSONDecodeError as exc:
            logger.error(f"Failed to parse JSON from batch response: {exc}")
            logger.debug(f"Raw content snippet: {raw_content[:400]}")
            return None
        
        if not isinstance(parsed, list):
            logger.error("Unexpected response format (expected list of records)")
            logger.debug(f"Parsed content: {parsed}")
            return None
        
        # Enrich parsed items (similar to send_openai_request)
        for idx, data in enumerate(parsed):
            if isinstance(data, dict):
                # Clean girdle value if present
                girdle_val = data.get('girdle')
                if girdle_val:
                    st, cleaned = clean_girdle_value(girdle_val)
                    if st:
                        data['girdle'] = cleaned
                
                # Attach image URL
                if idx < len(base64_images):
                    data['image_url'] = base64_images[idx]
            else:
                logger.debug(f"Skipping non-dict item at index {idx}: {data}")
        
        return parsed
        
    except Exception as e:
        logger.error(f"Error parsing batch response: {e}")
        return None


def create_batch_requests(
    list_of_url: List[str],
    prompt: str,
    config: Config,
    few_shot_examples: Optional[List[FewShotExample]] = None,
    markdown: Optional[str] = None,
    urls_per_request: int = 5,
    job_prefix: str = "",
) -> List[Dict[str, Any]]:
    """
    Create batch request dictionaries from input parameters.

    IMPORTANT:
        For batch jobs we send **base64-encoded images** (via data URIs),
        matching the non-batch flow. This avoids OpenAI needing to re-download
        images from external URLs, which can cause invalid_image_url / timeout
        errors during batch processing.
    
    This creates multiple batch requests, each containing a group of URLs.
    
    Args:
        list_of_url: List of image URLs
        prompt: Text prompt
        config: Configuration object
        few_shot_examples: Optional list of few-shot examples
        markdown: Optional markdown string
        urls_per_request: Number of URLs to include in each request (default: 5)
        job_prefix: Optional string prefix to make custom_id values globally unique
        
    Returns:
        List[Dict[str, Any]]: List of batch request dictionaries
    """
    # Local import to avoid circular import at module load time
    from src.openai_client import create_message_content

    batch_requests: List[Dict[str, Any]] = []
    
    # Group URLs into batches
    local_request_index = 0
    for batch_idx in range(0, len(list_of_url), urls_per_request):
        url_batch = list_of_url[batch_idx:batch_idx + urls_per_request]

        # Build message content using the same helper as the non-batch flow.
        # This downloads each image and embeds it as a base64 data URI.
        message_content = create_message_content(prompt, url_batch, markdown)

        # Build messages list
        messages: List[Dict[str, Any]] = []
        
        # Add few-shot examples if provided
        if few_shot_examples:
            for example in few_shot_examples:
                messages.append({
                    "role": example.role,
                    "content": example.content
                })
        
        # Add the main user message
        messages.append({
            "role": "user",
            "content": message_content
        })
        
        # Build request body
        body: Dict[str, Any] = {
            "model": config.model_name,
            "temperature": config.temperature,
            "messages": messages,
            "max_tokens": config.max_tokens
        }
        
        # Create batch request for this group
        batch_request: Dict[str, Any] = {
            # Ensure custom_id is globally unique, even across multiple files
            "custom_id": f"{job_prefix}request-{local_request_index}",
            "method": "POST",
            "url": BATCH_ENDPOINT,
            "body": body
        }
        
        batch_requests.append(batch_request)
        local_request_index += 1
    
    return batch_requests


def process_batch_requests(
    list_of_url: List[str],
    prompt: str,
    config: Config,
    few_shot_examples: Optional[List[FewShotExample]] = None,
    markdown: Optional[str] = None,
    poll_interval: int = 60,
    max_wait_time: Optional[int] = None,
    urls_per_request: int = 5
) -> Optional[List[Dict[str, Any]]]:
    """
    Process multiple requests using OpenAI batch API.
    
    Args:
        list_of_url: List of image URLs to process
        prompt: Text prompt
        config: Configuration object
        few_shot_examples: Optional list of few-shot examples
        markdown: Optional markdown string
        poll_interval: Seconds to wait between status checks
        max_wait_time: Maximum time to wait for completion
        urls_per_request: Number of URLs to include in each request (default: 5)
        
    Returns:
        Optional[List[Dict[str, Any]]]: Combined parsed results from all requests, or None if failed
    """
    try:
        # Initialize OpenAI client
        client = OpenAI(api_key=config.openai_api_key)
        
        # Create batch requests (grouped by urls_per_request)
        logger.info(f"Creating batch requests for {len(list_of_url)} URLs (grouped into requests of {urls_per_request} URLs each)")
        batch_requests = create_batch_requests(
            list_of_url, prompt, config, few_shot_examples, markdown, urls_per_request
        )
        logger.info(f"Created {len(batch_requests)} batch requests")
        
        # Create input file
        input_file = create_batch_input_file(batch_requests)
        
        try:
            # Upload file
            file_id = upload_batch_file(client, input_file)
            
            # Create batch job
            job_id = create_batch_job(client, file_id)
            
            # Monitor job
            job_status = monitor_batch_job(
                client, job_id, poll_interval, max_wait_time
            )
            
            if job_status["status"] != "completed":
                logger.error(f"Batch job did not complete successfully: {job_status['status']}")
                return None
            
            # Download results
            results = download_batch_results(client, job_status["output_file_id"])
            
            # Parse all results
            # Each result corresponds to one batch request (group of URLs)
            all_parsed_results = []
            for result in results:
                custom_id = result.get('custom_id', '')
                # Extract batch index from custom_id (e.g., "request-0" -> 0)
                try:
                    batch_idx = int(custom_id.split('-')[1]) if '-' in custom_id else 0
                    start_idx = batch_idx * urls_per_request
                    end_idx = min(start_idx + urls_per_request, len(list_of_url))
                    url_batch = list_of_url[start_idx:end_idx]
                except (ValueError, IndexError):
                    # Fallback: use all URLs if we can't parse the index
                    url_batch = list_of_url
                
                # Parse this result with its corresponding URL batch
                parsed = parse_batch_response(result, url_batch)
                if parsed:
                    all_parsed_results.extend(parsed)
            
            logger.info(f"Successfully processed {len(all_parsed_results)} results from batch")
            return all_parsed_results
            
        finally:
            # Clean up temporary file
            if os.path.exists(input_file):
                try:
                    os.remove(input_file)
                    logger.debug(f"Cleaned up temporary file: {input_file}")
                except Exception as e:
                    logger.warning(f"Failed to remove temporary file: {e}")
        
    except Exception as e:
        logger.error(f"Error in batch processing: {e}", exc_info=True)
        return None

