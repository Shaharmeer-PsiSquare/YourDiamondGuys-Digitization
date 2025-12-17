# Batch Processing Scripts

This folder contains scripts for processing OpenAI batch jobs in separate steps.

## Standard Scripts (Single Batch Job)

For processing with a single batch job, use:
- `create_batch.py` - Create batch input file
- `submit_batch.py` - Submit batch to OpenAI
- `check_batch.py` - Check status and download results

## Concurrent Scripts (Multiple Batch Jobs - Faster!)

For **maximum concurrency and speed** when processing 50+ URLs, use:
- `create_batch_concurrent.py` - Create multiple batch input files
- `submit_batch_concurrent.py` - Submit multiple jobs concurrently
- `check_batch_concurrent.py` - Monitor and download from multiple jobs concurrently

The concurrent scripts split your URLs into multiple batch jobs and process them in parallel, significantly increasing throughput.

## Scripts

### 1. `create_batch.py` - Create Batch Input File

Creates a JSONL input file from your URLs, grouped by batch size.

**Usage:**
```bash
python 2..CallOpenAI/scripts/create_batch.py [options]
```

**Options:**
- `--input-file`: Path to your URLs JSON file (default: `prompts/input/test.json`)
- `--output-file`: Output JSONL file path (default: `batch_input.jsonl`)
- `--batch-size`: URLs per request (default: `5`)
- `--prompt-file`: Prompt file path (default: `prompts/ai-configuration/prompt.txt`)

**Example:**
```bash
python 2..CallOpenAI/scripts/create_batch.py --input-file prompts/input/test.json --output-file batch_input.jsonl --batch-size 5
```

---

### 2. `submit_batch.py` - Submit Batch to OpenAI

Uploads the JSONL file and creates a batch job, saving the job ID.

**Usage:**
```bash
python 2..CallOpenAI/scripts/submit_batch.py [options]
```

**Options:**
- `--input-file`: Batch input JSONL file (default: `batch_input.jsonl`)
- `--job-id-file`: File to save job ID (default: `batch_job_id.txt`)

**Example:**
```bash
python 2..CallOpenAI/scripts/submit_batch.py --input-file batch_input.jsonl --job-id-file batch_job_id.txt
```

---

### 3. `check_batch.py` - Check Status and Download Results

Monitors the batch job and downloads results when completed.

**Usage:**
```bash
python 2..CallOpenAI/scripts/check_batch.py [options]
```

**Options:**
- `--job-id`: Job ID (if not provided, reads from job-id-file)
- `--job-id-file`: File containing job ID (default: `batch_job_id.txt`)
- `--output-file`: Raw results JSONL file (default: `batch_results.jsonl`)
- `--parsed-output-file`: Parsed results JSON file (default: `batch_results_parsed.json`)
- `--poll-interval`: Seconds between status checks (default: `60`)
- `--no-wait`: Check status once and exit (don't wait for completion)
- `--urls-file`: Original URLs file for enrichment (default: `prompts/input/test.json`)
- `--batch-size`: URLs per request (default: `5`)

**Examples:**
```bash
# Wait for completion and download results
python 2..CallOpenAI/scripts/check_batch.py

# Just check current status (don't wait)
python 2..CallOpenAI/scripts/check_batch.py --no-wait

# Use specific job ID
python 2..CallOpenAI/scripts/check_batch.py --job-id batch_xxxxx
```

---

## Complete Workflow Example

```bash
# Step 1: Create batch input file
python 2..CallOpenAI/scripts/create_batch.py --input-file prompts/input/test.json --batch-size 5

# Step 2: Submit to OpenAI
python 2..CallOpenAI/scripts/submit_batch.py --input-file batch_input.jsonl

# Step 3: Check status and download results
python 2..CallOpenAI/scripts/check_batch.py
```

---

## Output Files

All batch-related output files are created inside the `batchfiles/` folder in the project root:

1. `batchfiles/batch_input.jsonl` - Input file with all batch requests
2. `batchfiles/batch_job_id.txt` - Saved job ID for tracking
3. `batchfiles/batch_results.jsonl` - Raw results from OpenAI
4. `batchfiles/batch_results_parsed.json` - Parsed and enriched results

---

## Concurrent Processing (Recommended for 50+ URLs)

For processing 50+ URLs with maximum concurrency:

### 1. `create_batch_concurrent.py` - Create Multiple Batch Files

Creates multiple batch input files that can be processed in parallel.

**Usage:**
```bash
python 2..CallOpenAI/scripts/create_batch_concurrent.py [options]
```

**Options:**
- `--input-file`: Path to your URLs JSON file (default: `prompts/input/test.json`)
- `--batch-size`: URLs per request (default: `5`)
- `--jobs`: Number of concurrent batch jobs to create (default: `5`)
- `--output-prefix`: Prefix for output files (default: `batch_input`)
- `--prompt-file`: Prompt file path

**Example for 50 URLs with 5 concurrent jobs:**
```bash
python 2..CallOpenAI/scripts/create_batch_concurrent.py --input-file prompts/input/test.json --batch-size 10 --jobs 5
```

This creates 5 batch jobs, each processing 10 URLs (50 total).

### 2. `submit_batch_concurrent.py` - Submit Multiple Jobs Concurrently

Uploads all batch files and creates batch jobs concurrently.

**Usage:**
```bash
python 2..CallOpenAI/scripts/submit_batch_concurrent.py [options]
```

**Options:**
- `--manifest`: Path to manifest file (default: `batch_input_manifest.json`)
- `--max-workers`: Maximum concurrent workers (default: `5`)
- `--job-ids-file`: File to save job IDs (default: `batch_job_ids.json`)

**Example:**
```bash
python 2..CallOpenAI/scripts/submit_batch_concurrent.py --manifest batch_input_manifest.json --max-workers 5
```

### 3. `check_batch_concurrent.py` - Monitor Multiple Jobs Concurrently

Monitors all batch jobs concurrently and downloads results when completed.

**Usage:**
```bash
python 2..CallOpenAI/scripts/check_batch_concurrent.py [options]
```

**Options:**
- `--job-ids-file`: File containing job IDs (default: `batch_job_ids.json`)
- `--manifest-file`: Manifest file (default: `batch_input_manifest.json`)
- `--output-file`: Output file for results (default: `batch_results_concurrent.json`)
- `--poll-interval`: Seconds between status checks (default: `60`)
- `--max-workers`: Maximum concurrent workers (default: `5`)
- `--no-wait`: Check status once and exit

**Example:**
```bash
python 2..CallOpenAI/scripts/check_batch_concurrent.py --job-ids-file batch_job_ids.json
```

### Complete Concurrent Workflow Example

```bash
# Step 1: Create multiple batch files (5 jobs, 10 URLs each = 50 URLs total)
python 2..CallOpenAI/scripts/create_batch_concurrent.py --input-file prompts/input/test.json --batch-size 10 --jobs 5

# Step 2: Submit all jobs concurrently
python 2..CallOpenAI/scripts/submit_batch_concurrent.py --manifest batch_input_manifest.json --max-workers 5

# Step 3: Monitor and download results from all jobs
python 2..CallOpenAI/scripts/check_batch_concurrent.py --job-ids-file batch_job_ids.json
```

### Benefits of Concurrent Processing

- **5x faster**: Process 5 batch jobs simultaneously instead of sequentially
- **Better throughput**: Multiple jobs processed in parallel by OpenAI
- **Scalable**: Increase `--jobs` and `--max-workers` for even more concurrency
- **Fault tolerant**: If one job fails, others continue processing

---

## Notes

- All scripts use the project root as the base directory for file paths
- Logs are saved to the `logs/` directory
- Make sure your `.env` file contains `OPENAI_API_KEY`
- The scripts automatically handle path resolution relative to the project root

