import json
from pathlib import Path

import tiktoken


def count_tokens_in_file(file_path, model="gpt-4o"):
    """
    Counts tokens in a JSONL file for a specific model encoding.
    """
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        print("Warning: Model not found. Using cl100k_base encoding.")
        encoding = tiktoken.get_encoding("cl100k_base")

    total_tokens = 0
    line_count = 0

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line_count += 1
                # Parse the JSON line to extract the content you are actually sending
                # If your file is raw text, just use: content = line
                try:
                    data = json.loads(line)
                    # We convert the whole JSON object to a string to estimate the overhead
                    # or you can target specific fields like data['body']['messages']
                    content = json.dumps(data) 
                except json.JSONDecodeError:
                    content = line # Fallback for non-JSON files

                tokens = len(encoding.encode(content))
                total_tokens += tokens
                
        print(f"✅ Scanning Complete.")
        print(f"Total Lines: {line_count}")
        print(f"Total Tokens: {total_tokens:,}")
        print(f"model: {model}")
        return total_tokens

    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return 0
    except Exception as e:
        print(f"Error while reading '{file_path}': {e}")
        return 0


if __name__ == "__main__":
    """
    Default usage: count tokens for the latest main batch file.
    Assumes the batch input file lives in 2..CallOpenAI/batchfiles/.
    """
    # helpers/ -> scripts/ -> 2..CallOpenAI/
    project_root = Path(__file__).resolve().parents[2]
    file_path = project_root / "batchfiles" / "batch_input_0.jsonl"
    count_tokens_in_file(str(file_path), model="gpt-4.1-mini")