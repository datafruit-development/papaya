import os
from google.genai import Client, types
from typing import Dict, List
from papaya.github_utils import make_code_change, get_repo_contents, get_file_contents
from dotenv import load_dotenv
import json

load_dotenv()

MODEL = "gemini-2.5-pro-preview-05-06"
client = Client(api_key=os.getenv("GEMINI_API_KEY"))

def gather_codebase_contents(repo_owner: str, repo_name: str, path: str = "") -> str:
    """
    Recursively gathers all file contents from a repository and formats them for the LLM.

    Args:
        repo_owner: The owner of the repository
        repo_name: The name of the repository
        path: Current path in the repository

    Returns:
        str: Formatted string containing all file contents
    """
    contents = get_repo_contents(repo_owner, repo_name, path)
    result = []

    for item in contents:
        if item.type == "dir":
            # Recursively get contents of subdirectories
            result.append(gather_codebase_contents(repo_owner, repo_name, item.path))
        elif item.type == "file":
            # Skip binary files and very large files
            if item.size > 1000000:  # Skip files larger than ~1MB
                result.append(f"File {item.path} (size: {item.size} bytes) - too large to include")
                continue

            # Get file extension
            _, ext = os.path.splitext(item.path)
            if ext.lower() in ['.png', '.jpg', '.jpeg', '.gif', '.pdf', '.zip', '.exe']:
                result.append(f"File {item.path} - binary file skipped")
                continue

            try:
                file_content = get_file_contents(repo_owner, repo_name, item.path)
                decoded_content = file_content.decoded_content.decode('utf-8')
                result.append(f"File: {item.path}\n```\n{decoded_content}\n```\n")
            except UnicodeDecodeError:
                result.append(f"File {item.path} - binary or non-utf8 file skipped")
            except Exception as e:
                result.append(f"Error reading {item.path}: {str(e)}")

    return "\n".join(result)

def repair_code(repo_owner: str, repo_name: str, report: str):
    """
    Creates a pull request that fixes the problem described in the report.

    Args:
        repo_owner: The owner of the repository
        repo_name: The name of the repository
        report: The incident report containing details about the issue

    Returns:
        str: The URL of the created pull request
    """
    # Initialize Gemini model

    codebase = gather_codebase_contents(repo_owner, repo_name)

    system_prompt = f"""You are an expert software engineer, that has tasked with fixing an issue in a codebase.

This issue has been diagnosed by an AI, and you are given a report on the issue.

Your job is to fix the issue, and create a pull request with the changes.

The report is as follows:

{report}

And here is the codebase:

{codebase}

You should fix the issue, and create a pull request with the changes.

You should respond with the following format:

```
{{
    "changed_filepath_1": "The complete new content for the first file to change",
    "changed_filepath_2": "The complete new content for the second file to change",
}}
```
"""

    contents = [
        types.Content(role="user", parts=[{"text": system_prompt}])
    ]

    response = client.models.generate_content(model=MODEL, contents=contents,
                                              config={"response_mime_type": "application/json"})

    try:
        json_response = json.loads(response.text)
        return make_code_change(repo_owner, repo_name, json_response, pr_body=report)
    except Exception as e:
        print("Failed to parse json response from Code Repair agent. defaulting to updating nothing.")
        return None



if __name__ == "__main__":
    response = repair_code("lychee-development", "spark_debugger_demo", "the whole coebase is broken. ")

    print("Response: ", response)
