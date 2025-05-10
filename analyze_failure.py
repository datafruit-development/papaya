from github_utils import get_repo_contents, get_file_contents
from google.genai import Client, types 
from dotenv import load_dotenv
import os

load_dotenv()

client = Client(api_key=os.getenv("GEMINI_API_KEY"))

def get_system_prompt(logs, codebase_structure):

    return f"""You are a helpful assistant that analyzes logs from failed Apache Spark jobs and provides a report on the failure.

    Your final message should be in the following format:
    {Report.format()}

    You should analyze the following logs:
    {logs}

    Additionally, you have access to the Spark codebase and the data that was ingested to the pipeline via tool calls. Here's the codebase structure:
    {codebase_structure}
    You may spend some time thinking, but your final message should be in the report format above.
    """

# Logs structure is tbd, ingestion_data is tbd
def analyze_failure(logs, github_repo_owner, github_repo_url, discord_channel_id, ingestion_data) -> Report:

    codebase_structure = get_repo_contents(github_repo_owner, github_repo_url)

    system_prompt = get_system_prompt(logs, codebase_structure)

    tool_definitions = [{
        "name": "read_github_file",
        "description": "Returns the contents of a file from the Github repository.",
        "parameters": {
            "type": "object",
            "properties": {
                "filename": {
                    "type": "string",
                    "description": "Name of the file to read.",
                },
            },
            "required": ["filename"],
        },
    }]
    
    tools = types.Tool(function_declarations=tool_definitions)
    config = types.GenerateContentConfig(tools=[tools])
    
    # Call Gemini API with system prompt
    response = client.models.generate_content(
        model="gemini-2.5-pro",
        contents=system_prompt,
        config=config,
    )
    final_message = response.text

    report = Report(final_message)

    return report



class Report:
    def __init__(self, final_message: str):
        self.final_message = final_message
        self.logs, self.code, self.ingestion_data, self.hypothesis, self.suggested_fix = self.parse_final_message(final_message)
    
    def parse_final_message(self, final_message: str):
        pass

    def format():
        return f"""
        """