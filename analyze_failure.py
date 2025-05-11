from github_utils import get_repo_contents, get_file_contents
from postgres_utils import make_pg_query
from google.genai import Client, types 
from dotenv import load_dotenv
import os
from pydantic import BaseModel

load_dotenv()

client = Client(api_key=os.getenv("GEMINI_API_KEY"))

def get_system_prompt(logs, codebase_structure):

    return f"""You are a helpful assistant that analyzes logs from failed Apache Spark jobs and provides a report on the failure.

    Your final message should be in the following format:
    {Report.report_format()}

    You should analyze the following logs:
    {logs}

    Additionally, you have access to the Spark codebase and the data that was ingested to the pipeline via tool calls. Here's the codebase structure:
    {codebase_structure}
    You may spend some time thinking, but your final message should be in the report format above.
    """

# Logs structure is tbd, ingestion_data is tbd
def analyze_failure(logs, github_repo_owner, github_repo_url, pg_db_url, spark_job_id) -> Report:

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
    config = types.GenerateContentConfig(tools=[tools], response_mime_type="application/json", response_schema=Report)
    contents = [types.Content(role="system", parts=[system_prompt])] 

    # Call Gemini API with system prompt
    response = client.models.generate_content(
        model="gemini-2.5-pro",
        contents=system_prompt,
        config=config,
    )
    
    # Handling function calls
    has_function_calls = response.candidates[0].content.parts.function_call
    while has_function_calls:
        function_call = response.candidates[0].content.parts.function_call

        if function_call.name == "read_github_file":
            result = get_file_contents(github_repo_owner, github_repo_url, **function_call.args)
            contents.append(types.Content(role="system", parts=[f":Called function: {function_call.name} with args {function_call.args} and got result: {result}"]))
        elif function_call.name == "make_pg_query":
            result = make_pg_query(pg_db_url, **function_call.args)
            contents.append(types.Content(role="system", parts=[f":Called function: {function_call.name} with args {function_call.args} and got result: {result}"]))
        else:
            raise ValueError(f"Function {function_call.name} not found")

        response = client.models.generate_content(
        model="gemini-2.5-pro",
        contents=contents,
        config=config,
        )

        has_function_calls = response.candidates[0].content.parts.function_call

    final_message = response.text
    report = Report(final_message, spark_job_id)
    return report

class Report(BaseModel):
    spark_job_id: str
    relevant_logs: str
    relevant_code: str
    hypothesis: str
    suggested_fix: str

    @classmethod
    def report_format():
        return f"""Relevant Logs: Any snippets from the logs that are relevant to diagnosing the error. This should be a few sentences at most.
Relevant Code: Any snippets from the codebase that are relevant to diagnosing the error. This should be a few sentences at most.
Hypothesis: A hypothesis for what the problem is. This should be a paragraph.
Suggested Fix: A suggested fix for the problem. This should be a paragraph, with actionable steps as to how the user can fix the problem.
"""

    def format(self):
        return f"""Diagnosed Error with Spark Job: {self.spark_job_id} \n\n
Relevant Logs: {self.relevant_logs} \n\n
Relevant Code: {self.relevant_code} \n\n
Hypothesis: {self.hypothesis} \n\n
Suggested Fix: {self.suggested_fix}
"""
    
    def parse_final_message(final_message: str):
        pass
    
