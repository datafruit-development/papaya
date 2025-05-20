from github_utils import get_repo_contents, get_file_contents
from postgres_utils import make_pg_query
from google.genai import Client, types 
from dotenv import load_dotenv
import os
from pydantic import BaseModel

load_dotenv()

MODEL = "gemini-2.5-pro-preview-03-25"
client = Client(api_key=os.getenv("GEMINI_API_KEY"))

class Report(BaseModel):
    spark_job_id: str
    relevant_logs: str
    relevant_code: str
    hypothesis: str
    suggested_fix: str

    @staticmethod
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
    # Handle missing GitHub repo gracefully
    if not github_repo_owner or not github_repo_url:
        codebase_structure = "[GitHub repository not provided; codebase structure unavailable.]"
        tool_definitions = []
    else:
        codebase_structure = get_repo_contents(github_repo_owner, github_repo_url)
        tool_definitions = [{
            "name": "read_github_file",
            "description": "Returns the contents of a file from the Github repository.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path to the file to read.",
                    },
                },
                "required": ["path"],
            },
        }]

    system_prompt = get_system_prompt(logs, codebase_structure)

    tools = types.Tool(function_declarations=tool_definitions) if tool_definitions else None
    config = types.GenerateContentConfig(tools=[tools], response_schema=Report) if tools else types.GenerateContentConfig(response_schema=Report)
    contents = [
        types.Content(role="user", parts=[{"text": system_prompt}])
    ]

    # Call Gemini API with system prompt
    response = client.models.generate_content(
        model=MODEL,
        contents=contents,
        config=config,
    )


    # Only process function calls if tools are defined
    while hasattr(response, 'function_calls') and response.function_calls and tool_definitions:
        # Handle each function call
        for function_call in response.function_calls:
            if function_call.name == "read_github_file":
                result = get_file_contents(github_repo_owner, github_repo_url, **function_call.args)
                contents.append(types.Content(role="model", parts=[{"text": f"Called function: {function_call.name} with args {function_call.args} and got result: {result}"}]))
                contents.append(types.Content(role="user", parts=[{"text": "Please continue with the analysis and provide your final response in the structured format specified, without any additional text."}]))
            # elif function_call.name == "make_pg_query":
            #     result = make_pg_query(pg_db_url, **function_call.args)
            #     contents.append(types.Content(role="system", parts=[{"text": f"Called function: {function_call.name} with args {function_call.args} and got result: {result}"}]))
            else:
                raise ValueError(f"Function {function_call.name} not found")

        # Get the next response
        response = client.models.generate_content(
            model=MODEL,
            contents=contents,
            config=config,
        )

    return response.text 


if __name__ == "__main__":
    report = analyze_failure(
        logs="This is a test. Make up a fake report, and call the function you have access to.",
        github_repo_owner="",
        github_repo_url="",
        pg_db_url="",
        spark_job_id=""
    )

    print(report)