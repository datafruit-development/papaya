from github_utils import get_repo_contents, get_file_contents
from google.generativeai import GenerativeAI
from dotenv import load_dotenv

load_dotenv()

genai = GenerativeAI()

def sys_prompt(logs, codebase_structure):

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

    sys_prompt = sys_prompt(logs, codebase_structure)

    # Call Gemini API with system prompt
    response = genai.generate_text(
        model="gemini-2.5-pro",
        prompt=sys_prompt,
        temperature=0.3,
        max_output_tokens=2048
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