from analyze_failure import analyze_failure
from fastapi import FastAPI, Request
import os
from dotenv import load_dotenv
from discord_utils import send_discord_message
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

load_dotenv()

app = FastAPI()

GITHUB_REPO_OWNER = os.getenv("GITHUB_REPO_OWNER")
GITHUB_REPO_URL = os.getenv("GITHUB_REPO_URL")
DISCORD_CHANNEL_ID = os.getenv("DISCORD_CHANNEL_ID")
INGESTION_DATA = os.getenv("INGESTION_DATA")
PG_DB_URL = os.getenv("PG_DB_URL")

class FailedJobDetail(BaseModel):
    job_id: int
    name: str
    status: str
    failure_reason: Optional[str] = None

class FailedStageDetail(BaseModel):
    stage_id: int
    attempt_id: int
    name: str
    status: str
    failure_reason: Optional[str] = None
    exception: Optional[str] = None

class TaskFailure(BaseModel):
    stage_id: int
    task_id: int
    attempt: int
    status: str
    error_message: Optional[str] = None
    has_exception: bool
    executor_id: str

class FailureInfo(BaseModel):
    app_id: str
    app_name: str
    timestamp: float
    has_failures: bool
    is_completed: bool
    has_failed_jobs: bool
    failed_jobs_details: List[FailedJobDetail]
    has_failed_stages: bool
    failed_stages_details: List[FailedStageDetail]
    task_failure_count: int
    task_failures: List[TaskFailure]
    executor_failure_count: int
    has_executor_failures: bool

class ApplicationStateChange(BaseModel):
    event_type: str
    is_new_app: bool
    failures: FailureInfo

def extract_error_summary(error_message: str) -> str:
    """Extract the main Python error from a full stack trace."""
    if not error_message:
        return "Unknown error"
    
    # Look for Python traceback
    lines = error_message.split('\n')
    for line in lines:
        if any(err in line for err in ['Error:', 'Exception:']):
            return line.strip()
    return lines[0].strip()

@app.post("/analyze_failure")
async def analyze_failure_endpoint(request: Request):
    data = await request.json()
    logs = data["logs"]

    report = analyze_failure(logs, GITHUB_REPO_OWNER, GITHUB_REPO_URL, PG_DB_URL)
    await send_discord_message(report.to_string(), DISCORD_CHANNEL_ID)

@app.post("/webhook/app_state")
async def handle_app_state_webhook(payload: ApplicationStateChange):
    if payload.event_type != "application_state_change":
        return {"status": "error", "message": "Invalid event type"}
    
    if payload.failures.has_failures:
        # Extract all variables from payload
        event_type = payload.event_type
        is_new_app = payload.is_new_app
        
        # Application info
        app_id = payload.failures.app_id
        app_name = payload.failures.app_name
        timestamp = payload.failures.timestamp
        is_completed = payload.failures.is_completed
        
        # Job failures
        has_failed_jobs = payload.failures.has_failed_jobs
        failed_jobs_details = payload.failures.failed_jobs_details
        
        # Stage failures
        has_failed_stages = payload.failures.has_failed_stages
        failed_stages_details = payload.failures.failed_stages_details
        
        # Task failures
        task_failure_count = payload.failures.task_failure_count
        task_failures = payload.failures.task_failures
        
        # Executor failures
        executor_failure_count = payload.failures.executor_failure_count
        has_executor_failures = payload.failures.has_executor_failures


        # Convert timestamp to human-readable form
        readable_ts = datetime.fromtimestamp(timestamp).isoformat()

        # Build up log lines
        log_lines = [
            f"Event Type: {event_type}",
            f"New App: {is_new_app}",
            f"App ID: {app_id}",
            f"App Name: {app_name}",
            f"Timestamp: {timestamp} ({readable_ts})",
            f"Completed: {is_completed}",
        ]

        # Include job failures
        if has_failed_jobs and failed_jobs_details:
            log_lines.append("\nJob Failures:")
            for job in failed_jobs_details:
                log_lines.append(
                    f"  • Job {job['job_id']} — {job['name']} | Status: {job['status']} | "
                    f"Reason: {job.get('failure_reason', 'N/A')}"
                )

        # Include stage failures
        if has_failed_stages and failed_stages_details:
            log_lines.append("\nStage Failures:")
            for stage in failed_stages_details:
                log_lines.append(
                    f"  • Stage {stage['stage_id']} (attempt {stage['attempt_id']}) — {stage['name']} | "
                    f"Status: {stage['status']} | Reason: {stage.get('failure_reason', 'N/A')}"
                )

        # Include task failures (cap error message length for brevity)
        log_lines.append(f"\nTotal Task Failures: {task_failure_count}")
        for task in task_failures:
            err = task.get('error_message', '').replace("\n", " ")
            truncated_err = (err[:200] + '…') if len(err) > 200 else err
            log_lines.append(
                f"  • Stage {task['stage_id']} Task {task['task_id']} (attempt {task['attempt']}) | "
                f"Executor: {task['executor_id']} | Error: {truncated_err}"
            )

        # Include executor failure summary
        log_lines.append(f"\nExecutor Failures: {executor_failure_count}")
        log_lines.append(f"Has Executor Failures: {has_executor_failures}")

        # Final log string to pass to the LLM
        log_string = "\n".join(log_lines)

        report = analyze_failure(log_string, GITHUB_REPO_OWNER, GITHUB_REPO_URL, PG_DB_URL, app_id)
        await send_discord_message(report.to_string(), DISCORD_CHANNEL_ID)

    return {"status": "success", "message": "Webhook processed successfully"}
    
