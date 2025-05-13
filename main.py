from analyze_failure import analyze_failure
from fastapi import FastAPI, Body 
import os
from dotenv import load_dotenv
from discord_utils import start_discord_bot, send_discord_message
from typing import List, Optional, Dict, Any, Tuple
from pydantic import BaseModel, Field
from datetime import datetime

load_dotenv()

app = FastAPI()

GITHUB_REPO_OWNER = os.getenv("GITHUB_REPO_OWNER")
GITHUB_REPO_URL = os.getenv("GITHUB_REPO_URL")
DISCORD_CHANNEL_ID = os.getenv("DISCORD_CHANNEL_ID")
INGESTION_DATA = os.getenv("INGESTION_DATINGESTION_DATAA")
PG_DB_URL = os.getenv("PG_DB_URL")

start_discord_bot()

class AppAttempt(BaseModel):
    startTime: str
    endTime: str
    lastUpdated: str
    duration: int
    sparkUser: str
    completed: bool
    appSparkVersion: str
    startTimeEpoch: int
    endTimeEpoch: int
    lastUpdatedEpoch: int

class AppInfo(BaseModel):
    id: str
    name: str
    attempts: List[AppAttempt]

class Job(BaseModel):
    jobId: int
    name: str
    submissionTime: str
    completionTime: str
    stageIds: List[int]
    jobTags: List[str]
    status: str
    numTasks: int
    numFailedTasks: int
    numKilledTasks: int
    numCompletedTasks: int
    killedTasksSummary: Dict[str, Any]

class Stage(BaseModel):
    status: str
    stageId: int
    attemptId: int
    numTasks: int
    numFailedTasks: int
    failureReason: str

class Executor(BaseModel):
    id: str
    hostPort: str
    isActive: bool
    totalCores: int
    failedTasks: int
    completedTasks: int

class ResourceMetrics(BaseModel):
    resourceName: str
    amount: float

class ResourceProfile(BaseModel):
    id: int
    executorResources: Dict[str, ResourceMetrics]
    taskResources: Dict[str, ResourceMetrics]

class Environment(BaseModel):
    runtime: Dict[str, str]
    sparkProperties: List[Tuple[str, str]]
    hadoopProperties: List[Tuple[str, str]]
    systemProperties: List[Tuple[str, str]]
    metricsProperties: List[Tuple[str, str]]
    classpathEntries: List[Tuple[str, str]]
    resourceProfiles: List[ResourceProfile]

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
    failure_reason: str

class TaskFailure(BaseModel):
    stage_id: int
    task_id: int
    attempt: int
    status: str
    error_message: str
    executor_id: str

class AppState(BaseModel):
    app_id: str
    timestamp: float
    app_info: AppInfo
    jobs: List[Job]
    stages: List[Stage]
    executors: List[Executor]
    environment: Environment
    
    # Failure summary
    has_failed_jobs: bool
    failed_jobs_details: List[FailedJobDetail]
    has_failed_stages: bool
    failed_stages_details: List[FailedStageDetail]
    task_failure_count: int
    task_failures: List[TaskFailure]
    executor_failure_count: int
    has_executor_failures: bool
    has_failures: bool
    is_completed: bool
    hash: str

class ApplicationStateChange(BaseModel):
    message: str
    event_type: str
    app_state: AppState


@app.post("/webhook/analyze_failure")
async def handle_app_state_webhook(
    payload: ApplicationStateChange = Body(...)
):
    # only process the correct event
    if payload.event_type != "application_state_change":
        return {"status": "error", "message": "Invalid event type"}

    f = payload.app_state

    # nothing to do if no failures
    if not f.has_failures:
        return {"status": "ok", "message": "No failures detected"}

    # human‐readable timestamp
    readable_ts = datetime.fromtimestamp(f.timestamp).isoformat()

    # start building up the logs
    logs = [
        f"Event Type: {payload.event_type}",
        f"Message: {payload.message}",
        f"App ID: {f.app_id}",
        f"App Name: {f.app_info.name}",
        f"Timestamp: {f.timestamp} ({readable_ts})",
        f"Completed: {f.is_completed}",
    ]

    # job failures
    if f.has_failed_jobs:
        logs.append("\nJob Failures:")
        for job in f.failed_jobs_details:
            logs.append(
                f"  • Job {job.job_id} — {job.name} — {job.status} — "
                f"{job.failure_reason or 'N/A'}"
            )

    # stage failures
    if f.has_failed_stages:
        logs.append("\nStage Failures:")
        for stage in f.failed_stages_details:
            logs.append(
                f"  • Stage {stage.stage_id}.{stage.attempt_id} — "
                f"{stage.name} — {stage.status} — "
                f"{stage.failure_reason}"
            )

    # task failures (take just the first line of each error)
    if f.task_failure_count:
        logs.append(f"\nTask Failures ({f.task_failure_count}):")
        for t in f.task_failures:
            summary = t.error_message.split("\n", 1)[0] if t.error_message else "N/A"
            logs.append(
                f"  • Task {t.stage_id}.{t.task_id}.{t.attempt} on "
                f"{t.executor_id} — {t.status} — {summary}"
            )

    # executor‐failure count
    if f.has_executor_failures:
        logs.append(f"\nExecutor Failures Count: {f.executor_failure_count}")

    # join them all into one string
    log_text = "\n".join(logs)

    # hand it off
    report = analyze_failure(log_text, GITHUB_REPO_OWNER, GITHUB_REPO_URL, PG_DB_URL, f.app_id)
    send_discord_message(report, DISCORD_CHANNEL_ID)

    return {"status": "ok"}
    
