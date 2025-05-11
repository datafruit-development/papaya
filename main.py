from analyze_failure import analyze_failure
from fastapi import FastAPI, Request
import os
from dotenv import load_dotenv
from discord_utils import send_discord_message

load_dotenv()

app = FastAPI()

GITHUB_REPO_OWNER = os.getenv("GITHUB_REPO_OWNER")
GITHUB_REPO_URL = os.getenv("GITHUB_REPO_URL")
DISCORD_CHANNEL_ID = os.getenv("DISCORD_CHANNEL_ID")
INGESTION_DATA = os.getenv("INGESTION_DATA")
PG_DB_URL = os.getenv("PG_DB_URL")

@app.post("/analyze_failure")
async def analyze_failure_endpoint(request: Request):
    data = await request.json()
    logs = data["logs"]

    report = analyze_failure(logs, GITHUB_REPO_OWNER, GITHUB_REPO_URL, PG_DB_URL)
    await send_discord_message(report.to_string(), DISCORD_CHANNEL_ID)
    
