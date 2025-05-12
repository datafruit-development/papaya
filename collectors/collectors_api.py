#!/usr/bin/env python3
# collectors_api.py - Streamlined API for Spark Pipeline Debugger with failure monitoring

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Any, Optional
import os
import json
import logging
import time
import asyncio
import requests
from datetime import datetime

# Import the collectors
from base_collector import BaseCollector
from sparkmeasure_collector import SparkMeasureCollector
from httpendpoints_collector import SparkHttpCollector
from custom_collector import CustomLoggingCollector
from os_metrics_collector import OSMetricsCollector
from custom_app_metrics_collector import CustomAppMetricsCollector
from external_metrics_collector import ExternalMetricsCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("spark_debugger_api")

# Create FastAPI app
app = FastAPI(
    title="Spark Pipeline Debugger API",
    description="REST API for monitoring and debugging Spark applications",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global settings
settings = {
    "spark_ui_url": os.environ.get("SPARK_UI_URL", "http://localhost:4040"),
    "log_dir": os.environ.get("SPARK_LOG_DIR", "/tmp/spark-logs"),
    "collection_interval": int(os.environ.get("COLLECTION_INTERVAL", "60")),  # seconds
    "failure_check_interval": int(os.environ.get("FAILURE_CHECK_INTERVAL", "30")),  # seconds
    "discord_webhook_url": os.environ.get("DISCORD_WEBHOOK_URL", ""),
    "webhook_url": os.environ.get("WEBHOOK_URL", ""),
    "webhook_auth_token": os.environ.get("WEBHOOK_AUTH_TOKEN", ""),
    "webhook_enabled": os.environ.get("WEBHOOK_ENABLED", "true").lower() == "true",
}

# Collector instances
http_collector = None
logging_collector = None
sparkmeasure_collector = None

# Failure tracking
failure_history = []
MAX_FAILURE_HISTORY = 100

# Pydantic models for API
class FailureAlert(BaseModel):
    failure_type: str
    details: Dict[str, Any]
    severity: str = "high"


# Initialize collectors
def initialize_collectors():
    """Initialize the core collector instances needed for failure monitoring"""
    global http_collector, logging_collector, sparkmeasure_collector
    
    try:
        # Initialize HTTP collector for job status monitoring
        http_collector = SparkHttpCollector(base_url=settings["spark_ui_url"])
        
        # Initialize custom logging collector for exception detection
        logging_collector = CustomLoggingCollector(log_dir=settings["log_dir"])
        
        # Initialize SparkMeasure collector for performance metrics
        sparkmeasure_collector = SparkMeasureCollector(mode="stage")
        
        logger.info("Core collectors initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing collectors: {str(e)}")
        raise

async def send_webhook_notification(failure_type: str, details: Dict[str, Any]):
    """Send a webhook notification to an external system when a failure is detected.
    
    Args:
        failure_type: Type of failure (e.g., "job_failure", "exception")
        details: Dictionary with failure details
    
    Returns:
        True if notification was sent successfully, False otherwise
    """
    if not settings["webhook_enabled"] or not settings["webhook_url"]:
        logger.debug("Webhook notifications not enabled or URL not configured")
        return False
    
    try:
        # Prepare webhook payload
        payload = {
            "event_type": "spark_failure",
            "failure_type": failure_type,
            "timestamp": time.time(),
            "details": details
        }
        
        # Prepare headers
        headers = {
            "Content-Type": "application/json"
        }
        
        # Add auth token if provided
        if settings["webhook_auth_token"]:
            headers["Authorization"] = f"Bearer {settings['webhook_auth_token']}"
        
        # Send webhook request
        response = requests.post(
            settings["webhook_url"],
            json=payload,
            headers=headers,
            timeout=10  # 10 second timeout
        )
        
        # Check response
        if response.status_code >= 200 and response.status_code < 300:
            logger.info(f"Webhook notification sent for {failure_type} (status: {response.status_code})")
            return True
        else:
            logger.error(f"Failed to send webhook notification: {response.status_code} {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error sending webhook notification: {str(e)}")
        return False
    
# Discord notification function This needs updating bc we aren't using Webhooks for this -- bot
async def send_discord_alert(failure_type: str, details: Dict[str, Any], severity: str = "high"):
    """Send a failure alert to Discord webhook"""
    if not settings["discord_webhook_url"]:
        logger.warning("Discord webhook URL not configured, skipping alert")
        return False
    
    try:
        # Create webhook payload
        payload = {
            "embeds": [{
                "title": f"❌ Spark {failure_type.replace('_', ' ').title()} Detected",
                "color": 0xFF0000,  # Red
                "description": "A failure has been detected in your Spark job.",
                "fields": []
            }]
        }
        
        # Add fields based on failure type
        embed = payload["embeds"][0]
        
        if failure_type == "job_failure":
            job = details.get("failed_jobs", [{}])[0] if details.get("failed_jobs") else {}
            
            embed["fields"].extend([
                {"name": "Application ID", "value": job.get("app_id", "Unknown"), "inline": True},
                {"name": "Job ID", "value": job.get("job_id", "Unknown"), "inline": True},
                {"name": "Job Name", "value": job.get("name", "Unknown"), "inline": True}
            ])
            
            if job.get("failure_reason"):
                embed["fields"].append({
                    "name": "Failure Reason", 
                    "value": f"```{job.get('failure_reason')[:1000]}```", 
                    "inline": False
                })
                
        elif failure_type == "exception":
            exception = details.get("exceptions", [{}])[0] if details.get("exceptions") else {}
            
            embed["fields"].extend([
                {"name": "Exception Type", "value": exception.get("type", "Unknown"), "inline": True}
            ])
            
            if exception.get("message"):
                embed["fields"].append({
                    "name": "Message", 
                    "value": f"```{exception.get('message', '')[:1000]}```", 
                    "inline": False
                })
        
        # Add timestamp
        embed["timestamp"] = datetime.utcnow().isoformat()
        
        # Send webhook
        response = requests.post(settings["discord_webhook_url"], json=payload)
        
        if response.status_code >= 400:
            logger.error(f"Discord webhook error: {response.status_code} {response.text}")
            return False
            
        logger.info(f"Discord alert sent for {failure_type}")
        return True
        
    except Exception as e:
        logger.error(f"Error sending Discord alert: {str(e)}")
        return False

# Store failure in history
def record_failure(failure_type: str, details: Dict[str, Any]):
    """Record a failure in the failure history"""
    global failure_history
    
    # Create failure record
    failure = {
        "timestamp": time.time(),
        "failure_type": failure_type,
        "details": details
    }
    
    # Add to history (most recent first)
    failure_history.insert(0, failure)
    
    # Trim history if needed
    if len(failure_history) > MAX_FAILURE_HISTORY:
        failure_history = failure_history[:MAX_FAILURE_HISTORY]
    
    logger.info(f"Recorded {failure_type} in failure history")

# Main failure monitoring function
async def monitor_for_failures():
    """Continuously monitor for failures using collectors"""
    logger.info("Starting failure monitoring loop")
    
    while True:
        try:
            # Check for job failures using HTTP collector
            if http_collector:
                job_failures = http_collector.check_for_failures()
                if job_failures:
                    logger.warning(f"Job failure detected: {job_failures['num_failed_jobs']} failed jobs")
                    
                    # Record the failure
                    record_failure("job_failure", job_failures)
                    
                    # Send notifications
                    notifications_tasks = []
                    
                    # Discord notification (if enabled)
                    if settings.get("discord_webhook_url"):
                        notifications_tasks.append(send_discord_alert("job_failure", job_failures))
                    
                    # Webhook notification (if enabled)
                    if settings.get("webhook_enabled") and settings.get("webhook_url"):
                        notifications_tasks.append(send_webhook_notification("job_failure", job_failures))
                    
                    # Wait for all notifications to complete
                    if notifications_tasks:
                        await asyncio.gather(*notifications_tasks)
            
            # Check for critical exceptions in logs
            if logging_collector:
                exceptions = logging_collector.check_for_critical_exceptions()
                if exceptions:
                    logger.warning(f"Critical exceptions detected: {exceptions['num_critical_exceptions']} exceptions")
                    
                    # Record the failure
                    record_failure("exception", exceptions)
                    
                    # Send notifications
                    notifications_tasks = []
                    
                    # Discord notification (if enabled)
                    if settings.get("discord_webhook_url"):
                        notifications_tasks.append(send_discord_alert("exception", exceptions))
                    
                    # Webhook notification (if enabled)
                    if settings.get("webhook_enabled") and settings.get("webhook_url"):
                        notifications_tasks.append(send_webhook_notification("exception", exceptions))
                    
                    # Wait for all notifications to complete
                    if notifications_tasks:
                        await asyncio.gather(*notifications_tasks)
                    
        except Exception as e:
            logger.error(f"Error in failure monitoring loop: {str(e)}")
        
        # Wait before checking again
        await asyncio.sleep(settings["failure_check_interval"])
# API routes
@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "name": "Spark Pipeline Debugger API",
        "version": "1.0.0",
        "status": "running",
        "collectors": {
            "http": http_collector is not None,
            "logging": logging_collector is not None,
            "sparkmeasure": sparkmeasure_collector is not None
        },
        "failure_monitoring": {
            "enabled": True,
            "check_interval": settings["failure_check_interval"]
        }
    }

@app.get("/api/v1/failures")
async def get_failures(limit: int = 10):
    """Get recent failures"""
    return {
        "count": min(len(failure_history), limit),
        "failures": failure_history[:limit]
    }

@app.post("/api/v1/check-failures")
async def trigger_failure_check(background_tasks: BackgroundTasks):
    """Manually trigger a failure check"""
    results = {
        "timestamp": time.time(),
        "checks_performed": [],
        "failures_detected": False
    }
    
    # Check for job failures
    if http_collector:
        job_failures = http_collector.check_for_failures()
        results["checks_performed"].append("job_failures")
        
        if job_failures:
            results["failures_detected"] = True
            results["job_failures"] = job_failures
            
            # Trigger alert and record failure in background
            background_tasks.add_task(send_discord_alert, "job_failure", job_failures)
            record_failure("job_failure", job_failures)
    
    # Check for critical exceptions
    if logging_collector:
        exceptions = logging_collector.check_for_critical_exceptions()
        results["checks_performed"].append("critical_exceptions")
        
        if exceptions:
            results["failures_detected"] = True
            results["critical_exceptions"] = exceptions
            
            # Trigger alert and record failure in background
            background_tasks.add_task(send_discord_alert, "exception", exceptions)
            record_failure("exception", exceptions)
    
    return results

@app.post("/api/v1/send-alert")
async def send_alert(alert: FailureAlert):
    """Manually send a failure alert"""
    success = await send_discord_alert(alert.failure_type, alert.details, alert.severity)
    
    if success:
        return {"status": "success", "message": "Alert sent successfully"}
    else:
        raise HTTPException(status_code=500, detail="Failed to send alert")

# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Initialize collectors and start monitoring on startup"""
    # Initialize collectors
    initialize_collectors()
    
    # Start failure monitoring loop
    asyncio.create_task(monitor_for_failures())
    
    logger.info("API server started with failure monitoring enabled")

# Run the API server
if __name__ == "__main__":
    import uvicorn
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Spark Pipeline Debugger API")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    parser.add_argument("--spark-ui-url", type=str, help="Spark UI URL (default: http://localhost:4040)")
    parser.add_argument("--log-dir", type=str, help="Spark log directory")
    parser.add_argument("--discord-webhook", type=str, help="Discord webhook URL for alerts")
    args = parser.parse_args()
    
    # Update settings
    if args.spark_ui_url:
        settings["spark_ui_url"] = args.spark_ui_url
    if args.log_dir:
        settings["log_dir"] = args.log_dir
    if args.discord_webhook:
        settings["discord_webhook_url"] = args.discord_webhook
    
    # Start the server
    uvicorn.run(app, host=args.host, port=args.port)