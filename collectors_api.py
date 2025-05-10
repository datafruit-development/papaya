#!/usr/bin/env python3
# comprehensive_api.py - Comprehensive REST API for Spark Pipeline Debugger

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Depends, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional, Union
import os
import json
import logging
import time
import asyncio
from datetime import datetime
import uvicorn

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
    title="Comprehensive Spark Pipeline Debugger API",
    description="REST API for monitoring and debugging Spark applications",
    version="1.0.0"
)

# Add CORS middleware to allow cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global cache for metrics
metrics_cache = {
    "sparkmeasure": {},
    "http": {},
    "logging": {},
    "os_metrics": {},
    "app_metrics": {},
    "external_metrics": {},
    "last_updated": 0
}

# Global settings
settings = {
    "spark_ui_url": os.environ.get("SPARK_UI_URL", "http://localhost:4040"),
    "log_dir": os.environ.get("SPARK_LOG_DIR", "/tmp/spark-logs"),
    "cache_ttl": int(os.environ.get("CACHE_TTL", "60")),  # seconds
    "collection_interval": int(os.environ.get("COLLECTION_INTERVAL", "30")),  # seconds
    "monitoring_enabled": True,
    "os_metrics_enabled": True,
    "app_metrics_enabled": True,
    "external_metrics_enabled": True,
    "prometheus_url": os.environ.get("PROMETHEUS_URL", "http://localhost:9090"),
    "custom_metrics_path": os.environ.get("CUSTOM_METRICS_PATH", None),
    "cloudwatch_region": os.environ.get("AWS_REGION", None),
    "ganglia_host": os.environ.get("GANGLIA_HOST", None)
}

# Create collector instances
sparkmeasure_collector = None
http_collector = None
logging_collector = None
os_metrics_collector = None
app_metrics_collector = None
external_metrics_collector = None

# Pydantic models for API requests and responses
class ApplicationInfo(BaseModel):
    id: str
    name: str
    status: str
    startTime: Optional[int] = None
    endTime: Optional[int] = None
    duration: Optional[int] = None
    user: Optional[str] = None

class JobInfo(BaseModel):
    id: str
    name: str
    status: str
    startTime: Optional[int] = None
    endTime: Optional[int] = None
    duration: Optional[int] = None
    numTasks: Optional[int] = None
    numActiveTasks: Optional[int] = None
    numCompletedTasks: Optional[int] = None
    numFailedTasks: Optional[int] = None

class StageInfo(BaseModel):
    id: str
    attemptId: int
    name: str
    status: str
    numTasks: Optional[int] = None
    executorRunTime: Optional[int] = None
    inputBytes: Optional[int] = None
    outputBytes: Optional[int] = None
    shuffleReadBytes: Optional[int] = None
    shuffleWriteBytes: Optional[int] = None
    memoryBytesSpilled: Optional[int] = None
    diskBytesSpilled: Optional[int] = None
    failureReason: Optional[str] = None

class ExceptionInfo(BaseModel):
    type: str
    message: str
    timestamp: int
    stackTrace: Optional[List[str]] = None

class SettingsUpdate(BaseModel):
    spark_ui_url: Optional[str] = None
    log_dir: Optional[str] = None
    cache_ttl: Optional[int] = None
    collection_interval: Optional[int] = None
    monitoring_enabled: Optional[bool] = None
    os_metrics_enabled: Optional[bool] = None
    app_metrics_enabled: Optional[bool] = None
    external_metrics_enabled: Optional[bool] = None
    prometheus_url: Optional[str] = None
    custom_metrics_path: Optional[str] = None
    cloudwatch_region: Optional[str] = None
    ganglia_host: Optional[str] = None
    external_system_type: Optional[str] = None

class PerformanceMetrics(BaseModel):
    dataSkew: Optional[float] = None
    gcPressure: Optional[float] = None
    shuffleIntensity: Optional[float] = None
    memorySpill: Optional[Dict[str, Any]] = None
    skewLevel: Optional[str] = None
    gcLevel: Optional[str] = None
    shuffleLevel: Optional[str] = None
    spillLevel: Optional[str] = None

class DataQualityConfig(BaseModel):
    table_name: Optional[str] = None
    columns: Optional[List[str]] = None
    rules: Optional[List[Dict[str, Any]]] = None

class ExternalMetricsConfig(BaseModel):
    system_type: str = Field(..., description="Type of external system (cloudwatch, ganglia, grafana, prometheus)")
    config: Dict[str, Any] = Field(..., description="Configuration for the external system")
    queries: Optional[List[Dict[str, Any]]] = None

class OSMetricsConfig(BaseModel):
    prometheus_url: Optional[str] = None
    node_hostnames: Optional[List[str]] = None

# Initialization and monitoring functions
def initialize_collectors():
    """Initialize the collector instances"""
    global sparkmeasure_collector, http_collector, logging_collector
    global os_metrics_collector, app_metrics_collector, external_metrics_collector
    
    try:
        # Initialize SparkMeasure collector
        sparkmeasure_collector = SparkMeasureCollector(mode="stage")
        
        # Initialize HTTP collector
        http_collector = SparkHttpCollector(base_url=settings["spark_ui_url"])
        
        # Initialize custom logging collector
        logging_collector = CustomLoggingCollector(log_dir=settings["log_dir"])
        
        # Initialize OS metrics collector
        if settings["os_metrics_enabled"]:
            os_metrics_collector = OSMetricsCollector(prometheus_url=settings.get("prometheus_url"))
        
        # Initialize application metrics collector
        if settings["app_metrics_enabled"]:
            app_metrics_collector = CustomAppMetricsCollector(custom_metrics_path=settings.get("custom_metrics_path"))
        
        # Initialize external metrics collector
        if settings["external_metrics_enabled"]:
            external_system_type = "cloudwatch"
            if settings.get("ganglia_host"):
                external_system_type = "ganglia"
                
            external_metrics_collector = ExternalMetricsCollector(system_type=external_system_type)
            
            # Set up with available config
            if external_system_type == "cloudwatch" and settings.get("cloudwatch_region"):
                external_metrics_collector.setup({
                    "aws_region": settings["cloudwatch_region"]
                })
            elif external_system_type == "ganglia" and settings.get("ganglia_host"):
                external_metrics_collector.setup({
                    "ganglia_host": settings["ganglia_host"]
                })
        
        logger.info("Collectors initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing collectors: {str(e)}")
        raise

def setup_spark_session():
    """Set up a Spark session for the collectors"""
    try:
        from pyspark.sql import SparkSession
        
        # Try to get existing session or create a new one
        spark = SparkSession.getActiveSession()
        if not spark:
            # Try creating a new session, but handle issues with event logging
            try:
                spark = (SparkSession.builder
                        .appName("SparkDebuggerAPI")
                        .master("local[*]")
                        .config("spark.eventLog.enabled", "false")
                        .getOrCreate())
            except Exception as e:
                logger.warning(f"Error creating Spark session with default config: {str(e)}")
                # Try with minimal configuration
                spark = (SparkSession.builder
                        .appName("SparkDebuggerAPI")
                        .master("local[*]")
                        .config("spark.eventLog.enabled", "false")
                        .config("spark.ui.enabled", "true")
                        .getOrCreate())
        
        # Set up collectors with Spark session
        if sparkmeasure_collector:
            try:
                sparkmeasure_collector.setup({"spark_session": spark})
            except Exception as e:
                logger.warning(f"Error setting up SparkMeasure collector: {str(e)}")
        
        if http_collector:
            try:
                http_collector.setup({"spark_session": spark})
            except Exception as e:
                logger.warning(f"Error setting up HTTP collector: {str(e)}")
                
        if app_metrics_collector:
            try:
                app_metrics_collector.setup({"spark_session": spark})
            except Exception as e:
                logger.warning(f"Error setting up app metrics collector: {str(e)}")
        
        logger.info("Spark session set up for collectors")
        return spark
    except Exception as e:
        logger.error(f"Error setting up Spark session: {str(e)}")
        return None

def collect_metrics():
    """Collect metrics from all collectors"""
    global metrics_cache
    
    try:
        # Prepare context
        context = {}
        
        # Try to get a Spark session
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark:
                context["spark_session"] = spark
        except:
            pass
            
        # Collect metrics from each collector
        metrics = {
            "timestamp": time.time(),
            "sparkmeasure": {},
            "http": {},
            "logging": {},
            "os_metrics": {},
            "app_metrics": {},
            "external_metrics": {}
        }
        
        # Collect from SparkMeasure
        if sparkmeasure_collector:
            try:
                metrics["sparkmeasure"] = sparkmeasure_collector.collect(context)
                logger.debug("Collected SparkMeasure metrics")
            except Exception as e:
                logger.error(f"Error collecting SparkMeasure metrics: {str(e)}")
        
        # Collect from HTTP endpoints
        if http_collector:
            try:
                metrics["http"] = http_collector.collect(context)
                logger.debug("Collected HTTP metrics")
            except Exception as e:
                logger.error(f"Error collecting HTTP metrics: {str(e)}")
        
        # Collect from custom logging
        if logging_collector:
            try:
                metrics["logging"] = logging_collector.collect(context)
                logger.debug("Collected custom logging metrics")
            except Exception as e:
                logger.error(f"Error collecting custom logging metrics: {str(e)}")
                
        # Collect from OS metrics
        if os_metrics_collector and settings["os_metrics_enabled"]:
            try:
                metrics["os_metrics"] = os_metrics_collector.collect(context)
                logger.debug("Collected OS metrics")
            except Exception as e:
                logger.error(f"Error collecting OS metrics: {str(e)}")
                
        # Collect from app metrics
        if app_metrics_collector and settings["app_metrics_enabled"]:
            try:
                # We need a DataFrame for app metrics, but we'll skip if not available
                if "spark_session" in context:
                    # Try to get an active table or DataFrame
                    spark = context["spark_session"]
                    try:
                        # Find an available table to analyze
                        tables = spark.catalog.listTables()
                        if tables:
                            table_name = tables[0].name
                            app_context = context.copy()
                            app_context["table_name"] = table_name
                            metrics["app_metrics"] = app_metrics_collector.collect(app_context)
                            logger.debug(f"Collected app metrics for table {table_name}")
                    except Exception as e:
                        logger.debug(f"No tables found for app metrics: {str(e)}")
            except Exception as e:
                logger.error(f"Error collecting app metrics: {str(e)}")
                
        # Collect from external metrics
        if external_metrics_collector and settings["external_metrics_enabled"]:
            try:
                metrics["external_metrics"] = external_metrics_collector.collect(context)
                logger.debug("Collected external metrics")
            except Exception as e:
                logger.error(f"Error collecting external metrics: {str(e)}")
        
        # Update the cache
        metrics_cache.update(metrics)
        metrics_cache["last_updated"] = time.time()
        
        logger.info(f"All metrics collected and cached at {datetime.fromtimestamp(metrics_cache['last_updated']).isoformat()}")
        
        return metrics
    except Exception as e:
        logger.error(f"Error collecting metrics: {str(e)}")
        return {"error": str(e)}

async def periodic_collection():
    """Periodically collect metrics in the background"""
    while settings["monitoring_enabled"]:
        try:
            collect_metrics()
        except Exception as e:
            logger.error(f"Error in periodic collection: {str(e)}")
            
        # Sleep for the collection interval
        await asyncio.sleep(settings["collection_interval"])

# API routes
@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "name": "Comprehensive Spark Pipeline Debugger API",
        "version": "1.0.0",
        "status": "running",
        "metrics_last_updated": datetime.fromtimestamp(metrics_cache["last_updated"]).isoformat() if metrics_cache["last_updated"] > 0 else None,
        "enabled_collectors": {
            "sparkmeasure": sparkmeasure_collector is not None,
            "http": http_collector is not None,
            "logging": logging_collector is not None,
            "os_metrics": os_metrics_collector is not None and settings["os_metrics_enabled"],
            "app_metrics": app_metrics_collector is not None and settings["app_metrics_enabled"],
            "external_metrics": external_metrics_collector is not None and settings["external_metrics_enabled"]
        }
    }

@app.get("/api/v1/settings")
async def get_settings():
    """Get current settings"""
    return settings

@app.post("/api/v1/settings")
async def update_settings(new_settings: SettingsUpdate):
    """Update settings"""
    global settings
    
    # Update settings with new values
    if new_settings.spark_ui_url is not None:
        settings["spark_ui_url"] = new_settings.spark_ui_url
        if http_collector:
            http_collector.base_url = new_settings.spark_ui_url
    
    if new_settings.log_dir is not None:
        settings["log_dir"] = new_settings.log_dir
        if logging_collector:
            logging_collector.log_dir = new_settings.log_dir
    
    if new_settings.cache_ttl is not None:
        settings["cache_ttl"] = new_settings.cache_ttl
    
    if new_settings.collection_interval is not None:
        settings["collection_interval"] = new_settings.collection_interval
    
    if new_settings.monitoring_enabled is not None:
        settings["monitoring_enabled"] = new_settings.monitoring_enabled
        
    if new_settings.os_metrics_enabled is not None:
        settings["os_metrics_enabled"] = new_settings.os_metrics_enabled
        # Initialize collector if not already
        if settings["os_metrics_enabled"] and os_metrics_collector is None:
            os_metrics_collector = OSMetricsCollector(prometheus_url=settings.get("prometheus_url"))
        
    if new_settings.app_metrics_enabled is not None:
        settings["app_metrics_enabled"] = new_settings.app_metrics_enabled
        # Initialize collector if not already
        if settings["app_metrics_enabled"] and app_metrics_collector is None:
            app_metrics_collector = CustomAppMetricsCollector(custom_metrics_path=settings.get("custom_metrics_path"))
    
    if new_settings.external_metrics_enabled is not None:
        settings["external_metrics_enabled"] = new_settings.external_metrics_enabled
        # Initialize collector if not already
        if settings["external_metrics_enabled"] and external_metrics_collector is None:
            external_system_type = new_settings.external_system_type or "cloudwatch"
            external_metrics_collector = ExternalMetricsCollector(system_type=external_system_type)
            
    if new_settings.prometheus_url is not None:
        settings["prometheus_url"] = new_settings.prometheus_url
        if os_metrics_collector:
            os_metrics_collector.prometheus_url = new_settings.prometheus_url
    
    if new_settings.custom_metrics_path is not None:
        settings["custom_metrics_path"] = new_settings.custom_metrics_path
        if app_metrics_collector:
            app_metrics_collector.custom_metrics_path = new_settings.custom_metrics_path
            # Reload custom metrics
            app_metrics_collector._load_custom_metrics(new_settings.custom_metrics_path)
    
    if new_settings.cloudwatch_region is not None:
        settings["cloudwatch_region"] = new_settings.cloudwatch_region
        if external_metrics_collector and external_metrics_collector.system_type == "cloudwatch":
            external_metrics_collector.setup({
                "aws_region": new_settings.cloudwatch_region
            })
    
    if new_settings.ganglia_host is not None:
        settings["ganglia_host"] = new_settings.ganglia_host
        if external_metrics_collector and external_metrics_collector.system_type == "ganglia":
            external_metrics_collector.setup({
                "ganglia_host": new_settings.ganglia_host
            })
            
    if new_settings.external_system_type is not None:
        if external_metrics_collector:
            external_metrics_collector.system_type = new_settings.external_system_type
            
    return settings

@app.post("/api/v1/metrics/collect")
async def trigger_collection(background_tasks: BackgroundTasks):
    """Trigger a metrics collection"""
    background_tasks.add_task(collect_metrics)
    return {"message": "Collection started", "status": "success"}

@app.get("/api/v1/metrics")
async def get_all_metrics(force_refresh: bool = False):
    """Get all metrics"""
    # Check if cache is expired or force refresh is requested
    cache_age = time.time() - metrics_cache["last_updated"]
    if force_refresh or cache_age > settings["cache_ttl"]:
        logger.info("Collecting fresh metrics")
        collect_metrics()
    
    return metrics_cache

# Original metrics endpoints
@app.get("/api/v1/metrics/sparkmeasure")
async def get_sparkmeasure_metrics(force_refresh: bool = False):
    """Get SparkMeasure metrics"""
    # Check if cache is expired or force refresh is requested
    cache_age = time.time() - metrics_cache["last_updated"]
    if force_refresh or cache_age > settings["cache_ttl"]:
        logger.info("Collecting fresh metrics")
        collect_metrics()
    
    return metrics_cache["sparkmeasure"]

@app.get("/api/v1/metrics/http")
async def get_http_metrics(force_refresh: bool = False):
    """Get HTTP metrics"""
    # Check if cache is expired or force refresh is requested
    cache_age = time.time() - metrics_cache["last_updated"]
    if force_refresh or cache_age > settings["cache_ttl"]:
        logger.info("Collecting fresh metrics")
        collect_metrics()
    
    return metrics_cache["http"]

@app.get("/api/v1/metrics/logging")
async def get_logging_metrics(force_refresh: bool = False):
    """Get custom logging metrics"""
    # Check if cache is expired or force refresh is requested
    cache_age = time.time() - metrics_cache["last_updated"]
    if force_refresh or cache_age > settings["cache_ttl"]:
        logger.info("Collecting fresh metrics")
        collect_metrics()
    
    return metrics_cache["logging"]

# New metrics endpoints
@app.get("/api/v1/metrics/os")
async def get_os_metrics(force_refresh: bool = False):
    """Get OS-level metrics"""
    if not settings["os_metrics_enabled"] or not os_metrics_collector:
        raise HTTPException(status_code=400, detail="OS metrics collection is not enabled")
        
    # Check if cache is expired or force refresh is requested
    cache_age = time.time() - metrics_cache["last_updated"]
    if force_refresh or cache_age > settings["cache_ttl"]:
        logger.info("Collecting fresh metrics")
        collect_metrics()
    
    return metrics_cache["os_metrics"]

@app.post("/api/v1/metrics/os/collect")
async def collect_os_metrics(config: OSMetricsConfig):
    """Collect OS metrics with custom configuration"""
    if not settings["os_metrics_enabled"] or not os_metrics_collector:
        raise HTTPException(status_code=400, detail="OS metrics collection is not enabled")
        
    try:
        # Create context from config
        context = {}
        if config.prometheus_url:
            context["prometheus_url"] = config.prometheus_url
        if config.node_hostnames:
            context["node_hostnames"] = config.node_hostnames
            
        # Collect metrics
        metrics = os_metrics_collector.collect(context)
        
        # Update cache
        metrics_cache["os_metrics"] = metrics
        metrics_cache["last_updated"] = time.time()
        
        return metrics
    except Exception as e:
        logger.error(f"Error collecting OS metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/metrics/app")
async def get_app_metrics(force_refresh: bool = False):
    """Get application-specific metrics"""
    if not settings["app_metrics_enabled"] or not app_metrics_collector:
        raise HTTPException(status_code=400, detail="Application metrics collection is not enabled")
        
    # Check if cache is expired or force refresh is requested
    cache_age = time.time() - metrics_cache["last_updated"]
    if force_refresh or cache_age > settings["cache_ttl"]:
        logger.info("Collecting fresh metrics")
        collect_metrics()
    
    return metrics_cache["app_metrics"]

@app.post("/api/v1/metrics/app/data-quality")
async def analyze_data_quality(config: DataQualityConfig):
    """Analyze data quality with custom configuration"""
    if not settings["app_metrics_enabled"] or not app_metrics_collector:
        raise HTTPException(status_code=400, detail="Application metrics collection is not enabled")
        
    try:
        # Get Spark session
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if not spark:
            raise HTTPException(status_code=400, detail="No active Spark session found")
            
        # Create context
        context = {
            "spark_session": spark
        }
        
        # Add table name if provided
        if config.table_name:
            context["table_name"] = config.table_name
            
        # Add custom_args if needed
        if config.columns or config.rules:
            context["custom_args"] = {}
            if config.columns:
                context["custom_args"]["columns"] = config.columns
            if config.rules:
                context["custom_args"]["rules"] = config.rules
                
        # Collect metrics
        metrics = app_metrics_collector.collect(context)
        
        # Update cache
        metrics_cache["app_metrics"] = metrics
        metrics_cache["last_updated"] = time.time()
        
        return metrics
    except Exception as e:
        logger.error(f"Error analyzing data quality: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/metrics/external")
async def get_external_metrics(force_refresh: bool = False):
    """Get external metrics"""
    if not settings["external_metrics_enabled"] or not external_metrics_collector:
        raise HTTPException(status_code=400, detail="External metrics collection is not enabled")
        
    # Check if cache is expired or force refresh is requested
    cache_age = time.time() - metrics_cache["last_updated"]
    if force_refresh or cache_age > settings["cache_ttl"]:
        logger.info("Collecting fresh metrics")
        collect_metrics()
    
    return metrics_cache["external_metrics"]

@app.post("/api/v1/metrics/external/collect")
async def collect_external_metrics(config: ExternalMetricsConfig):
    """Collect external metrics with custom configuration"""
    if not settings["external_metrics_enabled"]:
        raise HTTPException(status_code=400, detail="External metrics collection is not enabled")
        
    try:
        # Initialize collector if not already
        global external_metrics_collector
        if not external_metrics_collector:
            external_metrics_collector = ExternalMetricsCollector(system_type=config.system_type)
            
        # Update collector if system type changed
        if external_metrics_collector.system_type != config.system_type:
            external_metrics_collector.system_type = config.system_type
            
        # Set up collector with config
        external_metrics_collector.setup(config.config)
        
        # Create context with queries if provided
        context = config.config.copy()
        if config.queries:
            context["queries"] = config.queries
            
        # Collect metrics
        metrics = external_metrics_collector.collect(context)
        
        # Update cache
        metrics_cache["external_metrics"] = metrics
        metrics_cache["last_updated"] = time.time()
        
        return metrics
    except Exception as e:
        logger.error(f"Error collecting external metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Application and Job endpoints
@app.get("/api/v1/applications")
async def get_applications():
    """Get all applications"""
    # Make sure we have recent HTTP metrics
    cache_age = time.time() - metrics_cache["last_updated"]
    if cache_age > settings["cache_ttl"]:
        collect_metrics()
    
    http_metrics = metrics_cache.get("http", {})
    app_info = http_metrics.get("app", {})
    
    # Format as a list for consistency
    if app_info and not isinstance(app_info, list):
        app_info = [app_info]
    
    return app_info

@app.get("/api/v1/applications/{app_id}")
async def get_application(app_id: str):
    """Get details for a specific application"""
    # Make sure we have recent HTTP metrics
    cache_age = time.time() - metrics_cache["last_updated"]
    if cache_age > settings["cache_ttl"]:
        collect_metrics()
    
    http_metrics = metrics_cache.get("http", {})
    app_info = http_metrics.get("app", {})
    
    # Check if we have the requested app
    if not app_info or (isinstance(app_info, dict) and app_info.get("id") != app_id) or (
            isinstance(app_info, list) and not any(app.get("id") == app_id for app in app_info)):
        # Try to refresh with the specific app ID
        if http_collector:
            http_collector.app_id = app_id
            http_metrics = http_collector.collect({})
            metrics_cache["http"] = http_metrics
            app_info = http_metrics.get("app", {})
    
    # Format the result
    if isinstance(app_info, list):
        for app in app_info:
            if app.get("id") == app_id:
                return app
        raise HTTPException(status_code=404, detail=f"Application {app_id} not found")
    elif isinstance(app_info, dict) and app_info.get("id") == app_id:
        return app_info
    else:
        raise HTTPException(status_code=404, detail=f"Application {app_id} not found")

@app.get("/api/v1/applications/{app_id}/jobs")
async def get_jobs(app_id: str):
    """Get all jobs for an application"""
    # Make sure we have recent HTTP metrics
    cache_age = time.time() - metrics_cache["last_updated"]
    if cache_age > settings["cache_ttl"]:
        collect_metrics()
    
    http_metrics = metrics_cache.get("http", {})
    
    # Check if we have the requested app
    if http_collector and http_collector.app_id != app_id:
        http_collector.app_id = app_id
        http_metrics = http_collector.collect({})
        metrics_cache["http"] = http_metrics
    
    jobs_info = http_metrics.get("jobs", {})
    if not jobs_info:
        raise HTTPException(status_code=404, detail=f"No jobs found for application {app_id}")
    
    return jobs_info

@app.get("/api/v1/applications/{app_id}/jobs/{job_id}")
async def get_job(app_id: str, job_id: str):
    """Get details for a specific job"""
    # Make sure we have recent HTTP metrics
    cache_age = time.time() - metrics_cache["last_updated"]
    if cache_age > settings["cache_ttl"]:
        collect_metrics()
    
    http_metrics = metrics_cache.get("http", {})
    
    # Check if we have the requested app
    if http_collector and http_collector.app_id != app_id:
        http_collector.app_id = app_id
        http_metrics = http_collector.collect({})
        metrics_cache["http"] = http_metrics
    
    jobs_info = http_metrics.get("jobs", {})
    if not jobs_info:
        raise HTTPException(status_code=404, detail=f"No jobs found for application {app_id}")
    
    # Find the specific job
    for job in jobs_info.get("details", []):
        if str(job.get("jobId")) == job_id:
            return job
    
    raise HTTPException(status_code=404, detail=f"Job {job_id} not found for application {app_id}")

@app.get("/api/v1/applications/{app_id}/stages")
async def get_stages(app_id: str):
    """Get all stages for an application"""
    # Make sure we have recent HTTP metrics
    cache_age = time.time() - metrics_cache["last_updated"]
    if cache_age > settings["cache_ttl"]:
        collect_metrics()
    
    http_metrics = metrics_cache.get("http", {})
    
    # Check if we have the requested app
    if http_collector and http_collector.app_id != app_id:
        http_collector.app_id = app_id
        http_metrics = http_collector.collect({})
        metrics_cache["http"] = http_metrics
    
    stages_info = http_metrics.get("stages", {})
    if not stages_info:
        raise HTTPException(status_code=404, detail=f"No stages found for application {app_id}")
    
    return stages_info

@app.get("/api/v1/applications/{app_id}/stages/{stage_id}")
async def get_stage(app_id: str, stage_id: str):
    """Get details for a specific stage"""
    # Make sure we have recent HTTP metrics
    cache_age = time.time() - metrics_cache["last_updated"]
    if cache_age > settings["cache_ttl"]:
        collect_metrics()
    
    http_metrics = metrics_cache.get("http", {})
    
    # Check if we have the requested app
    if http_collector and http_collector.app_id != app_id:
        http_collector.app_id = app_id
        http_metrics = http_collector.collect({})
        metrics_cache["http"] = http_metrics
    
    stages_info = http_metrics.get("stages", {})
    if not stages_info:
        raise HTTPException(status_code=404, detail=f"No stages found for application {app_id}")
    
    # Find the specific stage
    for stage in stages_info.get("details", []):
        if str(stage.get("stageId")) == stage_id:
            return stage
    
    raise HTTPException(status_code=404, detail=f"Stage {stage_id} not found for application {app_id}")

@app.get("/api/v1/applications/{app_id}/executors")
async def get_executors(app_id: str):
    """Get all executors for an application"""
    # Make sure we have recent HTTP metrics
    cache_age = time.time() - metrics_cache["last_updated"]
    if cache_age > settings["cache_ttl"]:
        collect_metrics()
    
    http_metrics = metrics_cache.get("http", {})
    
    # Check if we have the requested app
    if http_collector and http_collector.app_id != app_id:
        http_collector.app_id = app_id
        http_metrics = http_collector.collect({})
        metrics_cache["http"] = http_metrics
    
    executors_info = http_metrics.get("executors", {})
    if not executors_info:
        raise HTTPException(status_code=404, detail=f"No executors found for application {app_id}")
    
    return executors_info

@app.get("/api/v1/applications/{app_id}/exceptions")
async def get_exceptions(app_id: str):
    """Get all exceptions for an application"""
    # Make sure we have recent logging metrics
    cache_age = time.time() - metrics_cache["last_updated"]
    if cache_age > settings["cache_ttl"]:
        collect_metrics()
    
    logging_metrics = metrics_cache.get("logging", {})
    exceptions = logging_metrics.get("exceptions", {})
    
    # Check if we have logging collector and update the app ID
    if logging_collector:
        try:
            logging_collector.setup({"app_id": app_id})
            logging_metrics = logging_collector.collect({})
            metrics_cache["logging"] = logging_metrics
            exceptions = logging_metrics.get("exceptions", {})
        except Exception as e:
            logger.error(f"Error collecting logging metrics for app {app_id}: {str(e)}")
    
    return exceptions

@app.get("/api/v1/applications/{app_id}/jobs/{job_id}/diagnosis")
async def get_job_diagnosis(app_id: str, job_id: str):
    """Get diagnosis for a specific job"""
    # Collect metrics from all collectors
    cache_age = time.time() - metrics_cache["last_updated"]
    if cache_age > settings["cache_ttl"]:
        collect_metrics()
    
    # Get metrics from each collector
    sparkmeasure_metrics = metrics_cache.get("sparkmeasure", {})
    http_metrics = metrics_cache.get("http", {})
    logging_metrics = metrics_cache.get("logging", {})
    os_metrics = metrics_cache.get("os_metrics", {})
    app_metrics = metrics_cache.get("app_metrics", {})
    
    # Prepare diagnosis information
    diagnosis = {
        "timestamp": time.time(),
        "application_id": app_id,
        "job_id": job_id,
        "status": "SUCCESS",  # Default status
        "summary": "No issues detected",
        "details": [],
        "recommendations": []
    }
    
    # Check for performance issues from SparkMeasure
    if "derived" in sparkmeasure_metrics:
        derived = sparkmeasure_metrics["derived"]
        
        # Check for data skew
        if derived.get("dataSkew", 0) > 3:
            diagnosis["status"] = "WARNING"
            diagnosis["details"].append(f"Data skew detected (skew ratio: {derived.get('dataSkew', 0):.2f})")
            diagnosis["recommendations"].append(
                "Consider using salting techniques or repartitioning to distribute data more evenly"
            )
        
        # Check for GC pressure
        if derived.get("gcPressure", 0) > 0.1:
            diagnosis["status"] = "WARNING"
            diagnosis["details"].append(f"High GC pressure detected ({derived.get('gcPressure', 0)*100:.2f}% of execution time)")
            diagnosis["recommendations"].append(
                "Consider increasing executor memory or adjusting GC settings"
            )
        
        # Check for memory spill
        if "memorySpill" in derived and derived["memorySpill"].get("total", 0) > 1e8:
            diagnosis["status"] = "WARNING"
            spill_mb = derived["memorySpill"].get("total", 0) / 1024 / 1024
            diagnosis["details"].append(f"Significant memory spill detected ({spill_mb:.2f} MB)")
            diagnosis["recommendations"].append(
                "Consider increasing spark.memory.fraction or increasing executor memory"
            )
    
    # Check for failures in jobs from HTTP metrics
    job_found = False
    for job in http_metrics.get("jobs", {}).get("details", []):
        if str(job.get("jobId")) == job_id:
            job_found = True
            if job.get("status") == "FAILED":
                diagnosis["status"] = "ERROR"
                diagnosis["summary"] = f"Job {job_id} failed"
                diagnosis["details"].append(f"Job {job_id} failed with {job.get('numFailedTasks', 0)} failed tasks")
            
            # Check for task failures
            if job.get("numFailedTasks", 0) > 0:
                diagnosis["status"] = "WARNING" if diagnosis["status"] != "ERROR" else "ERROR"
                diagnosis["details"].append(f"{job.get('numFailedTasks', 0)} tasks failed in this job")
    
    # Check for exceptions in logging metrics
    exceptions = logging_metrics.get("exceptions", {}).get("latest", [])
    for exception in exceptions[:5]:  # Limit to first 5 exceptions
        exception_type = exception.get("type", "Unknown")
        diagnosis["details"].append(f"Exception detected: {exception_type}")
        
        # Update status based on exceptions
        if exception_type in ["OutOfMemoryError", "SparkException"]:
            diagnosis["status"] = "ERROR"
        elif diagnosis["status"] != "ERROR":
            diagnosis["status"] = "WARNING"
        
        # Add specific recommendations based on exception type
        if "OutOfMemoryError" in exception_type:
            diagnosis["recommendations"].append(
                "Increase executor memory or driver memory, check for data skew"
            )
        elif "SparkException" in exception_type and "Task not serializable" in exception.get("message", ""):
            diagnosis["recommendations"].append(
                "Check for non-serializable objects in your Spark transformations"
            )
        elif "FetchFailedException" in exception_type:
            diagnosis["recommendations"].append(
                "Check for executor failures during shuffle, consider enabling external shuffle service"
            )
    
    # Check OS metrics for system-level issues
    if os_metrics and "nodes" in os_metrics:
        for node_name, node_metrics in os_metrics.get("nodes", {}).items():
            # Check CPU usage
            if "cpu" in node_metrics and "usage" in node_metrics["cpu"]:
                cpu_usage = node_metrics["cpu"]["usage"]
                if cpu_usage > 90:
                    diagnosis["status"] = "WARNING" if diagnosis["status"] != "ERROR" else "ERROR"
                    diagnosis["details"].append(f"High CPU usage on node {node_name}: {cpu_usage:.2f}%")
                    diagnosis["recommendations"].append(
                        "Consider increasing executor cores or reducing parallelism"
                    )
            
            # Check memory usage
            if "memory" in node_metrics and "percent" in node_metrics["memory"]:
                memory_usage = node_metrics["memory"]["percent"]
                if memory_usage > 90:
                    diagnosis["status"] = "WARNING" if diagnosis["status"] != "ERROR" else "ERROR"
                    diagnosis["details"].append(f"High memory usage on node {node_name}: {memory_usage:.2f}%")
                    diagnosis["recommendations"].append(
                        "Consider increasing node memory or reducing executor memory"
                    )
            
            # Check disk usage
            if "disk" in node_metrics and "partitions" in node_metrics["disk"]:
                for mount, partition in node_metrics["disk"]["partitions"].items():
                    if partition.get("percent", 0) > 90:
                        diagnosis["status"] = "WARNING" if diagnosis["status"] != "ERROR" else "ERROR"
                        diagnosis["details"].append(f"High disk usage on node {node_name}, mount {mount}: {partition.get('percent', 0):.2f}%")
                        diagnosis["recommendations"].append(
                            "Consider cleaning up disk space or increasing disk size"
                        )
    
    # Check app metrics for data quality issues
    if app_metrics and "data_quality" in app_metrics:
        data_quality = app_metrics["data_quality"]
        
        # Check for high null rate
        if "overall_null_rate" in data_quality and data_quality["overall_null_rate"] > 0.1:
            diagnosis["status"] = "WARNING" if diagnosis["status"] != "ERROR" else "ERROR"
            diagnosis["details"].append(f"High null rate in data: {data_quality['overall_null_rate']*100:.2f}%")
            diagnosis["recommendations"].append(
                "Check data source for missing values and add null handling in processing"
            )
        
        # Check for quality rule violations
        if "quality_rules" in data_quality:
            for rule_name, rule_result in data_quality["quality_rules"].items():
                if isinstance(rule_result, dict) and rule_result.get("pass") is False:
                    diagnosis["status"] = "WARNING" if diagnosis["status"] != "ERROR" else "ERROR"
                    diagnosis["details"].append(f"Data quality rule violation: {rule_name} - {rule_result.get('message', '')}")
                    diagnosis["recommendations"].append(
                        "Add data quality validation at source or implement cleanup in pipeline"
                    )
    
    # Update summary based on status
    if diagnosis["status"] == "ERROR":
        diagnosis["summary"] = "Critical issues detected that caused job failure"
    elif diagnosis["status"] == "WARNING":
        diagnosis["summary"] = "Performance issues detected that may impact job execution"
    
    # If no specific issues found, check for job in HTTP metrics
    if len(diagnosis["details"]) == 0:
        if not job_found:
            diagnosis["summary"] = f"No data available for job {job_id}"
            diagnosis["status"] = "UNKNOWN"
        else:
            diagnosis["summary"] = f"Job {job_id} completed successfully with no issues detected"
    
    return diagnosis

@app.get("/api/v1/applications/{app_id}/performance")
async def get_performance_metrics(app_id: str):
    """Get performance metrics for an application"""
    # Collect metrics from all collectors
    cache_age = time.time() - metrics_cache["last_updated"]
    if cache_age > settings["cache_ttl"]:
        collect_metrics()
    
    # Get metrics from each collector
    sparkmeasure_metrics = metrics_cache.get("sparkmeasure", {})
    http_metrics = metrics_cache.get("http", {})
    os_metrics = metrics_cache.get("os_metrics", {})
    
    # Extract performance metrics
    performance = {
        "timestamp": time.time(),
        "application_id": app_id,
    }
    
    # Add derived metrics from SparkMeasure
    if "derived" in sparkmeasure_metrics:
        performance["sparkmeasure"] = sparkmeasure_metrics["derived"]
    
    # Add executor metrics from HTTP
    if "executors" in http_metrics:
        performance["executors"] = http_metrics["executors"].get("metrics", {})
    
    # Add stage metrics if available
    if "stage" in sparkmeasure_metrics:
        stage_metrics = sparkmeasure_metrics["stage"]
        
        # Extract relevant performance metrics
        stage_performance = {}
        for key in ["executorRunTime", "executorCpuTime", "jvmGCTime", 
                    "shuffleFetchWaitTime", "shuffleWriteTime", "resultSize",
                    "diskBytesSpilled", "memoryBytesSpilled", "peakExecutionMemory"]:
            if key in stage_metrics:
                stage_performance[key] = stage_metrics[key]
        
        performance["stage_metrics"] = stage_performance
        
    # Add OS metrics if available
    if os_metrics and "nodes" in os_metrics:
        performance["os"] = {
            "nodes": {}
        }
        
        # Extract key OS metrics for each node
        for node_name, node_metrics in os_metrics["nodes"].items():
            performance["os"]["nodes"][node_name] = {}
            
            # CPU metrics
            if "cpu" in node_metrics:
                performance["os"]["nodes"][node_name]["cpu"] = {
                    "usage": node_metrics["cpu"].get("usage"),
                    "load1": node_metrics["cpu"].get("load1"),
                    "load5": node_metrics["cpu"].get("load5"),
                    "load15": node_metrics["cpu"].get("load15")
                }
                
            # Memory metrics
            if "memory" in node_metrics:
                performance["os"]["nodes"][node_name]["memory"] = {
                    "usage_percent": node_metrics["memory"].get("percent"),
                    "used": node_metrics["memory"].get("used"),
                    "total": node_metrics["memory"].get("total")
                }
                
            # Disk metrics
            if "disk" in node_metrics:
                # Disk I/O rates
                disk_io = {}
                if "read_bytes_per_sec" in node_metrics["disk"]:
                    disk_io["read_rate"] = node_metrics["disk"]["read_bytes_per_sec"]
                if "write_bytes_per_sec" in node_metrics["disk"]:
                    disk_io["write_rate"] = node_metrics["disk"]["write_bytes_per_sec"]
                    
                performance["os"]["nodes"][node_name]["disk"] = disk_io
                
            # Network metrics
            if "network" in node_metrics and "total" in node_metrics["network"]:
                network_io = {}
                if "bytes_sent_per_sec" in node_metrics["network"]["total"]:
                    network_io["send_rate"] = node_metrics["network"]["total"]["bytes_sent_per_sec"]
                if "bytes_recv_per_sec" in node_metrics["network"]["total"]:
                    network_io["receive_rate"] = node_metrics["network"]["total"]["bytes_recv_per_sec"]
                    
                performance["os"]["nodes"][node_name]["network"] = network_io
    
    return performance

@app.get("/api/v1/dashboard")
async def get_dashboard_metrics():
    """Get metrics for dashboard"""
    # Collect metrics from all collectors
    cache_age = time.time() - metrics_cache["last_updated"]
    if cache_age > settings["cache_ttl"]:
        collect_metrics()
    
    # Prepare dashboard data
    dashboard = {
        "timestamp": time.time(),
        "applications": [],
        "jobs": {
            "total": 0,
            "running": 0,
            "completed": 0,
            "failed": 0,
            "recent": []
        },
        "stages": {
            "total": 0,
            "active": 0,
            "completed": 0,
            "failed": 0,
            "skipped": 0
        },
        "tasks": {
            "total": 0,
            "active": 0,
            "completed": 0,
            "failed": 0
        },
        "executors": {
            "total": 0,
            "active": 0,
            "dead": 0,
            "memory_usage": 0,
            "memory_total": 0
        },
        "performance": {
            "dataSkew": None,
            "gcPressure": None,
            "shuffleIntensity": None,
            "memorySpill": None
        },
        "exceptions": {
            "count": 0,
            "latest": []
        },
        "os_metrics": {
            "cpu_usage": None,
            "memory_usage": None,
            "disk_io": None,
            "network_io": None
        },
        "data_quality": {
            "null_rate": None,
            "duplicate_rate": None,
            "quality_rules": []
        }
    }
    
    # Extract application info
    http_metrics = metrics_cache.get("http", {})
    app_info = http_metrics.get("app", {})
    
    if app_info:
        # Format as a list for consistency
        if not isinstance(app_info, list):
            app_info = [app_info]
        
        dashboard["applications"] = app_info
    
    # Extract job info
    jobs_info = http_metrics.get("jobs", {})
    if jobs_info:
        dashboard["jobs"]["total"] = jobs_info.get("count", 0)
        dashboard["jobs"]["running"] = jobs_info.get("active", 0)
        dashboard["jobs"]["completed"] = jobs_info.get("completed", 0)
        dashboard["jobs"]["failed"] = jobs_info.get("failed", 0)
        dashboard["jobs"]["recent"] = jobs_info.get("details", [])
    
    # Extract stage info
    stages_info = http_metrics.get("stages", {})
    if stages_info:
        dashboard["stages"]["total"] = stages_info.get("count", 0)
        dashboard["stages"]["active"] = stages_info.get("active", 0)
        dashboard["stages"]["completed"] = stages_info.get("completed", 0)
        dashboard["stages"]["failed"] = stages_info.get("failed", 0)
        dashboard["stages"]["skipped"] = stages_info.get("skipped", 0)
    
    # Extract executor info
    executors_info = http_metrics.get("executors", {})
    if executors_info:
        dashboard["executors"]["total"] = executors_info.get("count", 0)
        dashboard["executors"]["active"] = executors_info.get("active", 0)
        dashboard["executors"]["dead"] = executors_info.get("dead", 0)
        
        if "metrics" in executors_info:
            dashboard["executors"]["memory_usage"] = executors_info["metrics"].get("usedMemory", 0)
            dashboard["executors"]["memory_total"] = executors_info["metrics"].get("totalMemory", 0)
    
    # Extract performance metrics
    sparkmeasure_metrics = metrics_cache.get("sparkmeasure", {})
    if "derived" in sparkmeasure_metrics:
        dashboard["performance"] = sparkmeasure_metrics["derived"]
    
    # Extract exception info
    logging_metrics = metrics_cache.get("logging", {})
    exceptions = logging_metrics.get("exceptions", {})
    if exceptions:
        dashboard["exceptions"]["count"] = exceptions.get("count", 0)
        dashboard["exceptions"]["latest"] = exceptions.get("latest", [])
    
    # Extract OS metrics
    os_metrics = metrics_cache.get("os_metrics", {})
    if os_metrics and "nodes" in os_metrics:
        # Calculate average across all nodes
        cpu_values = []
        memory_values = []
        disk_read_values = []
        disk_write_values = []
        network_recv_values = []
        network_send_values = []
        
        for node_name, node_metrics in os_metrics["nodes"].items():
            # CPU
            if "cpu" in node_metrics and "usage" in node_metrics["cpu"]:
                cpu_values.append(node_metrics["cpu"]["usage"])
                
            # Memory
            if "memory" in node_metrics and "percent" in node_metrics["memory"]:
                memory_values.append(node_metrics["memory"]["percent"])
                
            # Disk I/O
            if "disk" in node_metrics:
                if "read_bytes_per_sec" in node_metrics["disk"]:
                    disk_read_values.append(node_metrics["disk"]["read_bytes_per_sec"])
                if "write_bytes_per_sec" in node_metrics["disk"]:
                    disk_write_values.append(node_metrics["disk"]["write_bytes_per_sec"])
                    
            # Network I/O
            if "network" in node_metrics and "total" in node_metrics["network"]:
                if "bytes_recv_per_sec" in node_metrics["network"]["total"]:
                    network_recv_values.append(node_metrics["network"]["total"]["bytes_recv_per_sec"])
                if "bytes_sent_per_sec" in node_metrics["network"]["total"]:
                    network_send_values.append(node_metrics["network"]["total"]["bytes_sent_per_sec"])
        
        # Calculate averages
        if cpu_values:
            dashboard["os_metrics"]["cpu_usage"] = sum(cpu_values) / len(cpu_values)
        if memory_values:
            dashboard["os_metrics"]["memory_usage"] = sum(memory_values) / len(memory_values)
        if disk_read_values and disk_write_values:
            dashboard["os_metrics"]["disk_io"] = {
                "read_rate": sum(disk_read_values) / len(disk_read_values),
                "write_rate": sum(disk_write_values) / len(disk_write_values)
            }
        if network_recv_values and network_send_values:
            dashboard["os_metrics"]["network_io"] = {
                "receive_rate": sum(network_recv_values) / len(network_recv_values),
                "send_rate": sum(network_send_values) / len(network_send_values)
            }
    
    # Extract data quality metrics
    app_metrics = metrics_cache.get("app_metrics", {})
    if app_metrics and "data_quality" in app_metrics:
        data_quality = app_metrics["data_quality"]
        
        # Overall null rate
        if "overall_null_rate" in data_quality:
            dashboard["data_quality"]["null_rate"] = data_quality["overall_null_rate"]
            
        # Duplicate rate
        if "duplicate_percentage" in data_quality:
            dashboard["data_quality"]["duplicate_rate"] = data_quality["duplicate_percentage"] / 100.0
            
        # Quality rules
        if "quality_rules" in data_quality:
            quality_rules = []
            for rule_name, rule_result in data_quality["quality_rules"].items():
                if isinstance(rule_result, dict):
                    quality_rules.append({
                        "name": rule_name,
                        "passed": rule_result.get("pass", True),
                        "message": rule_result.get("message", "")
                    })
            
            dashboard["data_quality"]["quality_rules"] = quality_rules
    
    return dashboard

# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Initialize collectors and start background tasks on startup"""
    # Initialize collectors
    initialize_collectors()
    
    # Try to set up Spark session
    setup_spark_session()
    
    # Collect initial metrics
    collect_metrics()
    
    # Start background task for periodic collection
    asyncio.create_task(periodic_collection())
    
    logger.info("API server started")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown"""
    settings["monitoring_enabled"] = False
    logger.info("API server shutting down")

# Run the API server
if __name__ == "__main__":
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Comprehensive Spark Pipeline Debugger API")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    parser.add_argument("--spark-ui-url", type=str, default="http://localhost:4040", help="Spark UI URL")
    parser.add_argument("--log-dir", type=str, default="/tmp/spark-logs", help="Spark log directory")
    parser.add_argument("--collection-interval", type=int, default=30, help="Metrics collection interval in seconds")
    parser.add_argument("--prometheus-url", type=str, default=None, help="Prometheus URL for OS metrics")
    parser.add_argument("--custom-metrics-path", type=str, default=None, help="Path to custom metrics definition file")
    parser.add_argument("--cloudwatch-region", type=str, default=None, help="AWS region for CloudWatch metrics")
    parser.add_argument("--ganglia-host", type=str, default=None, help="Ganglia host for metrics")
    parser.add_argument("--no-os-metrics", action="store_true", help="Disable OS metrics collection")
    parser.add_argument("--no-app-metrics", action="store_true", help="Disable application metrics collection")
    parser.add_argument("--no-external-metrics", action="store_true", help="Disable external metrics collection")
    args = parser.parse_args()
    
    # Update settings
    settings["spark_ui_url"] = args.spark_ui_url
    settings["log_dir"] = args.log_dir
    settings["collection_interval"] = args.collection_interval
    
    if args.prometheus_url:
        settings["prometheus_url"] = args.prometheus_url
    if args.custom_metrics_path:
        settings["custom_metrics_path"] = args.custom_metrics_path
    if args.cloudwatch_region:
        settings["cloudwatch_region"] = args.cloudwatch_region
    if args.ganglia_host:
        settings["ganglia_host"] = args.ganglia_host
        
    settings["os_metrics_enabled"] = not args.no_os_metrics
    settings["app_metrics_enabled"] = not args.no_app_metrics
    settings["external_metrics_enabled"] = not args.no_external_metrics
    
    # Start the server
    uvicorn.run(app, host=args.host, port=args.port)