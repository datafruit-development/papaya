# spark_debugger/collectors/http_endpoints.py
import json
import time
import urllib.parse
import requests
from typing import Dict, Any, List, Optional, Union

from base_collector import BaseCollector

class SparkHttpCollector(BaseCollector):
    """Collects metrics from Spark UI HTTP endpoints.
    
    This collector provides information about applications, jobs, stages, and
    executors by querying the Spark Web UI's REST API endpoints.
    """
    
    def __init__(self, base_url: Optional[str] = None):
        """Initialize the HTTP endpoints collector.
        
        Args:
            base_url: Base URL for Spark UI (e.g., http://localhost:4040)
                     If None, will attempt to auto-discover
        """
        super().__init__(name="spark_http")
        self.base_url = base_url
        self.app_id = None
        self.timeout = 10  # seconds for HTTP requests
        
    def setup(self, context: Dict[str, Any]) -> None:
        """Set up the collector with context information.
        
        Args:
            context: Dictionary that may contain:
                    - spark_session: Active Spark session
                    - base_url: Override for Spark UI URL
                    - app_id: Specific application ID to monitor
                    - timeout: Timeout for HTTP requests
        """
        # Override base_url if provided
        if "base_url" in context:
            self.base_url = context["base_url"]
            
        # Try to get the application ID
        if "app_id" in context:
            self.app_id = context["app_id"]
        elif "spark_session" in context:
            spark = context["spark_session"]
            self.app_id = spark.sparkContext.applicationId
            
        # Set timeout if provided
        if "timeout" in context:
            self.timeout = context["timeout"]
            
        # If we don't have a base_url yet, try to discover it
        if self.base_url is None:
            if "spark_session" in context:
                spark = context["spark_session"]
                ui_web_url = spark.sparkContext.uiWebUrl
                if ui_web_url:
                    self.base_url = ui_web_url
                    self.logger.info(f"Discovered Spark UI URL: {self.base_url}")
            
            # Fallback to default
            if self.base_url is None:
                self.base_url = "http://localhost:4040"
                self.logger.warning(f"Using default Spark UI URL: {self.base_url}")
                
        self.logger.info(f"HTTP Endpoints collector initialized with base URL: {self.base_url}")
    
    def get_supported_metrics(self) -> List[str]:
        """Return a list of metrics that this collector can provide."""
        return [
            # Application metrics
            "app.id", "app.name", "app.status", "app.startTime", "app.endTime", 
            "app.duration", "app.user",
            
            # Job metrics
            "jobs.count", "jobs.active", "jobs.completed", "jobs.failed",
            "jobs.details", "jobs.duration",
            
            # Stage metrics
            "stages.count", "stages.active", "stages.completed", "stages.failed",
            "stages.skipped", "stages.details",
            
            # Executor metrics
            "executors.count", "executors.active", "executors.dead",
            "executors.metrics", "executors.memory", "executors.disk",
            
            # Storage metrics
            "storage.memoryUsed", "storage.diskUsed", "storage.totalCached",
            
            # Environment
            "environment.spark", "environment.jvm", "environment.system"
        ]
    
    def collect(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Collect metrics from Spark UI HTTP endpoints.
        
        Args:
            context: Dictionary containing collection context
                     May include app_id, base_url to override defaults
                     
        Returns:
            Dictionary of collected metrics
        """
        # Update setup if context contains relevant info
        if any(key in context for key in ["base_url", "app_id", "spark_session", "timeout"]):
            self.setup(context)
            
        metrics = {
            "timestamp": time.time(),
            "base_url": self.base_url
        }
        
        # First, get application information
        app_info = self._get_application_info()
        if app_info:
            metrics["app"] = app_info
            
            # Get application ID if we don't have it yet
            if self.app_id is None and "id" in app_info:
                self.app_id = app_info["id"]
        
        if self.app_id:
            # Get jobs information
            metrics["jobs"] = self._get_jobs_info()
            
            # Get stages information
            metrics["stages"] = self._get_stages_info()
            
            # Get executors information
            metrics["executors"] = self._get_executors_info()
            
            # Get storage information
            metrics["storage"] = self._get_storage_info()
            
            # Get environment information
            metrics["environment"] = self._get_environment_info()
            
        return metrics
    
    def get_app_id(self, force_refresh: bool = False) -> Optional[str]:
        """Get the application ID from the Spark UI.
        
        Args:
            force_refresh: If True, query the API even if we already have an ID
            
        Returns:
            Application ID or None if not found
        """
        if self.app_id is not None and not force_refresh:
            return self.app_id
            
        try:
            response = self._make_request("/api/v1/applications")
            if response and isinstance(response, list) and len(response) > 0:
                self.app_id = response[0].get("id")
                return self.app_id
        except Exception as e:
            self.logger.error(f"Error getting application ID: {str(e)}")
            
        return None
    
    def check_for_failures(self) -> Optional[Dict[str, Any]]:
        """Check for failed jobs and return failure details if any.
        
        Returns:
            Dictionary with failure details or None if no failures
        """
        if not self.app_id:
            self.logger.warning("No application ID available for failure detection")
            return None
            
        try:
            # Get jobs information
            jobs_info = self._get_jobs_info()
            
            # Check for failed jobs
            failed_jobs = []
            for job in jobs_info.get("details", []):
                if job.get("status") == "FAILED":
                    # Found a failed job
                    job_id = job.get("jobId")
                    self.logger.info(f"Detected failed job: {job_id}")
                    
                    # Get more details about the failure
                    failure_details = {
                        "app_id": self.app_id,
                        "job_id": job_id,
                        "name": job.get("name", "Unknown"),
                        "submission_time": job.get("submissionTime"),
                        "completion_time": job.get("completionTime"),
                        "num_tasks": job.get("numTasks"),
                        "num_failed_tasks": job.get("numFailedTasks", 0),
                        "failure_reason": None  # Will try to fill this in
                    }
                    
                    # Get failed stages for this job
                    stages_info = self._get_stages_info()
                    failure_details["stages"] = []
                    
                    for stage in stages_info.get("details", []):
                        if stage.get("status") == "FAILED":
                            # Check if this stage belongs to our job
                            stage_job_ids = stage.get("jobIds", [])
                            if isinstance(stage_job_ids, list) and str(job_id) in [str(j) for j in stage_job_ids]:
                                failure_details["stages"].append({
                                    "stage_id": stage.get("stageId"),
                                    "name": stage.get("name"),
                                    "failure_reason": stage.get("failureReason")
                                })
                                
                                # If we don't have a failure reason yet, use this stage's reason
                                if not failure_details["failure_reason"] and stage.get("failureReason"):
                                    failure_details["failure_reason"] = stage.get("failureReason")
                    
                    failed_jobs.append(failure_details)
            
            if failed_jobs:
                return {
                    "timestamp": time.time(),
                    "num_failed_jobs": len(failed_jobs),
                    "failed_jobs": failed_jobs
                }
            
            return None
        
        except Exception as e:
            self.logger.error(f"Error checking for failures: {str(e)}")
            return None
        
    def _get_application_info(self) -> Dict[str, Any]:
        """Get information about the Spark application.
        
        Returns:
            Dictionary with application details
        """
        app_info = {}
        
        # First, get the list of applications
        try:
            response = self._make_request("/api/v1/applications")
            
            if response and isinstance(response, list):
                if len(response) == 0:
                    self.logger.warning("No Spark applications found")
                    return app_info
                    
                # If we have a specific app_id, find that application
                app = None
                if self.app_id:
                    for a in response:
                        if a.get("id") == self.app_id:
                            app = a
                            break
                
                # If no specific app found, use the first one
                if app is None:
                    app = response[0]
                    
                # Extract basic info
                app_info = {
                    "id": app.get("id"),
                    "name": app.get("name"),
                    "attempts": app.get("attempts", [])
                }
                
                # If we have an ID, get more detailed info
                if "id" in app_info:
                    self.app_id = app_info["id"]
                    app_detail = self._make_request(f"/api/v1/applications/{self.app_id}")
                    
                    if app_detail:
                        # Update with more detailed information
                        app_info.update({
                            "startTime": app_detail.get("startTime"),
                            "endTime": app_detail.get("endTime"),
                            "duration": app_detail.get("duration"),
                            "sparkUser": app_detail.get("sparkUser"),
                            "completed": app_detail.get("completed", False),
                            "lastUpdated": app_detail.get("lastUpdated")
                        })
                        
        except Exception as e:
            self.logger.error(f"Error getting application info: {str(e)}")
            
        return app_info
    
    def _get_jobs_info(self) -> Dict[str, Any]:
        """Get information about Spark jobs.
        
        Returns:
            Dictionary with jobs information
        """
        jobs_info = {
            "count": 0,
            "active": 0,
            "completed": 0,
            "failed": 0,
            "details": []
        }
        
        if not self.app_id:
            self.logger.warning("No application ID available")
            return jobs_info
            
        try:
            response = self._make_request(f"/api/v1/applications/{self.app_id}/jobs")
            
            if response and isinstance(response, list):
                jobs_info["count"] = len(response)
                
                # Count different job statuses
                for job in response:
                    status = job.get("status")
                    if status == "RUNNING":
                        jobs_info["active"] += 1
                    elif status == "SUCCEEDED":
                        jobs_info["completed"] += 1
                    elif status == "FAILED":
                        jobs_info["failed"] += 1
                
                # Add detailed job information (limited to latest 10 jobs)
                latest_jobs = sorted(
                    response, 
                    key=lambda j: j.get("submissionTime", ""), 
                    reverse=True
                )[:10]
                
                jobs_info["details"] = []
                for job in latest_jobs:
                    jobs_info["details"].append({
                        "jobId": job.get("jobId"),
                        "name": job.get("name"),
                        "status": job.get("status"),
                        "stageIds": job.get("stageIds", []),
                        "submissionTime": job.get("submissionTime"),
                        "completionTime": job.get("completionTime"),
                        "numTasks": job.get("numTasks"),
                        "numActiveTasks": job.get("numActiveTasks"),
                        "numCompletedTasks": job.get("numCompletedTasks"),
                        "numSkippedTasks": job.get("numSkippedTasks"),
                        "numFailedTasks": job.get("numFailedTasks"),
                        "numKilledTasks": job.get("numKilledTasks")
                    })
                    
        except Exception as e:
            self.logger.error(f"Error getting jobs info: {str(e)}")
            
        return jobs_info
    
    def _get_stages_info(self) -> Dict[str, Any]:
        """Get information about Spark stages.
        
        Returns:
            Dictionary with stages information
        """
        stages_info = {
            "count": 0,
            "active": 0,
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "details": []
        }
        
        if not self.app_id:
            self.logger.warning("No application ID available")
            return stages_info
            
        try:
            response = self._make_request(f"/api/v1/applications/{self.app_id}/stages")
            
            if response and isinstance(response, list):
                stages_info["count"] = len(response)
                
                # Count different stage statuses
                for stage in response:
                    status = stage.get("status")
                    if status == "ACTIVE":
                        stages_info["active"] += 1
                    elif status == "COMPLETE":
                        stages_info["completed"] += 1
                    elif status == "FAILED":
                        stages_info["failed"] += 1
                    elif status == "SKIPPED":
                        stages_info["skipped"] += 1
                
                # Add detailed stage information for problematic stages
                failed_stages = [s for s in response if s.get("status") == "FAILED"]
                active_stages = [s for s in response if s.get("status") == "ACTIVE"]
                
                # Combine failed and active stages, limited to 10
                important_stages = (failed_stages + active_stages)[:10]
                
                stages_info["details"] = []
                for stage in important_stages:
                    # Get more detailed stage info
                    stage_id = stage.get("stageId")
                    attempt_id = stage.get("attemptId")
                    
                    stage_detail = self._make_request(
                        f"/api/v1/applications/{self.app_id}/stages/{stage_id}/{attempt_id}"
                    )
                    
                    if stage_detail:
                        stages_info["details"].append({
                            "stageId": stage_id,
                            "attemptId": attempt_id,
                            "name": stage.get("name"),
                            "status": stage.get("status"),
                            "numTasks": stage.get("numTasks"),
                            "numActiveTasks": stage.get("numActiveTasks"),
                            "numCompleteTasks": stage.get("numCompleteTasks"),
                            "numFailedTasks": stage.get("numFailedTasks"),
                            "executorRunTime": stage.get("executorRunTime"),
                            "inputBytes": stage.get("inputBytes"),
                            "outputBytes": stage.get("outputBytes"),
                            "shuffleReadBytes": stage.get("shuffleReadBytes"),
                            "shuffleWriteBytes": stage.get("shuffleWriteBytes"),
                            "memoryBytesSpilled": stage.get("memoryBytesSpilled"),
                            "diskBytesSpilled": stage.get("diskBytesSpilled"),
                            "failureReason": stage_detail.get("failureReason")
                        })
                    
        except Exception as e:
            self.logger.error(f"Error getting stages info: {str(e)}")
            
        return stages_info
    
    def _get_executors_info(self) -> Dict[str, Any]:
        """Get information about Spark executors.
        
        Returns:
            Dictionary with executors information
        """
        executors_info = {
            "count": 0,
            "active": 0,
            "dead": 0,
            "metrics": {},
            "details": []
        }
        
        if not self.app_id:
            self.logger.warning("No application ID available")
            return executors_info
            
        try:
            response = self._make_request(f"/api/v1/applications/{self.app_id}/executors")
            
            if response and isinstance(response, list):
                executors_info["count"] = len(response)
                
                # Count active vs. dead executors
                for executor in response:
                    if executor.get("isActive", True):
                        executors_info["active"] += 1
                    else:
                        executors_info["dead"] += 1
                
                # Calculate aggregate metrics
                if executors_info["count"] > 0:
                    total_memory = sum(e.get("maxMemory", 0) for e in response)
                    used_memory = sum(e.get("memoryUsed", 0) for e in response)
                    total_disk = sum(e.get("totalDiskUsed", 0) for e in response if "totalDiskUsed" in e)
                    total_cores = sum(e.get("totalCores", 0) for e in response)
                    
                    executors_info["metrics"] = {
                        "totalMemory": total_memory,
                        "usedMemory": used_memory,
                        "memoryUtilization": used_memory / total_memory if total_memory > 0 else 0,
                        "totalDisk": total_disk,
                        "totalCores": total_cores,
                        "totalTasks": sum(e.get("totalTasks", 0) for e in response),
                        "activeTasks": sum(e.get("activeTasks", 0) for e in response),
                        "failedTasks": sum(e.get("failedTasks", 0) for e in response),
                        "completedTasks": sum(e.get("completedTasks", 0) for e in response),
                        "totalDuration": sum(e.get("totalDuration", 0) for e in response),
                        "totalGCTime": sum(e.get("totalGCTime", 0) for e in response),
                        "totalInputBytes": sum(e.get("totalInputBytes", 0) for e in response),
                        "totalShuffleRead": sum(e.get("totalShuffleRead", 0) for e in response),
                        "totalShuffleWrite": sum(e.get("totalShuffleWrite", 0) for e in response)
                    }
                    
                    # Calculate GC pressure
                    total_duration = executors_info["metrics"]["totalDuration"]
                    total_gc_time = executors_info["metrics"]["totalGCTime"]
                    if total_duration > 0:
                        executors_info["metrics"]["gcPressure"] = total_gc_time / total_duration
                    
                # Add detailed executor information
                executors_info["details"] = []
                for executor in response:
                    executors_info["details"].append({
                        "id": executor.get("id"),
                        "hostPort": executor.get("hostPort"),
                        "isActive": executor.get("isActive", True),
                        "rddBlocks": executor.get("rddBlocks"),
                        "memoryUsed": executor.get("memoryUsed"),
                        "diskUsed": executor.get("diskUsed"),
                        "totalCores": executor.get("totalCores"),
                        "maxTasks": executor.get("maxTasks"),
                        "activeTasks": executor.get("activeTasks"),
                        "failedTasks": executor.get("failedTasks"),
                        "completedTasks": executor.get("completedTasks"),
                        "totalTasks": executor.get("totalTasks"),
                        "totalDuration": executor.get("totalDuration"),
                        "totalGCTime": executor.get("totalGCTime"),
                        "totalInputBytes": executor.get("totalInputBytes"),
                        "totalShuffleRead": executor.get("totalShuffleRead"),
                        "totalShuffleWrite": executor.get("totalShuffleWrite")
                    })
                    
        except Exception as e:
            self.logger.error(f"Error getting executors info: {str(e)}")
            
        return executors_info
    
    def _get_storage_info(self) -> Dict[str, Any]:
        """Get information about Spark storage (cached RDDs, DataFrames).
        
        Returns:
            Dictionary with storage information
        """
        storage_info = {
            "memoryUsed": 0,
            "diskUsed": 0,
            "totalCached": 0,
            "details": []
        }
        
        if not self.app_id:
            self.logger.warning("No application ID available")
            return storage_info
            
        try:
            response = self._make_request(f"/api/v1/applications/{self.app_id}/storage/rdd")
            
            if response and isinstance(response, list):
                storage_info["totalCached"] = len(response)
                
                # Aggregate storage metrics
                for rdd in response:
                    storage_info["memoryUsed"] += rdd.get("memoryUsed", 0)
                    storage_info["diskUsed"] += rdd.get("diskUsed", 0)
                
                # Add detailed RDD information
                storage_info["details"] = []
                for rdd in response:
                    storage_info["details"].append({
                        "rddId": rdd.get("id"),
                        "name": rdd.get("name"),
                        "memoryUsed": rdd.get("memoryUsed"),
                        "diskUsed": rdd.get("diskUsed"),
                        "numPartitions": rdd.get("numPartitions"),
                        "numCachedPartitions": rdd.get("numCachedPartitions"),
                        "storageLevel": rdd.get("storageLevel")
                    })
                    
        except Exception as e:
            self.logger.error(f"Error getting storage info: {str(e)}")
            
        return storage_info
    
    def _get_environment_info(self) -> Dict[str, Any]:
        """Get information about the Spark environment.
        
        Returns:
            Dictionary with environment information
        """
        env_info = {
            "spark": {},
            "jvm": {},
            "system": {}
        }
        
        if not self.app_id:
            self.logger.warning("No application ID available")
            return env_info
            
        try:
            response = self._make_request(f"/api/v1/applications/{self.app_id}/environment")
            
            if response and isinstance(response, dict):
                # Extract Spark properties
                if "sparkProperties" in response:
                    props = response["sparkProperties"]
                    env_info["spark"] = {
                        "version": props.get("spark.version", ""),
                        "driver.memory": props.get("spark.driver.memory", ""),
                        "executor.memory": props.get("spark.executor.memory", ""),
                        "executor.cores": props.get("spark.executor.cores", ""),
                        "executor.instances": props.get("spark.executor.instances", ""),
                        "master": props.get("spark.master", ""),
                        "app.name": props.get("spark.app.name", ""),
                        "network.timeout": props.get("spark.network.timeout", ""),
                        "shuffle.partitions": props.get("spark.sql.shuffle.partitions", ""),
                        "serializer": props.get("spark.serializer", "")
                    }
                
                # Extract JVM information
                if "systemProperties" in response:
                    sys_props = response["systemProperties"]
                    env_info["jvm"] = {
                        "version": sys_props.get("java.version", ""),
                        "vendor": sys_props.get("java.vendor", ""),
                        "home": sys_props.get("java.home", ""),
                        "runtime_name": sys_props.get("java.runtime.name", ""),
                        "vm_name": sys_props.get("java.vm.name", ""),
                        "vm_version": sys_props.get("java.vm.version", "")
                    }
                
                # Extract system information
                if "systemProperties" in response:
                    sys_props = response["systemProperties"]
                    env_info["system"] = {
                        "os_name": sys_props.get("os.name", ""),
                        "os_version": sys_props.get("os.version", ""),
                        "os_arch": sys_props.get("os.arch", ""),
                        "user_name": sys_props.get("user.name", ""),
                        "user_dir": sys_props.get("user.dir", "")
                    }
                    
        except Exception as e:
            self.logger.error(f"Error getting environment info: {str(e)}")
            
        return env_info
    
    def _make_request(self, endpoint: str) -> Optional[Union[Dict, List]]:
        """Make an HTTP request to the Spark UI API.
        
        Args:
            endpoint: API endpoint (e.g., /api/v1/applications)
            
        Returns:
            JSON response data or None if error
        """
        url = urllib.parse.urljoin(self.base_url, endpoint)
        
        try:
            response = requests.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.warning(f"HTTP {response.status_code} from {url}: {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error for {url}: {str(e)}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error for {url}: {str(e)}")
            return None
