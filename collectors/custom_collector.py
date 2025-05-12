# spark_debugger/collectors/custom_logging.py
import os
import re
import json
import time
import logging
from typing import Dict, Any, List, Optional, Union, TextIO
from datetime import datetime, timedelta

from base_collector import BaseCollector

class LogPattern:
    """Class representing a log pattern to match and extract information."""
    
    def __init__(self, name: str, pattern: str, is_error: bool = False, log_level: str = None):
        """Initialize a log pattern.
        
        Args:
            name: Name of the pattern (used for identification)
            pattern: Regular expression pattern to match
            is_error: Whether this pattern indicates an error
            log_level: Log level for this pattern (INFO, WARN, ERROR, etc.)
        """
        self.name = name
        self.pattern = re.compile(pattern)
        self.is_error = is_error
        self.log_level = log_level
    
    def match(self, line: str) -> Optional[Dict[str, str]]:
        """Match a log line against this pattern.
        
        Args:
            line: Log line to match
            
        Returns:
            Dictionary of captured groups or None if no match
        """
        match = self.pattern.search(line)
        if match:
            return {
                "pattern": self.name,
                "is_error": self.is_error,
                "log_level": self.log_level,
                "groups": match.groupdict()
            }
        return None

class CustomLoggingCollector(BaseCollector):
    """Collects and analyzes Spark logs to identify issues.
    
    This collector parses driver and executor logs for patterns indicating
    problems and extracts structured information from them.
    """
    
    def __init__(self, log_dir: Optional[str] = None):
        """Initialize the custom logging collector.
        
        Args:
            log_dir: Directory containing Spark logs. If None, will try common locations.
        """
        super().__init__(name="custom_logging")
        self.log_dir = log_dir
        self.log_patterns = self._build_patterns()
        self.last_scan_time = 0
        self.last_seen_position = {}  # Tracks position in each log file
        self.current_job_id = None
        
    def get_supported_metrics(self) -> List[str]:
        """Return a list of metrics that this collector can provide."""
        return [
            # Exception metrics
            "exceptions.count", "exceptions.types", "exceptions.trend",
            "exceptions.latest", "exceptions.patterns",
            
            # Data quality metrics
            "data_quality.nulls", "data_quality.schema_mismatch",
            "data_quality.type_conversions", "data_quality.constraints",
            
            # General log metrics
            "logs.error_count", "logs.warning_count", "logs.shuffle_failures",
            "logs.serialization_errors", "logs.gc_overhead",
            
            # Streaming specific metrics
            "streaming.dropped_batches", "streaming.processing_time",
            "streaming.trigger_delay", "streaming.batch_size"
        ]
    
    def setup(self, context: Dict[str, Any]) -> None:
        """Set up the collector with context information.
        
        Args:
            context: Dictionary that may contain:
                    - log_dir: Override for log directory
                    - app_id: Application ID to focus on
                    - job_id: Job ID to focus on
                    - spark_session: Active Spark session
        """
        # Override log_dir if provided
        if "log_dir" in context:
            self.log_dir = context["log_dir"]
            
        # Try to find log directory if not specified
        if not self.log_dir:
            self.log_dir = self._discover_log_dir(context)
            
        # Set job ID if provided
        if "job_id" in context:
            self.current_job_id = context["job_id"]
            
        self.logger.info(f"Custom logging collector initialized with log directory: {self.log_dir}")
        
    def collect(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Collect and analyze Spark logs.
        
        Args:
            context: Dictionary containing collection context
                    May include log_dir to override the default
                    
        Returns:
            Dictionary of collected metrics
        """
        # Update setup if context contains relevant info
        if any(key in context for key in ["log_dir", "app_id", "job_id", "spark_session"]):
            self.setup(context)
            
        if not self.log_dir or not os.path.exists(self.log_dir):
            self.logger.warning(f"Log directory not found: {self.log_dir}")
            return {
                "timestamp": time.time(),
                "status": "error",
                "message": f"Log directory not found: {self.log_dir}"
            }
            
        metrics = {
            "timestamp": time.time(),
            "log_dir": self.log_dir,
            "scan_duration": 0,  # Will be updated at the end
            "exceptions": {
                "count": 0,
                "types": {},
                "latest": []
            },
            "data_quality": {
                "issues": []
            },
            "logs": {
                "error_count": 0,
                "warning_count": 0,
                "info_count": 0,
                "shuffle_failures": 0,
                "serialization_errors": 0,
                "gc_overhead": 0
            },
            "streaming": {
                "metrics": {}
            },
            "job_specific": {}
        }
        
        # Get current time before starting scan
        start_time = time.time()
        
        # Get all log files
        log_files = self._get_log_files()
        
        # Scan all log files
        for log_file in log_files:
            file_metrics = self._scan_log_file(log_file, only_new=True)
            
            # Merge file metrics into overall metrics
            metrics["exceptions"]["count"] += file_metrics["exceptions"]["count"]
            
            # Merge exception types
            for exc_type, count in file_metrics["exceptions"]["types"].items():
                if exc_type in metrics["exceptions"]["types"]:
                    metrics["exceptions"]["types"][exc_type] += count
                else:
                    metrics["exceptions"]["types"][exc_type] = count
            
            # Add latest exceptions
            metrics["exceptions"]["latest"].extend(file_metrics["exceptions"]["latest"])
            
            # Merge log counts
            metrics["logs"]["error_count"] += file_metrics["logs"]["error_count"]
            metrics["logs"]["warning_count"] += file_metrics["logs"]["warning_count"]
            metrics["logs"]["info_count"] += file_metrics["logs"]["info_count"]
            metrics["logs"]["shuffle_failures"] += file_metrics["logs"]["shuffle_failures"]
            metrics["logs"]["serialization_errors"] += file_metrics["logs"]["serialization_errors"]
            metrics["logs"]["gc_overhead"] += file_metrics["logs"]["gc_overhead"]
            
            # Merge data quality issues
            metrics["data_quality"]["issues"].extend(file_metrics["data_quality"]["issues"])
            
            # Merge streaming metrics if available
            if file_metrics["streaming"]["metrics"]:
                metrics["streaming"]["metrics"].update(file_metrics["streaming"]["metrics"])
        
        # Sort latest exceptions by timestamp (newest first)
        metrics["exceptions"]["latest"] = sorted(
            metrics["exceptions"]["latest"],
            key=lambda e: e.get("timestamp", 0),
            reverse=True
        )
        
        # Limit to top 10 exceptions
        metrics["exceptions"]["latest"] = metrics["exceptions"]["latest"][:10]
        
        # Calculate scan duration
        metrics["scan_duration"] = time.time() - start_time
        
        # Add job-specific information if available
        if self.current_job_id:
            metrics["job_specific"] = self._extract_job_specific_logs(self.current_job_id)
            
        # Update last scan time
        self.last_scan_time = start_time
        
        return metrics
    
    def check_for_critical_exceptions(self) -> Optional[Dict[str, Any]]:
        """Check for critical exceptions in logs and return details if any.
    
        Returns:
            Dictionary with exception details or None if no critical exceptions
        """
        # Check for exceptions in the most recent logs
        context = {"only_new": True}  # Only check new log entries since last scan
        metrics = self.collect(context)
        
        critical_exceptions = []
        critical_patterns = [
            "OutOfMemoryError", 
            "SparkException",
            "ExecutorLostFailure",
            "FetchFailedException",
            "TaskKilledException"
        ]
        
        # Check latest exceptions
        for exception in metrics.get("exceptions", {}).get("latest", []):
            exception_type = exception.get("type", "Unknown")
            exception_message = exception.get("message", "")
            
            # Check if this is a critical exception
            is_critical = any(pattern in exception_type or pattern in exception_message 
                            for pattern in critical_patterns)
            
            if is_critical:
                self.logger.info(f"Detected critical exception: {exception_type}")
                critical_exceptions.append(exception)
        
        if critical_exceptions:
            return {
                "timestamp": time.time(),
                "num_critical_exceptions": len(critical_exceptions),
                "exceptions": critical_exceptions
            }
        
        return None
    def _get_log_files(self) -> List[str]:
        """Get all log files in the log directory.
        
        Returns:
            List of log file paths
        """
        log_files = []
        
        if not self.log_dir or not os.path.exists(self.log_dir):
            return log_files
            
        # Walk through the directory to find all log files
        for root, _, files in os.walk(self.log_dir):
            for file in files:
                if file.endswith(".log") or "stderr" in file or "stdout" in file:
                    log_files.append(os.path.join(root, file))
        
        return log_files
    
    def _scan_log_file(self, log_file: str, only_new: bool = True) -> Dict[str, Any]:
        """Scan a log file for patterns.
        
        Args:
            log_file: Path to the log file
            only_new: If True, only scan new content since last scan
            
        Returns:
            Dictionary of metrics extracted from the log file
        """
        metrics = {
            "file": log_file,
            "exceptions": {
                "count": 0,
                "types": {},
                "latest": []
            },
            "data_quality": {
                "issues": []
            },
            "logs": {
                "error_count": 0,
                "warning_count": 0,
                "info_count": 0,
                "shuffle_failures": 0,
                "serialization_errors": 0,
                "gc_overhead": 0
            },
            "streaming": {
                "metrics": {}
            }
        }
        
        # Check if the file exists
        if not os.path.exists(log_file):
            self.logger.warning(f"Log file not found: {log_file}")
            return metrics
            
        # Determine where to start scanning
        start_position = 0
        if only_new and log_file in self.last_seen_position:
            start_position = self.last_seen_position[log_file]
            
        try:
            with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                # Skip to the last position if needed
                if start_position > 0:
                    f.seek(start_position)
                
                # Process the file
                metrics = self._process_log_stream(f, metrics)
                
                # Update the last seen position
                self.last_seen_position[log_file] = f.tell()
                
        except Exception as e:
            self.logger.error(f"Error scanning log file {log_file}: {str(e)}")
            
        return metrics
    
    def _process_log_stream(self, log_stream: TextIO, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Process a log stream (file or buffer) for patterns.
        
        Args:
            log_stream: Text stream to process
            metrics: Metrics dictionary to update
            
        Returns:
            Updated metrics dictionary
        """
        # Variables to track multi-line exceptions
        in_exception = False
        current_exception = {
            "type": "Unknown",
            "message": "",
            "timestamp": 0,
            "stack_trace": []
        }
        
        # Process each line
        for line in log_stream:
            # Check for log levels
            if "ERROR" in line:
                metrics["logs"]["error_count"] += 1
            elif "WARN" in line:
                metrics["logs"]["warning_count"] += 1
            elif "INFO" in line:
                metrics["logs"]["info_count"] += 1
                
            # Try to parse timestamp from the line
            timestamp = self._extract_timestamp(line)
            
            # Check if this is the start of an exception
            exception_match = re.search(r"Exception|Error|Throwable|Failed", line)
            if exception_match and not in_exception:
                in_exception = True
                current_exception = {
                    "type": "Unknown",
                    "message": line.strip(),
                    "timestamp": timestamp or time.time(),
                    "stack_trace": []
                }
                
                # Try to extract the exception type
                exc_type_match = re.search(r"([\w\.]+Exception|[\w\.]+Error|[\w\.]+Throwable)", line)
                if exc_type_match:
                    current_exception["type"] = exc_type_match.group(1)
                    
                    # Update exception type counter
                    exc_type = current_exception["type"]
                    if exc_type in metrics["exceptions"]["types"]:
                        metrics["exceptions"]["types"][exc_type] += 1
                    else:
                        metrics["exceptions"]["types"][exc_type] = 1
            
            # If we're in an exception, collect the stack trace
            elif in_exception:
                current_exception["stack_trace"].append(line.strip())
                
                # Check if this is the end of the exception
                if not line.startswith(" ") and not line.startswith("\t") and line.strip() and not re.search(r"at [\w\.]+\(", line):
                    in_exception = False
                    
                    # Add the exception to the latest list
                    metrics["exceptions"]["count"] += 1
                    metrics["exceptions"]["latest"].append(current_exception)
            
            # Match specific patterns
            for pattern in self.log_patterns:
                match_result = pattern.match(line)
                if match_result:
                    # Handle different pattern types
                    pattern_name = match_result["pattern"]
                    
                    if pattern_name == "gc_overhead":
                        metrics["logs"]["gc_overhead"] += 1
                    elif pattern_name == "shuffle_failure":
                        metrics["logs"]["shuffle_failures"] += 1
                    elif pattern_name == "serialization_error":
                        metrics["logs"]["serialization_errors"] += 1
                    elif pattern_name.startswith("data_quality"):
                        # Add data quality issue
                        metrics["data_quality"]["issues"].append({
                            "type": pattern_name.replace("data_quality_", ""),
                            "message": line.strip(),
                            "timestamp": timestamp or time.time(),
                            "details": match_result["groups"]
                        })
                    elif pattern_name.startswith("streaming"):
                        # Extract streaming metrics
                        stream_metric = pattern_name.replace("streaming_", "")
                        
                        if stream_metric not in metrics["streaming"]["metrics"]:
                            metrics["streaming"]["metrics"][stream_metric] = []
                            
                        metrics["streaming"]["metrics"][stream_metric].append({
                            "timestamp": timestamp or time.time(),
                            "value": match_result["groups"].get("value"),
                            "details": match_result["groups"]
                        })
        
        # If we were in an exception when the file ended, add it
        if in_exception:
            metrics["exceptions"]["count"] += 1
            metrics["exceptions"]["latest"].append(current_exception)
            
        return metrics
    
    def _extract_job_specific_logs(self, job_id: str) -> Dict[str, Any]:
        """Extract logs specific to a job ID.
        
        Args:
            job_id: Job ID to focus on
            
        Returns:
            Dictionary of job-specific metrics
        """
        job_metrics = {
            "logs": [],
            "exceptions": [],
            "stages": {}
        }
        
        # Get all log files
        log_files = self._get_log_files()
        
        # Regular expression to match job IDs
        job_id_pattern = re.compile(r"job[_\s]+(?P<job_id>\d+)")
        stage_id_pattern = re.compile(r"stage[_\s]+(?P<stage_id>\d+)")
        
        # Scan all log files
        for log_file in log_files:
            try:
                with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                    for line in f:
                        # Check if this line mentions the job ID
                        job_match = job_id_pattern.search(line)
                        if job_match and job_match.group("job_id") == job_id:
                            # Add to job-specific logs
                            timestamp = self._extract_timestamp(line)
                            job_metrics["logs"].append({
                                "timestamp": timestamp or 0,
                                "message": line.strip(),
                                "file": log_file
                            })
                            
                            # Check if this line mentions a stage
                            stage_match = stage_id_pattern.search(line)
                            if stage_match:
                                stage_id = stage_match.group("stage_id")
                                
                                if stage_id not in job_metrics["stages"]:
                                    job_metrics["stages"][stage_id] = {
                                        "logs": [],
                                        "exceptions": []
                                    }
                                
                                job_metrics["stages"][stage_id]["logs"].append({
                                    "timestamp": timestamp or 0,
                                    "message": line.strip(),
                                    "file": log_file
                                })
                                
                            # Check if this line mentions an exception
                            if "Exception" in line or "Error" in line:
                                job_metrics["exceptions"].append({
                                    "timestamp": timestamp or 0,
                                    "message": line.strip(),
                                    "file": log_file
                                })
                                
                                # If it also mentions a stage, add to stage exceptions
                                if stage_match:
                                    stage_id = stage_match.group("stage_id")
                                    job_metrics["stages"][stage_id]["exceptions"].append({
                                        "timestamp": timestamp or 0,
                                        "message": line.strip(),
                                        "file": log_file
                                    })
                        
            except Exception as e:
                self.logger.error(f"Error extracting job logs from {log_file}: {str(e)}")
        
        # Sort logs and exceptions by timestamp
        job_metrics["logs"] = sorted(job_metrics["logs"], key=lambda x: x["timestamp"])
        job_metrics["exceptions"] = sorted(job_metrics["exceptions"], key=lambda x: x["timestamp"])
        
        # Sort stage logs and exceptions by timestamp
        for stage_id in job_metrics["stages"]:
            job_metrics["stages"][stage_id]["logs"] = sorted(
                job_metrics["stages"][stage_id]["logs"],
                key=lambda x: x["timestamp"]
            )
            job_metrics["stages"][stage_id]["exceptions"] = sorted(
                job_metrics["stages"][stage_id]["exceptions"],
                key=lambda x: x["timestamp"]
            )
        
        return job_metrics
    
    def _extract_timestamp(self, line: str) -> Optional[float]:
        """Extract timestamp from a log line.
        
        Args:
            line: Log line to extract timestamp from
            
        Returns:
            Timestamp as a float or None if not found
        """
        # Try different timestamp patterns
        timestamp_patterns = [
            # ISO format: 2023-05-09T14:32:05.123Z
            r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?)",
            # Common log format: [2023-05-09 14:32:05,123]
            r"\[(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}(,\d+)?)\]",
            # Simple format: 2023/05/09 14:32:05
            r"(\d{4}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2})"
        ]
        
        for pattern in timestamp_patterns:
            match = re.search(pattern, line)
            if match:
                try:
                    # Try to parse the timestamp
                    timestamp_str = match.group(1)
                    
                    # Handle different formats
                    if "T" in timestamp_str:
                        # ISO format
                        if "Z" not in timestamp_str:
                            timestamp_str += "Z"
                        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                    elif "[" in line:
                        # Common log format
                        timestamp_str = timestamp_str.replace(",", ".")
                        dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f" if "." in timestamp_str else "%Y-%m-%d %H:%M:%S")
                    else:
                        # Simple format
                        dt = datetime.strptime(timestamp_str, "%Y/%m/%d %H:%M:%S")
                        
                    return dt.timestamp()
                except (ValueError, TypeError):
                    # If parsing fails, try next pattern
                    continue
        
        return None
    
    def _discover_log_dir(self, context: Dict[str, Any]) -> Optional[str]:
        """Try to discover the Spark log directory.
        
        Args:
            context: Context information that might help locate the logs
            
        Returns:
            Log directory path or None if not found
        """
        # Try to get log directory from Spark session
        if "spark_session" in context:
            spark = context["spark_session"]
            
            try:
                # Get Spark configuration
                log_dir = spark.conf.get("spark.eventLog.dir")
                if log_dir and os.path.exists(log_dir):
                    return log_dir
            except:
                pass
        
        # Try common Spark log locations
        common_locations = [
            os.path.join(os.getcwd(), "spark-logs"),
            os.path.join(os.getcwd(), "logs"),
            "/tmp/spark-events",
            "/var/log/spark",
            os.path.expanduser("~/spark/logs"),
            os.path.expanduser("~/.spark/logs")
        ]
        
        # Check for SPARK_LOG_DIR environment variable
        if "SPARK_LOG_DIR" in os.environ:
            common_locations.insert(0, os.environ["SPARK_LOG_DIR"])
        
        # Check for SPARK_HOME environment variable
        if "SPARK_HOME" in os.environ:
            spark_home_logs = os.path.join(os.environ["SPARK_HOME"], "logs")
            common_locations.insert(0, spark_home_logs)
        
        # Try each location
        for location in common_locations:
            if os.path.exists(location) and os.path.isdir(location):
                return location
        
        return None
    
    def _build_patterns(self) -> List[LogPattern]:
        """Build a list of log patterns to match.
        
        Returns:
            List of LogPattern objects
        """
        patterns = []
        
        # Exception patterns
        patterns.append(LogPattern(
            "java_exception",
            r"Exception in thread .*?: ([\w\.]+Exception):(.*)",
            is_error=True,
            log_level="ERROR"
        ))
        
        patterns.append(LogPattern(
            "python_exception",
            r"Traceback \(most recent call last\):(.*?)([\w\.]+Error|[\w\.]+Exception):(.*)",
            is_error=True,
            log_level="ERROR"
        ))
        
        # GC overhead patterns
        patterns.append(LogPattern(
            "gc_overhead",
            r"(java\.lang\.OutOfMemoryError: GC overhead limit exceeded|Executor heartbeat timed out after \d+ ms)",
            is_error=True,
            log_level="ERROR"
        ))
        
        # Shuffle failure patterns
        patterns.append(LogPattern(
            "shuffle_failure",
            r"(Failed to fetch shuffle block|FetchFailedException|shuffle data of [^ ]+ not available|ShuffleMapStage \d+ failed|shuffle file not found)",
            is_error=True,
            log_level="ERROR"
        ))
        
        # Serialization error patterns
        patterns.append(LogPattern(
            "serialization_error",
            r"(Task not serializable|NotSerializableException|org\.apache\.spark\.SparkException: Task not serializable)",
            is_error=True,
            log_level="ERROR"
        ))
        
        # Data skew patterns
        patterns.append(LogPattern(
            "data_skew",
            r"Stage (?P<stage_id>\d+) contains a task of very large size \((?P<size>[\d\.]+) (?P<unit>\w+)\)",
            is_error=False,
            log_level="WARN"
        ))
        
        # Data quality patterns
        patterns.append(LogPattern(
            "data_quality_nulls",
            r"Nulls found in column '(?P<column>[^']+)'(?: \(count: (?P<count>\d+)\))?",
            is_error=False,
            log_level="WARN"
        ))
        
        patterns.append(LogPattern(
            "data_quality_schema_mismatch",
            r"Schema mismatch detected: expected '(?P<expected>[^']+)' but got '(?P<actual>[^']+)'",
            is_error=True,
            log_level="ERROR"
        ))
        
        patterns.append(LogPattern(
            "data_quality_type_conversion",
            r"Cannot convert value '(?P<value>[^']+)' to type (?P<type>\w+)",
            is_error=True,
            log_level="ERROR"
        ))
        
        # Streaming patterns
        patterns.append(LogPattern(
            "streaming_processing_time",
            r"Streaming query made progress:.*?batchDuration=(?P<value>[\d\.]+)(?P<unit>ms|s)",
            is_error=False,
            log_level="INFO"
        ))
        
        patterns.append(LogPattern(
            "streaming_input_rate",
            r"Streaming query made progress:.*?inputRowsPerSecond=(?P<value>[\d\.]+)",
            is_error=False,
            log_level="INFO"
        ))
        
        patterns.append(LogPattern(
            "streaming_processing_rate",
            r"Streaming query made progress:.*?processedRowsPerSecond=(?P<value>[\d\.]+)",
            is_error=False,
            log_level="INFO"
        ))
        
        patterns.append(LogPattern(
            "streaming_trigger_delay",
            r"Streaming query made progress:.*?triggerExecution=(?P<value>[\d\.]+)(?P<unit>ms|s)",
            is_error=False,
            log_level="INFO"
        ))
        
        patterns.append(LogPattern(
            "streaming_batch_size",
            r"Streaming query made progress:.*?numInputRows=(?P<value>\d+)",
            is_error=False,
            log_level="INFO"
        ))
        
        patterns.append(LogPattern(
            "streaming_watermark",
            r"Streaming query made progress:.*?watermark=(?P<value>[\d\-\s:\.]+)",
            is_error=False,
            log_level="INFO"
        ))
        
        patterns.append(LogPattern(
            "streaming_data_loss",
            r"(some data may have been lost|cannot find offset|no available offset|offsetOutOfRange)",
            is_error=True,
            log_level="ERROR"
        ))
        
        return patterns
