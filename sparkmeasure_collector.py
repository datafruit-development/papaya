# spark_debugger/collectors/spark_measure.py
import json
import time
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np

from base_collector import BaseCollector

class SparkMeasureCollector(BaseCollector):
    """Collects metrics from Spark using the SparkMeasure library.
    
    This collector provides detailed task, stage, and executor metrics
    by leveraging SparkMeasure's instrumentation capabilities.
    """
    
    def __init__(self, mode: str = "stage"):
        """Initialize the SparkMeasure collector.
        
        Args:
            mode: Collection mode, either "stage" or "task" (default: "stage")
                 "stage" mode is more lightweight, "task" gives more detailed metrics
        """
        super().__init__(name="spark_measure")
        self.mode = mode
        self.stage_metrics = None
        self.task_metrics = None
        self.current_job_id = None
        self.metrics_dfs = {}
        
    def setup(self, context: Dict[str, Any]) -> None:
        """Set up the SparkMeasure collector with a Spark session.
        
        Args:
            context: Dictionary containing at least "spark_session"
        """
        if "spark_session" not in context:
            raise ValueError("spark_session is required in context")
            
        spark = context["spark_session"]
        
        try:
            # Import SparkMeasure and initialize collectors
            from sparkmeasure import StageMetrics, TaskMetrics
            
            if self.mode == "stage" or self.mode == "both":
                self.stage_metrics = StageMetrics(spark)
                
            if self.mode == "task" or self.mode == "both":
                self.task_metrics = TaskMetrics(spark)
                
            self.logger.info(f"SparkMeasure {self.mode} metrics collector initialized")
            
        except ImportError:
            self.logger.error("Failed to import sparkmeasure. Make sure it's installed: pip install sparkmeasure")
            raise
    
    def get_supported_metrics(self) -> List[str]:
        """Return a list of metrics that this collector can provide."""
        return [
            # Stage-level metrics
            "stage.count", "stage.duration", "stage.executorRunTime", "stage.executorCpuTime",
            "stage.executorDeserializeTime", "stage.executorDeserializeCpuTime",
            "stage.resultSerializationTime", "stage.jvmGCTime", "stage.shuffleFetchWaitTime",
            "stage.shuffleWriteTime", "stage.resultSize", "stage.diskBytesSpilled",
            "stage.memoryBytesSpilled", "stage.peakExecutionMemory", "stage.recordsRead",
            "stage.bytesRead", "stage.recordsWritten", "stage.bytesWritten",
            "stage.shuffleRecordsRead", "stage.shuffleTotalBytesRead", "stage.shuffleBytesWritten",
            "stage.shuffleRecordsWritten",
            
            # Task-level metrics
            "task.count", "task.duration", "task.gcTime", "task.executorRunTime",
            "task.executorCpuTime", "task.bytesRead", "task.recordsRead",
            "task.bytesWritten", "task.recordsWritten", "task.shuffleBytesRead",
            "task.shuffleRecordsRead", "task.shuffleBytesWritten", "task.shuffleRecordsWritten",
            "task.memoryBytesSpilled", "task.diskBytesSpilled",
            
            # Derived metrics
            "derived.dataSkew", "derived.gcPressure", "derived.shuffleIntensity",
            "derived.taskFailureRate"
        ]
    
    def begin_collection(self, job_id: Optional[str] = None) -> None:
        """Begin collecting metrics for a specific job.
        
        Args:
            job_id: Optional identifier for the job
        """
        self.current_job_id = job_id or f"job_{int(time.time())}"
        
        if self.stage_metrics:
            self.stage_metrics.begin()
            
        if self.task_metrics:
            self.task_metrics.begin()
            
        self.logger.info(f"Started metrics collection for job {self.current_job_id}")
    
    def end_collection(self) -> Dict[str, Any]:
        """End collection and return the collected metrics.
        
        Returns:
            Dictionary of collected metrics
        """
        if not self.current_job_id:
            self.logger.warning("No active collection to end")
            return {}
            
        metrics = {"job_id": self.current_job_id}
        
        if self.stage_metrics:
            self.stage_metrics.end()
            metrics["stage"] = self._process_stage_metrics()
            
        if self.task_metrics:
            self.task_metrics.end()
            metrics["task"] = self._process_task_metrics()
            
        # Add derived metrics
        metrics["derived"] = self._calculate_derived_metrics(metrics)
        
        self.logger.info(f"Ended metrics collection for job {self.current_job_id}")
        self.current_job_id = None
        
        return metrics
    
    def collect(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Collect current metrics without begin/end cycle.
        
        This can be used for spot checks during job execution.
        
        Args:
            context: Dictionary containing collection context
            
        Returns:
            Dictionary of current metrics
        """
        if "spark_session" in context and (self.stage_metrics is None and self.task_metrics is None):
            self.setup(context)
            
        metrics = {}
        
        if self.stage_metrics:
            metrics["stage"] = self._get_current_stage_metrics()
            
        if self.task_metrics:
            metrics["task"] = self._get_current_task_metrics()
            
        # Add derived metrics
        metrics["derived"] = self._calculate_derived_metrics(metrics)
            
        return metrics
    
    def _process_stage_metrics(self) -> Dict[str, Any]:
        """Process the collected stage metrics.
        
        Returns:
            Dictionary of processed stage metrics
        """
        metrics_data = self.stage_metrics.aggregate_metrics()
        
        # Convert to a more accessible dictionary
        metrics = {}
        for key, value in metrics_data.items():
            metrics[key] = value
            
        # Create DataFrames for more detailed analysis
        try:
            stage_metrics_df = self.stage_metrics.create_stagemetrics_DF()
            self.metrics_dfs["stage"] = stage_metrics_df
            
            # Calculate statistics on stage durations
            if len(stage_metrics_df) > 0:
                metrics["duration_stats"] = {
                    "min": stage_metrics_df.select("duration").agg({"duration": "min"}).collect()[0][0],
                    "max": stage_metrics_df.select("duration").agg({"duration": "max"}).collect()[0][0],
                    "mean": stage_metrics_df.select("duration").agg({"duration": "avg"}).collect()[0][0],
                    "count": stage_metrics_df.count()
                }
                
                # Identify stages with high memory spill
                memory_spill = stage_metrics_df.select("memoryBytesSpilled", "diskBytesSpilled")
                metrics["memory_spill"] = {
                    "total_memory_spilled": memory_spill.agg({"memoryBytesSpilled": "sum"}).collect()[0][0],
                    "total_disk_spilled": memory_spill.agg({"diskBytesSpilled": "sum"}).collect()[0][0]
                }
                
                # Get the memory report
                mem_metrics = self.stage_metrics.print_memory_report()
                metrics["memory_report"] = mem_metrics
                
        except Exception as e:
            self.logger.error(f"Error processing stage metrics: {str(e)}")
            
        return metrics
    
    def _process_task_metrics(self) -> Dict[str, Any]:
        """Process the collected task metrics.
        
        Returns:
            Dictionary of processed task metrics
        """
        metrics_data = self.task_metrics.aggregate_metrics()
        
        # Convert to a more accessible dictionary
        metrics = {}
        for key, value in metrics_data.items():
            metrics[key] = value
            
        # Create DataFrames for more detailed analysis
        try:
            task_metrics_df = self.task_metrics.create_taskmetrics_DF()
            self.metrics_dfs["task"] = task_metrics_df
            
            # Calculate task duration statistics to detect skew
            if len(task_metrics_df) > 0:
                # Convert to pandas for more advanced statistics
                pdf = task_metrics_df.select("duration", "executorId", "host", "stageid").toPandas()
                
                # Calculate skew metrics
                metrics["duration_stats"] = {
                    "min": pdf["duration"].min(),
                    "max": pdf["duration"].max(),
                    "mean": pdf["duration"].mean(),
                    "median": pdf["duration"].median(),
                    "stddev": pdf["duration"].std(),
                    "count": len(pdf),
                    "skew_ratio": pdf["duration"].max() / pdf["duration"].mean() if pdf["duration"].mean() > 0 else 0
                }
                
                # Group by executor to see per-executor performance
                per_executor = pdf.groupby("executorId").agg({
                    "duration": ["sum", "mean", "count"]
                }).reset_index()
                
                # Convert to native Python types for JSON serialization
                metrics["per_executor"] = json.loads(per_executor.to_json(orient="records"))
                
                # Identify straggler tasks (those taking significantly longer than average)
                threshold = pdf["duration"].mean() + 2 * pdf["duration"].std()
                stragglers = pdf[pdf["duration"] > threshold]
                
                if len(stragglers) > 0:
                    metrics["stragglers"] = {
                        "count": len(stragglers),
                        "percentage": 100 * len(stragglers) / len(pdf),
                        "details": json.loads(stragglers.head(10).to_json(orient="records"))
                    }
                    
        except Exception as e:
            self.logger.error(f"Error processing task metrics: {str(e)}")
            
        return metrics
    
    def _get_current_stage_metrics(self) -> Dict[str, Any]:
        """Get current stage metrics without ending collection.
        
        Returns:
            Dictionary of current stage metrics
        """
        metrics = {}
        
        try:
            # Get the current metrics data
            metrics_data = self.stage_metrics.create_metrics_data()
            for key, value in metrics_data.items():
                metrics[key] = value
                
        except Exception as e:
            self.logger.error(f"Error getting current stage metrics: {str(e)}")
            
        return metrics
    
    def _get_current_task_metrics(self) -> Dict[str, Any]:
        """Get current task metrics without ending collection.
        
        Returns:
            Dictionary of current task metrics
        """
        metrics = {}
        
        try:
            # Get the current metrics data
            metrics_data = self.task_metrics.create_metrics_data()
            for key, value in metrics_data.items():
                metrics[key] = value
                
        except Exception as e:
            self.logger.error(f"Error getting current task metrics: {str(e)}")
            
        return metrics
    
    def _calculate_derived_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate derived metrics from the raw metrics.
        
        Args:
            metrics: Dictionary of raw metrics
            
        Returns:
            Dictionary of derived metrics
        """
        derived = {}
        
        try:
            # Calculate data skew metric if we have task duration stats
            if "task" in metrics and "duration_stats" in metrics["task"]:
                stats = metrics["task"]["duration_stats"]
                derived["dataSkew"] = stats["skew_ratio"]
                
                # Classify skew level
                if derived["dataSkew"] > 10:
                    derived["skewLevel"] = "severe"
                elif derived["dataSkew"] > 5:
                    derived["skewLevel"] = "high"
                elif derived["dataSkew"] > 3:
                    derived["skewLevel"] = "moderate"
                else:
                    derived["skewLevel"] = "low"
            
            # Calculate GC pressure metric
            if "stage" in metrics and "jvmGCTime" in metrics["stage"] and "executorRunTime" in metrics["stage"]:
                gc_time = metrics["stage"]["jvmGCTime"]
                run_time = metrics["stage"]["executorRunTime"]
                
                if run_time > 0:
                    derived["gcPressure"] = gc_time / run_time
                    
                    # Classify GC pressure level
                    if derived["gcPressure"] > 0.3:
                        derived["gcLevel"] = "severe"
                    elif derived["gcPressure"] > 0.1:
                        derived["gcLevel"] = "high"
                    elif derived["gcPressure"] > 0.05:
                        derived["gcLevel"] = "moderate"
                    else:
                        derived["gcLevel"] = "low"
            
            # Calculate shuffle intensity metric
            if "stage" in metrics and "shuffleTotalBytesRead" in metrics["stage"] and "bytesRead" in metrics["stage"]:
                shuffle_bytes = metrics["stage"].get("shuffleTotalBytesRead", 0)
                input_bytes = metrics["stage"].get("bytesRead", 0)
                
                total_bytes = shuffle_bytes + input_bytes
                if total_bytes > 0:
                    derived["shuffleIntensity"] = shuffle_bytes / total_bytes
                    
                    # Classify shuffle intensity level
                    if derived["shuffleIntensity"] > 0.8:
                        derived["shuffleLevel"] = "very_high"
                    elif derived["shuffleIntensity"] > 0.5:
                        derived["shuffleLevel"] = "high"
                    elif derived["shuffleIntensity"] > 0.2:
                        derived["shuffleLevel"] = "moderate"
                    else:
                        derived["shuffleLevel"] = "low"
                        
            # Add memory spill ratio if available
            if "stage" in metrics and "memory_spill" in metrics["stage"]:
                memory_spilled = metrics["stage"]["memory_spill"]["total_memory_spilled"]
                disk_spilled = metrics["stage"]["memory_spill"]["total_disk_spilled"]
                
                derived["memorySpill"] = {
                    "total": memory_spilled + disk_spilled,
                    "disk_ratio": disk_spilled / (memory_spilled + disk_spilled) if memory_spilled + disk_spilled > 0 else 0
                }
                
                # Classify memory spill level
                if derived["memorySpill"]["total"] > 1e9:  # > 1GB
                    derived["spillLevel"] = "severe"
                elif derived["memorySpill"]["total"] > 1e8:  # > 100MB
                    derived["spillLevel"] = "high"
                elif derived["memorySpill"]["total"] > 1e7:  # > 10MB
                    derived["spillLevel"] = "moderate"
                else:
                    derived["spillLevel"] = "low"
                
        except Exception as e:
            self.logger.error(f"Error calculating derived metrics: {str(e)}")
            
        return derived
