from abc import ABC, abstractmethod
import time
import logging
from typing import Dict, Any, Optional, List

class BaseCollector(ABC):
    """Abstract base class for all metric collectors in the Spark Pipeline Debugger."""
    
    def __init__(self, name: str):
        """Initialize the collector with a name.
        
        Args:
            name: Unique name for this collector
        """
        self.name = name
        self.logger = logging.getLogger(f"spark_debugger.collectors.{name}")
        self._metrics_cache = {}
        self._last_collection_time = 0
    
    @abstractmethod
    def collect(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Collect metrics from the source.
        
        Args:
            context: Dictionary containing relevant context for collection
                     (e.g., spark_session, application_id, etc.)
                     
        Returns:
            Dictionary of collected metrics
        """
        pass
    
    @abstractmethod
    def get_supported_metrics(self) -> List[str]:
        """Return a list of metrics that this collector can provide."""
        pass
    
    def collect_with_cache(self, context: Dict[str, Any], max_cache_age_seconds: int = 5) -> Dict[str, Any]:
        """Collect metrics with caching to avoid repeated calls in a short time period.
        
        Args:
            context: Collection context
            max_cache_age_seconds: Maximum age of cached metrics in seconds
            
        Returns:
            Dictionary of collected metrics (from cache or fresh collection)
        """
        current_time = time.time()
        if current_time - self._last_collection_time > max_cache_age_seconds:
            try:
                self._metrics_cache = self.collect(context)
                self._last_collection_time = current_time
                self.logger.debug(f"Collected fresh metrics for {self.name}")
            except Exception as e:
                self.logger.error(f"Error collecting metrics for {self.name}: {str(e)}")
                # If we have no cached metrics, raise the exception
                if not self._metrics_cache:
                    raise
        else:
            self.logger.debug(f"Using cached metrics for {self.name} (age: {current_time - self._last_collection_time:.2f}s)")
            
        return self._metrics_cache
    
    def get_metric(self, metric_name: str, context: Dict[str, Any] = None) -> Optional[Any]:
        """Get a specific metric value.
        
        Args:
            metric_name: Name of the metric to retrieve
            context: Optional context for collection if not using cache
            
        Returns:
            The metric value or None if not available
        """
        metrics = self._metrics_cache
        if context is not None:
            metrics = self.collect_with_cache(context)
            
        # Handle nested metric names with dot notation (e.g., "memory.heap.used")
        parts = metric_name.split('.')
        value = metrics
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return None
                
        return value
