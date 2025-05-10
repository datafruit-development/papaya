#!/usr/bin/env python3
# custom_app_metrics_collector.py - Collector for application-specific metrics

import time
import logging
import json
from typing import Dict, List, Any, Optional, Union, Callable
import importlib.util
import inspect
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, avg, min as min_, max as max_, sum as sum_

from base_collector import BaseCollector

class MetricFunction:
    """Class representing a custom metric function."""
    
    def __init__(self, name: str, func: Callable, description: str = "", 
                 required_args: List[str] = None, optional_args: Dict[str, Any] = None):
        """Initialize a metric function.
        
        Args:
            name: Name of the metric function
            func: The function to call for calculating the metric
            description: Description of what the metric calculates
            required_args: List of required argument names
            optional_args: Dictionary of optional arguments with default values
        """
        self.name = name
        self.func = func
        self.description = description
        self.required_args = required_args or []
        self.optional_args = optional_args or {}

class CustomAppMetricsCollector(BaseCollector):
    """Collector for application-specific and domain-specific metrics.
    
    This collector enables custom metrics definitions targeting the specific
    application domain and business logic. It can be extended with user-defined
    metric functions and data quality rules.
    """
    
    def __init__(self, custom_metrics_path: Optional[str] = None):
        """Initialize the custom application metrics collector.
        
        Args:
            custom_metrics_path: Path to a Python file with custom metric definitions
        """
        super().__init__(name="custom_app_metrics")
        self.spark = None
        self.custom_metrics_path = custom_metrics_path
        self.metric_functions = {}
        self.quality_rules = {}
        self.last_metrics = {}
        
        # Register built-in metrics
        self._register_builtin_metrics()
        
        # Load custom metrics if provided
        if custom_metrics_path:
            self._load_custom_metrics(custom_metrics_path)
    
    def get_supported_metrics(self) -> List[str]:
        """Return a list of metrics that this collector can provide."""
        return [
            # Built-in data quality metrics
            "app.data_quality.null_percentage", 
            "app.data_quality.duplicate_percentage",
            "app.data_quality.invalid_values",
            "app.data_quality.domain_errors",
            
            # Business metrics (examples, actual metrics depend on custom definitions)
            "app.business.success_rate",
            "app.business.latency",
            "app.business.throughput",
            "app.business.error_rate",
            
            # Custom metrics (loaded from external files)
            *[f"app.custom.{name}" for name in self.metric_functions.keys()]
        ]
    
    def setup(self, context: Dict[str, Any]) -> None:
        """Set up the collector with context information.
        
        Args:
            context: Dictionary containing at least "spark_session"
                     May also include custom_metrics_path to override the default
        """
        if "spark_session" not in context:
            raise ValueError("spark_session is required for custom app metrics")
            
        self.spark = context["spark_session"]
        
        # Override custom metrics path if provided
        if "custom_metrics_path" in context:
            self.custom_metrics_path = context["custom_metrics_path"]
            # Reload custom metrics
            self._load_custom_metrics(self.custom_metrics_path)
            
        self.logger.info(f"Custom app metrics collector initialized with {len(self.metric_functions)} metric functions")
    
    def collect(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Collect application-specific metrics.
        
        Args:
            context: Dictionary containing:
                     - spark_session: Spark session (if not already set)
                     - table_name: Name of the table to analyze
                     - dataset: DataFrame to analyze
                     - schema: Optionally provide schema information
                     - custom_args: Additional arguments for custom metrics
                     
        Returns:
            Dictionary of collected metrics
        """
        # Update spark session if provided
        if "spark_session" in context and not self.spark:
            self.spark = context["spark_session"]
            
        # Get the dataset to analyze
        dataset = None
        table_name = None
        
        if "dataset" in context:
            dataset = context["dataset"]
        elif "table_name" in context and self.spark:
            table_name = context["table_name"]
            try:
                dataset = self.spark.table(table_name)
            except Exception as e:
                self.logger.error(f"Error loading table {table_name}: {str(e)}")
                
        if dataset is None:
            self.logger.error("No dataset or table_name provided")
            return {
                "timestamp": time.time(),
                "status": "error",
                "message": "No dataset or table_name provided"
            }
            
        # Get schema info if provided
        schema_info = context.get("schema", None)
        if schema_info is None and dataset is not None:
            schema_info = self._extract_schema_info(dataset)
            
        # Get custom arguments
        custom_args = context.get("custom_args", {})
        
        # Prepare metrics result
        metrics = {
            "timestamp": time.time(),
            "source": table_name or "dataset",
            "data_quality": {},
            "business": {},
            "custom": {}
        }
        
        # Calculate data quality metrics
        metrics["data_quality"] = self._calculate_data_quality_metrics(dataset, schema_info)
        
        # Calculate domain-specific metrics
        metrics["business"] = self._calculate_business_metrics(dataset, custom_args)
        
        # Calculate custom metrics
        metrics["custom"] = self._calculate_custom_metrics(dataset, custom_args)
        
        # Update last metrics
        self.last_metrics = metrics
        
        return metrics
    
    def _register_builtin_metrics(self) -> None:
        """Register built-in metric functions."""
        # Register data quality metrics
        self.metric_functions["null_percentage"] = MetricFunction(
            name="null_percentage",
            func=self._calculate_null_percentage,
            description="Calculate percentage of null values in specified columns",
            required_args=["dataset"],
            optional_args={"columns": None}
        )
        
        self.metric_functions["duplicate_percentage"] = MetricFunction(
            name="duplicate_percentage",
            func=self._calculate_duplicate_percentage,
            description="Calculate percentage of duplicate rows based on specified columns",
            required_args=["dataset"],
            optional_args={"columns": None}
        )
        
        self.metric_functions["value_distribution"] = MetricFunction(
            name="value_distribution",
            func=self._calculate_value_distribution,
            description="Calculate value distribution for a column",
            required_args=["dataset", "column"],
            optional_args={"max_categories": 20}
        )
        
        self.metric_functions["numeric_stats"] = MetricFunction(
            name="numeric_stats",
            func=self._calculate_numeric_stats,
            description="Calculate statistics for numeric columns",
            required_args=["dataset"],
            optional_args={"columns": None}
        )
    
    def _load_custom_metrics(self, metrics_path: str) -> None:
        """Load custom metric functions from a Python file.
        
        Args:
            metrics_path: Path to a Python file with custom metric definitions
        """
        if not os.path.exists(metrics_path):
            self.logger.error(f"Custom metrics file not found: {metrics_path}")
            return
            
        try:
            # Load the module
            spec = importlib.util.spec_from_file_location("custom_metrics", metrics_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Find metric functions
            for name, obj in inspect.getmembers(module):
                # Look for functions decorated with @metric or @quality_rule
                if hasattr(obj, "_is_metric") and callable(obj):
                    metric_name = getattr(obj, "_metric_name", name)
                    description = getattr(obj, "_description", "")
                    required_args = getattr(obj, "_required_args", ["dataset"])
                    optional_args = getattr(obj, "_optional_args", {})
                    
                    self.metric_functions[metric_name] = MetricFunction(
                        name=metric_name,
                        func=obj,
                        description=description,
                        required_args=required_args,
                        optional_args=optional_args
                    )
                    
                    self.logger.info(f"Registered custom metric: {metric_name}")
                
                # Look for quality rules
                elif hasattr(obj, "_is_quality_rule") and callable(obj):
                    rule_name = getattr(obj, "_rule_name", name)
                    description = getattr(obj, "_description", "")
                    required_args = getattr(obj, "_required_args", ["dataset"])
                    optional_args = getattr(obj, "_optional_args", {})
                    
                    self.quality_rules[rule_name] = MetricFunction(
                        name=rule_name,
                        func=obj,
                        description=description,
                        required_args=required_args,
                        optional_args=optional_args
                    )
                    
                    self.logger.info(f"Registered quality rule: {rule_name}")
                    
        except Exception as e:
            self.logger.error(f"Error loading custom metrics: {str(e)}")
    
    def _extract_schema_info(self, dataset: DataFrame) -> Dict[str, Any]:
        """Extract schema information from a dataset.
        
        Args:
            dataset: DataFrame to extract schema from
            
        Returns:
            Dictionary with schema information
        """
        schema = dataset.schema
        schema_info = {
            "fields": [
                {
                    "name": field.name,
                    "type": str(field.dataType),
                    "nullable": field.nullable,
                    "metadata": dict(field.metadata) if field.metadata else {}
                }
                for field in schema.fields
            ],
            "column_names": [field.name for field in schema.fields],
            "numeric_columns": [
                field.name for field in schema.fields
                if str(field.dataType).startswith(("IntegerType", "LongType", "DoubleType", "FloatType", "DecimalType"))
            ],
            "string_columns": [
                field.name for field in schema.fields
                if str(field.dataType).startswith("StringType")
            ],
            "timestamp_columns": [
                field.name for field in schema.fields
                if str(field.dataType).startswith(("TimestampType", "DateType"))
            ]
        }
        
        return schema_info
    
    def _calculate_data_quality_metrics(self, dataset: DataFrame, schema_info: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate data quality metrics.
        
        Args:
            dataset: DataFrame to analyze
            schema_info: Schema information
            
        Returns:
            Dictionary with data quality metrics
        """
        metrics = {}
        
        try:
            # Get all column names
            columns = schema_info["column_names"]
            
            # Calculate null percentage for each column
            null_metrics = self._calculate_null_percentage(dataset, columns)
            metrics["null_percentage"] = null_metrics
            
            # Calculate overall null rate
            if null_metrics and "column_metrics" in null_metrics:
                total_nulls = sum(col_metric["null_count"] for col_metric in null_metrics["column_metrics"].values())
                total_cells = dataset.count() * len(columns)
                metrics["overall_null_rate"] = total_nulls / total_cells if total_cells > 0 else 0
            
            # Calculate duplicate percentage
            dup_metrics = self._calculate_duplicate_percentage(dataset)
            metrics["duplicate_percentage"] = dup_metrics["duplicate_percentage"]
            metrics["duplicate_count"] = dup_metrics["duplicate_count"]
            
            # Calculate numeric statistics for numeric columns
            if "numeric_columns" in schema_info and schema_info["numeric_columns"]:
                metrics["numeric_stats"] = self._calculate_numeric_stats(
                    dataset, schema_info["numeric_columns"]
                )
            
            # Run quality rules
            metrics["quality_rules"] = self._run_quality_rules(dataset)
            
        except Exception as e:
            self.logger.error(f"Error calculating data quality metrics: {str(e)}")
            metrics["error"] = str(e)
            
        return metrics
    
    def _calculate_business_metrics(self, dataset: DataFrame, custom_args: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate business-specific metrics.
        
        Args:
            dataset: DataFrame to analyze
            custom_args: Additional arguments for metrics
            
        Returns:
            Dictionary with business metrics
        """
        metrics = {}
        
        # This would typically be customized for the specific application
        # Here we provide a generic example
        
        try:
            # Example: Calculate success rate if status column exists
            if "status" in dataset.columns:
                success_count = dataset.filter(col("status") == "success").count()
                total_count = dataset.count()
                metrics["success_rate"] = success_count / total_count if total_count > 0 else 0
            
            # Example: Calculate average processing time if timestamp columns exist
            timestamp_cols = [c for c in dataset.columns if "time" in c.lower()]
            if len(timestamp_cols) >= 2:
                # Assuming start_time and end_time columns
                time_cols = [c for c in timestamp_cols if "start" in c.lower() or "end" in c.lower()]
                if len(time_cols) >= 2:
                    start_col = next((c for c in time_cols if "start" in c.lower()), None)
                    end_col = next((c for c in time_cols if "end" in c.lower()), None)
                    
                    if start_col and end_col:
                        # Calculate time difference
                        metrics["avg_processing_time"] = dataset.selectExpr(
                            f"avg(unix_timestamp({end_col}) - unix_timestamp({start_col})) as time_diff"
                        ).collect()[0][0]
            
            # Example: Calculate throughput if timestamp column exists
            if timestamp_cols:
                latest_col = timestamp_cols[0]  # Just use the first timestamp column
                min_time = dataset.agg(min_(col(latest_col))).collect()[0][0]
                max_time = dataset.agg(max_(col(latest_col))).collect()[0][0]
                
                if min_time and max_time:
                    # Calculate events per second
                    time_diff_seconds = (max_time.timestamp() - min_time.timestamp())
                    if time_diff_seconds > 0:
                        metrics["throughput"] = dataset.count() / time_diff_seconds
                        
        except Exception as e:
            self.logger.error(f"Error calculating business metrics: {str(e)}")
            metrics["error"] = str(e)
            
        return metrics
    
    def _calculate_custom_metrics(self, dataset: DataFrame, custom_args: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate custom metrics using registered functions.
        
        Args:
            dataset: DataFrame to analyze
            custom_args: Additional arguments for metrics
            
        Returns:
            Dictionary with custom metrics
        """
        metrics = {}
        
        # Prepare common arguments
        common_args = {"dataset": dataset}
        if self.spark:
            common_args["spark"] = self.spark
            
        # Update with custom arguments
        args = {**common_args, **custom_args}
        
        # Call each metric function
        for name, metric_func in self.metric_functions.items():
            try:
                # Check if we have all required arguments
                missing_args = [arg for arg in metric_func.required_args if arg not in args]
                if missing_args:
                    self.logger.warning(f"Skipping metric {name} due to missing arguments: {missing_args}")
                    continue
                
                # Prepare function arguments
                func_args = {
                    arg: args[arg]
                    for arg in metric_func.required_args
                    if arg in args
                }
                
                # Add optional arguments if available
                for arg, default_value in metric_func.optional_args.items():
                    func_args[arg] = args.get(arg, default_value)
                
                # Call the function
                result = metric_func.func(**func_args)
                metrics[name] = result
                
            except Exception as e:
                self.logger.error(f"Error calculating custom metric {name}: {str(e)}")
                metrics[f"{name}_error"] = str(e)
                
        return metrics
    
    def _run_quality_rules(self, dataset: DataFrame) -> Dict[str, Any]:
        """Run quality rules on the dataset.
        
        Args:
            dataset: DataFrame to analyze
            
        Returns:
            Dictionary with quality rule results
        """
        rule_results = {}
        
        for name, rule in self.quality_rules.items():
            try:
                # Call the rule function
                result = rule.func(dataset=dataset)
                rule_results[name] = result
                
            except Exception as e:
                self.logger.error(f"Error running quality rule {name}: {str(e)}")
                rule_results[f"{name}_error"] = str(e)
                
        return rule_results
    
    # Built-in metric functions
    
    def _calculate_null_percentage(self, dataset: DataFrame, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """Calculate percentage of null values in columns.
        
        Args:
            dataset: DataFrame to analyze
            columns: List of columns to check (if None, check all columns)
            
        Returns:
            Dictionary with null percentage metrics
        """
        if not columns:
            columns = dataset.columns
            
        # Count total rows
        total_rows = dataset.count()
        if total_rows == 0:
            return {
                "overall_null_rate": 0,
                "column_metrics": {}
            }
            
        # Create expressions for null counts
        exprs = []
        for col_name in columns:
            exprs.append(count(col(col_name).isNull().cast("int")).alias(f"{col_name}_null_count"))
            
        # Calculate null counts
        null_counts = dataset.select(*exprs).collect()[0].asDict()
        
        # Format results
        column_metrics = {}
        for col_name in columns:
            null_count = null_counts.get(f"{col_name}_null_count", 0)
            column_metrics[col_name] = {
                "null_count": null_count,
                "null_percentage": (null_count / total_rows) * 100 if total_rows > 0 else 0
            }
            
        return {
            "overall_null_rate": sum(m["null_count"] for m in column_metrics.values()) / (total_rows * len(columns)) if total_rows > 0 else 0,
            "column_metrics": column_metrics
        }
    
    def _calculate_duplicate_percentage(self, dataset: DataFrame, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """Calculate percentage of duplicate rows.
        
        Args:
            dataset: DataFrame to analyze
            columns: List of columns to consider for duplicates (if None, use all columns)
            
        Returns:
            Dictionary with duplicate percentage metrics
        """
        if not columns:
            columns = dataset.columns
            
        # Count total rows
        total_rows = dataset.count()
        if total_rows == 0:
            return {
                "duplicate_percentage": 0,
                "duplicate_count": 0,
                "unique_count": 0
            }
        
        # Count distinct rows
        distinct_rows = dataset.select(*columns).distinct().count()
        duplicate_rows = total_rows - distinct_rows
        
        return {
            "duplicate_percentage": (duplicate_rows / total_rows) * 100 if total_rows > 0 else 0,
            "duplicate_count": duplicate_rows,
            "unique_count": distinct_rows
        }
    
    def _calculate_value_distribution(self, dataset: DataFrame, column: str, max_categories: int = 20) -> Dict[str, Any]:
        """Calculate value distribution for a column.
        
        Args:
            dataset: DataFrame to analyze
            column: Column to analyze
            max_categories: Maximum number of categories to return
            
        Returns:
            Dictionary with value distribution metrics
        """
        if column not in dataset.columns:
            return {
                "error": f"Column {column} not found in dataset"
            }
            
        # Count total rows
        total_rows = dataset.count()
        if total_rows == 0:
            return {
                "total": 0,
                "distribution": {}
            }
            
        # Calculate value counts
        value_counts = dataset.groupBy(column).count().orderBy(col("count").desc()).limit(max_categories)
        distribution = {
            str(row[column]): {
                "count": row["count"],
                "percentage": (row["count"] / total_rows) * 100 if total_rows > 0 else 0
            }
            for row in value_counts.collect()
        }
        
        return {
            "total": total_rows,
            "distribution": distribution
        }
    
    def _calculate_numeric_stats(self, dataset: DataFrame, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """Calculate statistics for numeric columns.
        
        Args:
            dataset: DataFrame to analyze
            columns: List of numeric columns to analyze (if None, use all numeric columns)
            
        Returns:
            Dictionary with numeric statistics
        """
        if not columns:
            # Try to identify numeric columns
            schema = dataset.schema
            columns = [
                field.name for field in schema.fields
                if str(field.dataType).startswith(("IntegerType", "LongType", "DoubleType", "FloatType", "DecimalType"))
            ]
            
        if not columns:
            return {
                "error": "No numeric columns found"
            }
            
        # Create expressions for statistics
        exprs = []
        for col_name in columns:
            exprs.extend([
                min_(col(col_name)).alias(f"{col_name}_min"),
                max_(col(col_name)).alias(f"{col_name}_max"),
                avg(col(col_name)).alias(f"{col_name}_avg"),
                expr(f"percentile({col_name}, 0.5)").alias(f"{col_name}_median")
            ])
            
        # Calculate statistics
        stats = dataset.select(*exprs).collect()[0].asDict()
        
        # Format results
        column_stats = {}
        for col_name in columns:
            column_stats[col_name] = {
                "min": stats.get(f"{col_name}_min"),
                "max": stats.get(f"{col_name}_max"),
                "avg": stats.get(f"{col_name}_avg"),
                "median": stats.get(f"{col_name}_median")
            }
            
        return column_stats

# Decorator for custom metrics
def metric(name: Optional[str] = None, description: str = "", 
           required_args: List[str] = None, optional_args: Dict[str, Any] = None):
    """Decorator to register a function as a custom metric.
    
    Args:
        name: Custom name for the metric (default: function name)
        description: Description of the metric
        required_args: List of required argument names
        optional_args: Dictionary of optional arguments with default values
        
    Returns:
        Decorated function
    """
    def decorator(func):
        func._is_metric = True
        func._metric_name = name or func.__name__
        func._description = description
        func._required_args = required_args or ["dataset"]
        func._optional_args = optional_args or {}
        return func
    return decorator

# Decorator for quality rules
def quality_rule(name: Optional[str] = None, description: str = ""):
    """Decorator to register a function as a quality rule.
    
    Args:
        name: Custom name for the rule (default: function name)
        description: Description of the rule
        
    Returns:
        Decorated function
    """
    def decorator(func):
        func._is_quality_rule = True
        func._rule_name = name or func.__name__
        func._description = description
        func._required_args = ["dataset"]
        func._optional_args = {}
        return func
    return decorator

# Example of custom metrics file structure:
"""
# my_custom_metrics.py
from custom_app_metrics_collector import metric, quality_rule
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when

@metric(name="order_success_rate", description="Calculate order success rate")
def calculate_order_success_rate(dataset: DataFrame):
    successful_orders = dataset.filter(col("status") == "COMPLETED").count()
    total_orders = dataset.count()
    return successful_orders / total_orders if total_orders > 0 else 0

@quality_rule(name="valid_email_format", description="Check if emails are valid format")
def check_valid_email_format(dataset: DataFrame):
    if "email" not in dataset.columns:
        return {"pass": True, "message": "No email column found"}
        
    invalid_emails = dataset.filter(~col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")).count()
    return {
        "pass": invalid_emails == 0,
        "invalid_count": invalid_emails,
        "message": f"Found {invalid_emails} invalid email formats"
    }
"""