# Spark Pipeline Debuggers Collectors Guide

This document provides an overview of the collectors in the Spark Pipeline Debugger framework and explains the available API endpoints.

## Collectors Overview

The Spark Pipeline Debugger framework includes several collectors, each responsible for gathering different types of metrics to help diagnose and monitor Spark applications.

### 1. BaseCollector

The `BaseCollector` is an abstract base class that all other collectors inherit from. It provides:

- A unified interface for collecting metrics
- Caching functionality to avoid repeated calls in a short time period
- Methods for retrieving specific metrics

### 2. CustomLoggingCollector

The `CustomLoggingCollector` parses Spark logs to identify issues and extract structured information:

- **Purpose**: Scan log files for patterns indicating problems in Spark jobs
- **Key metrics collected**:
  - Exception counts, types, and details
  - Data quality issues (null values, schema mismatches, type conversions)
  - GC overhead alerts
  - Shuffle failures
  - Serialization errors
  - Streaming metrics (processing time, input/output rates, batch sizes)
- **Features**:
  - Automatically discovers log directories
  - Tracks specific job and stage logs
  - Parses timestamps and extracts structured information from logs
  - Maintains pattern library for common Spark issues

### 3. CustomAppMetricsCollector

The `CustomAppMetricsCollector` enables domain-specific metrics for your application:

- **Purpose**: Collect application-specific and domain-specific metrics
- **Key metrics collected**:
  - Data quality metrics (null percentages, duplicate percentages)
  - Business metrics (success rates, latency, throughput)
  - Custom metrics defined by users
- **Features**:
  - Extensible with user-defined metric functions
  - Supports quality rules for data validation
  - Automatically extracts schema information
  - Provides built-in metrics for common data quality checks

### 4. SparkMeasureCollector

The `SparkMeasureCollector` provides detailed performance metrics by leveraging the SparkMeasure library:

- **Purpose**: Collect fine-grained performance metrics for stages and tasks
- **Key metrics collected**:
  - Execution times
  - CPU times
  - Memory usage
  - Shuffle metrics
  - I/O metrics
- **Derived metrics**:
  - Data skew detection
  - GC pressure analysis
  - Shuffle intensity calculation
  - Memory spill ratio
- **Modes**: Supports both stage-level (lightweight) and task-level (detailed) collection

### 5. SparkHttpCollector

The `SparkHttpCollector` interfaces with Spark's REST API to provide application-level metrics:

- **Purpose**: Collect metrics from Spark UI HTTP endpoints
- **Key metrics collected**:
  - Application information
  - Job statistics and details
  - Stage information
  - Executor metrics
  - Storage metrics (cached RDDs, DataFrames)
  - Environment information
- **Features**:
  - Auto-discovery of Spark UI URL
  - Detailed executor statistics
  - Job and stage failure analysis

### 6. ExternalMetricsCollector

The `ExternalMetricsCollector` integrates with external monitoring systems:

- **Purpose**: Interface with third-party monitoring tools
- **Supported systems**:
  - CloudWatch (AWS)
  - Ganglia
  - Grafana
  - Prometheus
- **Key metrics collected**:
  - Instance metrics (CPU, memory, disk, network)
  - EMR cluster metrics
  - Custom metrics from these systems
- **Features**:
  - Automatic configuration based on available systems
  - Flexible query capabilities
  - Authentication with external systems

### 7. OSMetricsCollector

The `OSMetricsCollector` provides system-level metrics:

- **Purpose**: Collect OS-level metrics from nodes running Spark
- **Key metrics collected**:
  - CPU usage and load
  - Memory usage
  - Disk I/O and utilization
  - Network traffic
  - Process counts
- **Collection methods**:
  - Prometheus/node_exporter integration
  - Direct system calls using psutil
- **Features**:
  - Calculates rates for cumulative metrics
  - Works on single-node or cluster environments
  - Supports both local and remote node metrics

## API Endpoints

The Spark Pipeline Debugger provides a comprehensive REST API for accessing metrics and controlling collection:

### Root Endpoint

- **GET /** - Returns basic information about the API and the status of enabled collectors

### Settings Endpoints

- **GET /api/v1/settings** - Get current settings for collectors and monitoring
- **POST /api/v1/settings** - Update settings for collectors and monitoring

### Metrics Collection Endpoints

- **POST /api/v1/metrics/collect** - Trigger metrics collection from all enabled collectors
- **GET /api/v1/metrics** - Get all metrics from all collectors

#### Individual Collector Endpoints

- **GET /api/v1/metrics/sparkmeasure** - Get performance metrics from SparkMeasure
- **GET /api/v1/metrics/http** - Get metrics from Spark UI HTTP endpoints
- **GET /api/v1/metrics/logging** - Get custom logging metrics
- **GET /api/v1/metrics/os** - Get OS-level metrics
- **POST /api/v1/metrics/os/collect** - Collect OS metrics with custom configuration
- **GET /api/v1/metrics/app** - Get application-specific metrics
- **POST /api/v1/metrics/app/data-quality** - Analyze data quality with custom configuration
- **GET /api/v1/metrics/external** - Get external metrics
- **POST /api/v1/metrics/external/collect** - Collect external metrics with custom configuration

### Application Endpoints

- **GET /api/v1/applications** - Get all applications
- **GET /api/v1/applications/{app_id}** - Get details for a specific application
- **GET /api/v1/applications/{app_id}/jobs** - Get all jobs for an application
- **GET /api/v1/applications/{app_id}/jobs/{job_id}** - Get details for a specific job
- **GET /api/v1/applications/{app_id}/stages** - Get all stages for an application
- **GET /api/v1/applications/{app_id}/stages/{stage_id}** - Get details for a specific stage
- **GET /api/v1/applications/{app_id}/executors** - Get all executors for an application
- **GET /api/v1/applications/{app_id}/exceptions** - Get all exceptions for an application

### Analysis Endpoints

- **GET /api/v1/applications/{app_id}/jobs/{job_id}/diagnosis** - Get diagnosis for a specific job
- **GET /api/v1/applications/{app_id}/performance** - Get performance metrics for an application
- **GET /api/v1/dashboard** - Get combined metrics for dashboard display

## Usage Examples

### Starting the API Server

```bash
python collectors_api.py --host 0.0.0.0 --port 8000 --spark-ui-url http://spark-master:4040 --log-dir /path/to/spark/logs --prometheus-url http://prometheus:9090
```

### Collecting All Metrics

```bash
curl -X POST http://localhost:8000/api/v1/metrics/collect
```

### Getting a Job Diagnosis

```bash
curl http://localhost:8000/api/v1/applications/app-20250509123456/jobs/1/diagnosis
```

### Analyzing Data Quality

```bash
curl -X POST http://localhost:8000/api/v1/metrics/app/data-quality -H "Content-Type: application/json" -d '{"table_name": "my_table", "columns": ["id", "name", "value"]}'
```

## Configuration Options

The API server and collectors can be configured through environment variables or command-line arguments:

- **SPARK_UI_URL**: URL for the Spark UI (default: http://localhost:4040)
- **SPARK_LOG_DIR**: Directory containing Spark logs (default: /tmp/spark-logs)
- **CACHE_TTL**: Time-to-live for metrics cache in seconds (default: 60)
- **COLLECTION_INTERVAL**: Interval for automatic metrics collection in seconds (default: 30)
- **PROMETHEUS_URL**: URL for Prometheus server (for OS metrics)
- **CUSTOM_METRICS_PATH**: Path to custom metrics definition file
- **AWS_REGION**: AWS region for CloudWatch metrics
- **GANGLIA_HOST**: Hostname for Ganglia metrics

## Extending the Framework

The Spark Pipeline Debugger framework is designed to be extensible. You can:

1. Create custom metrics using the `@metric` and `@quality_rule` decorators
2. Add new collectors by inheriting from `BaseCollector`
3. Integrate with additional external systems

## Troubleshooting

Common issues and solutions:

- **No metrics displayed**: Ensure Spark application is running and accessible
- **No logs collected**: Verify log directory path is correct
- **High latency**: Adjust collection interval and cache TTL
- **Missing OS metrics**: Install node_exporter or provide Prometheus URL

For more detailed diagnostics, check the API server logs.