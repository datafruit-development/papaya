#!/usr/bin/env python3
# external_metrics_collector.py - Collector for external metrics systems

import time
import logging
import json
import re
import requests
from typing import Dict, List, Any, Optional, Union, Tuple
import boto3
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

from base_collector import BaseCollector

class ExternalMetricsCollector(BaseCollector):
    """Collector for external metrics systems like Ganglia, CloudWatch, etc.
    
    This collector interfaces with third-party monitoring systems to collect
    additional metrics about the cluster and Spark application.
    """
    
    def __init__(self, system_type: str = "cloudwatch"):
        """Initialize the external metrics collector.
        
        Args:
            system_type: Type of external system to collect from
                         Supported: cloudwatch, ganglia, grafana, prometheus
        """
        super().__init__(name="external_metrics")
        self.system_type = system_type
        self.configs = {}
        self.clients = {}
        
    def get_supported_metrics(self) -> List[str]:
        """Return a list of metrics that this collector can provide."""
        common_metrics = [
            "external.system_type",
            "external.collection_time"
        ]
        
        # CloudWatch specific metrics
        cloudwatch_metrics = [
            "external.cloudwatch.cpu_utilization",
            "external.cloudwatch.memory_utilization",
            "external.cloudwatch.disk_utilization",
            "external.cloudwatch.network_in",
            "external.cloudwatch.network_out",
            "external.cloudwatch.emr_metrics",
            "external.cloudwatch.step_metrics"
        ]
        
        # Ganglia specific metrics
        ganglia_metrics = [
            "external.ganglia.load_one",
            "external.ganglia.load_five",
            "external.ganglia.load_fifteen",
            "external.ganglia.cpu_user",
            "external.ganglia.cpu_system",
            "external.ganglia.cpu_idle",
            "external.ganglia.mem_free",
            "external.ganglia.mem_used",
            "external.ganglia.network_in",
            "external.ganglia.network_out"
        ]
        
        # Grafana specific metrics
        grafana_metrics = [
            "external.grafana.dashboard_metrics",
            "external.grafana.panel_metrics"
        ]
        
        # Prometheus specific metrics (for completeness, though we have a separate collector for it)
        prometheus_metrics = [
            "external.prometheus.query_metrics"
        ]
        
        # Return appropriate metrics based on system type
        if self.system_type == "cloudwatch":
            return common_metrics + cloudwatch_metrics
        elif self.system_type == "ganglia":
            return common_metrics + ganglia_metrics
        elif self.system_type == "grafana":
            return common_metrics + grafana_metrics
        elif self.system_type == "prometheus":
            return common_metrics + prometheus_metrics
        else:
            return common_metrics
    
    def setup(self, context: Dict[str, Any]) -> None:
        """Set up the collector with context information.
        
        Args:
            context: Dictionary containing configuration for the external system
                     For CloudWatch: aws_region, cluster_id, etc.
                     For Ganglia: ganglia_host, ganglia_port, etc.
                     For Grafana: grafana_url, api_key, etc.
        """
        # Update system type if provided
        if "system_type" in context:
            self.system_type = context["system_type"]
            
        # Store configuration
        self.configs = context.copy()
        
        # Set up clients based on system type
        if self.system_type == "cloudwatch":
            self._setup_cloudwatch_client(context)
        elif self.system_type == "ganglia":
            self._setup_ganglia_client(context)
        elif self.system_type == "grafana":
            self._setup_grafana_client(context)
        elif self.system_type == "prometheus":
            self._setup_prometheus_client(context)
            
        self.logger.info(f"External metrics collector initialized for {self.system_type}")
    
    def collect(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Collect metrics from the external system.
        
        Args:
            context: Dictionary containing collection context.
                     May include specific metrics to collect or time ranges.
                     
        Returns:
            Dictionary of collected metrics
        """
        # Update configuration if needed
        if "system_type" in context:
            self.system_type = context["system_type"]
            
            # Reinitialize client if system type changed
            self.setup(context)
        elif any(key in context for key in self.configs.keys()):
            # Update configs
            self.configs.update({k: v for k, v in context.items() if k in self.configs})
            
        # Collect metrics based on system type
        metrics = {
            "timestamp": time.time(),
            "system_type": self.system_type,
            "collection_time": 0
        }
        
        start_time = time.time()
        
        try:
            if self.system_type == "cloudwatch":
                metrics.update(self._collect_cloudwatch_metrics(context))
            elif self.system_type == "ganglia":
                metrics.update(self._collect_ganglia_metrics(context))
            elif self.system_type == "grafana":
                metrics.update(self._collect_grafana_metrics(context))
            elif self.system_type == "prometheus":
                metrics.update(self._collect_prometheus_metrics(context))
            else:
                metrics["error"] = f"Unsupported system type: {self.system_type}"
        except Exception as e:
            self.logger.error(f"Error collecting {self.system_type} metrics: {str(e)}")
            metrics["error"] = str(e)
            
        # Calculate collection time
        metrics["collection_time"] = time.time() - start_time
        
        return metrics
    
    def _setup_cloudwatch_client(self, context: Dict[str, Any]) -> None:
        """Set up AWS CloudWatch client.
        
        Args:
            context: Configuration for CloudWatch
                     Required: aws_region
                     Optional: aws_access_key_id, aws_secret_access_key, etc.
        """
        if "aws_region" not in context:
            raise ValueError("aws_region is required for CloudWatch")
            
        # Extract AWS credentials
        aws_region = context["aws_region"]
        aws_access_key_id = context.get("aws_access_key_id")
        aws_secret_access_key = context.get("aws_secret_access_key")
        
        # Create CloudWatch client
        session_args = {"region_name": aws_region}
        if aws_access_key_id and aws_secret_access_key:
            session_args.update({
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": aws_secret_access_key
            })
            
        session = boto3.Session(**session_args)
        self.clients["cloudwatch"] = session.client('cloudwatch')
        
        # Create EMR client if collecting EMR metrics
        if context.get("collect_emr_metrics", False):
            self.clients["emr"] = session.client('emr')
    
    def _setup_ganglia_client(self, context: Dict[str, Any]) -> None:
        """Set up Ganglia client (no actual client, just configuration).
        
        Args:
            context: Configuration for Ganglia
                     Required: ganglia_host, ganglia_port
        """
        if "ganglia_host" not in context:
            raise ValueError("ganglia_host is required for Ganglia")
            
        if "ganglia_port" not in context:
            context["ganglia_port"] = 8652  # Default Ganglia XML port
    
    def _setup_grafana_client(self, context: Dict[str, Any]) -> None:
        """Set up Grafana client (no actual client, just configuration).
        
        Args:
            context: Configuration for Grafana
                     Required: grafana_url, api_key
        """
        if "grafana_url" not in context:
            raise ValueError("grafana_url is required for Grafana")
            
        if "api_key" not in context:
            raise ValueError("api_key is required for Grafana")
    
    def _setup_prometheus_client(self, context: Dict[str, Any]) -> None:
        """Set up Prometheus client (no actual client, just configuration).
        
        Args:
            context: Configuration for Prometheus
                     Required: prometheus_url
        """
        if "prometheus_url" not in context:
            raise ValueError("prometheus_url is required for Prometheus")
    
    def _collect_cloudwatch_metrics(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Collect metrics from AWS CloudWatch.
        
        Args:
            context: Collection context
                     Optional: start_time, end_time, metrics, etc.
                     
        Returns:
            Dictionary of CloudWatch metrics
        """
        metrics = {
            "cloudwatch": {}
        }
        
        if "cloudwatch" not in self.clients:
            metrics["cloudwatch"]["error"] = "CloudWatch client not initialized"
            return metrics
            
        # Determine the time range for metrics
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)  # Default: last hour
        
        if "start_time" in context:
            if isinstance(context["start_time"], datetime):
                start_time = context["start_time"]
            else:
                # Try to parse as ISO format
                try:
                    start_time = datetime.fromisoformat(context["start_time"].replace('Z', '+00:00'))
                except (ValueError, AttributeError):
                    # If parsing fails, use default
                    pass
                    
        if "end_time" in context:
            if isinstance(context["end_time"], datetime):
                end_time = context["end_time"]
            else:
                # Try to parse as ISO format
                try:
                    end_time = datetime.fromisoformat(context["end_time"].replace('Z', '+00:00'))
                except (ValueError, AttributeError):
                    # If parsing fails, use default
                    pass
        
        # Get instance metrics if instance IDs are provided
        if "instance_ids" in context:
            metrics["cloudwatch"]["instances"] = self._get_instance_metrics(
                context["instance_ids"], start_time, end_time
            )
        
        # Get EMR metrics if cluster ID is provided
        if "emr_cluster_id" in context and "emr" in self.clients:
            metrics["cloudwatch"]["emr"] = self._get_emr_metrics(
                context["emr_cluster_id"], start_time, end_time
            )
        
        # Get custom metrics
        if "custom_metrics" in context:
            metrics["cloudwatch"]["custom"] = self._get_custom_cloudwatch_metrics(
                context["custom_metrics"], start_time, end_time
            )
            
        return metrics
    
    def _get_instance_metrics(self, instance_ids: List[str], start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Get CloudWatch metrics for EC2 instances.
        
        Args:
            instance_ids: List of EC2 instance IDs
            start_time: Start time for metrics
            end_time: End time for metrics
            
        Returns:
            Dictionary of instance metrics
        """
        metrics_result = {}
        
        # Define metrics to collect
        metrics_to_collect = [
            {"name": "CPUUtilization", "unit": "Percent", "stat": "Average"},
            {"name": "MemoryUtilization", "unit": "Percent", "stat": "Average"},
            {"name": "DiskReadBytes", "unit": "Bytes", "stat": "Sum"},
            {"name": "DiskWriteBytes", "unit": "Bytes", "stat": "Sum"},
            {"name": "NetworkIn", "unit": "Bytes", "stat": "Sum"},
            {"name": "NetworkOut", "unit": "Bytes", "stat": "Sum"}
        ]
        
        # Collect metrics for each instance
        for instance_id in instance_ids:
            instance_metrics = {}
            
            for metric in metrics_to_collect:
                try:
                    response = self.clients["cloudwatch"].get_metric_statistics(
                        Namespace="AWS/EC2",
                        MetricName=metric["name"],
                        Dimensions=[
                            {
                                "Name": "InstanceId",
                                "Value": instance_id
                            }
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=300,  # 5-minute periods
                        Statistics=[metric["stat"]],
                        Unit=metric["unit"]
                    )
                    
                    # Extract datapoints
                    datapoints = response.get("Datapoints", [])
                    if datapoints:
                        # Sort by timestamp
                        datapoints = sorted(datapoints, key=lambda x: x["Timestamp"])
                        
                        # Extract metric values
                        instance_metrics[metric["name"]] = {
                            "values": [point[metric["stat"]] for point in datapoints],
                            "timestamps": [point["Timestamp"].isoformat() for point in datapoints],
                            "unit": metric["unit"],
                            "stat": metric["stat"],
                            "latest": datapoints[-1][metric["stat"]] if datapoints else None,
                            "average": sum(point[metric["stat"]] for point in datapoints) / len(datapoints) if datapoints else None
                        }
                    else:
                        instance_metrics[metric["name"]] = {
                            "values": [],
                            "timestamps": [],
                            "unit": metric["unit"],
                            "stat": metric["stat"],
                            "latest": None,
                            "average": None
                        }
                        
                except Exception as e:
                    self.logger.error(f"Error getting {metric['name']} for instance {instance_id}: {str(e)}")
                    instance_metrics[metric["name"]] = {
                        "error": str(e)
                    }
            
            metrics_result[instance_id] = instance_metrics
            
        return metrics_result
    
    def _get_emr_metrics(self, cluster_id: str, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Get CloudWatch metrics for an EMR cluster.
        
        Args:
            cluster_id: EMR cluster ID
            start_time: Start time for metrics
            end_time: End time for metrics
            
        Returns:
            Dictionary of EMR metrics
        """
        metrics_result = {
            "cluster_info": {},
            "metrics": {}
        }
        
        try:
            # Get cluster info
            cluster_info = self.clients["emr"].describe_cluster(
                ClusterId=cluster_id
            )
            
            if "Cluster" in cluster_info:
                cluster = cluster_info["Cluster"]
                metrics_result["cluster_info"] = {
                    "name": cluster.get("Name", ""),
                    "status": cluster.get("Status", {}).get("State", ""),
                    "normalized_status": cluster.get("Status", {}).get("StateChangeReason", {}).get("Code", ""),
                    "creation_time": cluster.get("Status", {}).get("Timeline", {}).get("CreationDateTime", "").isoformat() if cluster.get("Status", {}).get("Timeline", {}).get("CreationDateTime") else None,
                    "end_time": cluster.get("Status", {}).get("Timeline", {}).get("EndDateTime", "").isoformat() if cluster.get("Status", {}).get("Timeline", {}).get("EndDateTime") else None,
                    "instance_count": cluster.get("InstanceCollectionType", ""),
                    "instance_type": cluster.get("InstanceGroups", [{}])[0].get("InstanceType", "") if cluster.get("InstanceGroups", []) else "",
                    "master_public_dns": cluster.get("MasterPublicDnsName", "")
                }
                
                # Get instance groups info
                if "InstanceGroups" in cluster:
                    metrics_result["cluster_info"]["instance_groups"] = []
                    
                    for group in cluster["InstanceGroups"]:
                        metrics_result["cluster_info"]["instance_groups"].append({
                            "id": group.get("Id", ""),
                            "name": group.get("Name", ""),
                            "type": group.get("InstanceGroupType", ""),
                            "instance_type": group.get("InstanceType", ""),
                            "requested_instances": group.get("RequestedInstanceCount", 0),
                            "running_instances": group.get("RunningInstanceCount", 0),
                            "status": group.get("Status", {}).get("State", "")
                        })
                        
            # Define metrics to collect
            metrics_to_collect = [
                {"name": "HDFSUtilization", "unit": "Percent", "stat": "Average"},
                {"name": "YARNMemoryAvailablePercentage", "unit": "Percent", "stat": "Average"},
                {"name": "AppsCompleted", "unit": "Count", "stat": "Sum"},
                {"name": "AppsFailed", "unit": "Count", "stat": "Sum"},
                {"name": "AppsRunning", "unit": "Count", "stat": "Average"},
                {"name": "AppsSubmitted", "unit": "Count", "stat": "Sum"},
                {"name": "ContainerAllocated", "unit": "Count", "stat": "Average"},
                {"name": "ContainerPending", "unit": "Count", "stat": "Average"},
                {"name": "IsIdle", "unit": "Count", "stat": "Average"}
            ]
            
            # Collect metrics
            for metric in metrics_to_collect:
                try:
                    response = self.clients["cloudwatch"].get_metric_statistics(
                        Namespace="AWS/ElasticMapReduce",
                        MetricName=metric["name"],
                        Dimensions=[
                            {
                                "Name": "JobFlowId",
                                "Value": cluster_id
                            }
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=300,  # 5-minute periods
                        Statistics=[metric["stat"]],
                        Unit=metric["unit"]
                    )
                    
                    # Extract datapoints
                    datapoints = response.get("Datapoints", [])
                    if datapoints:
                        # Sort by timestamp
                        datapoints = sorted(datapoints, key=lambda x: x["Timestamp"])
                        
                        # Extract metric values
                        metrics_result["metrics"][metric["name"]] = {
                            "values": [point[metric["stat"]] for point in datapoints],
                            "timestamps": [point["Timestamp"].isoformat() for point in datapoints],
                            "unit": metric["unit"],
                            "stat": metric["stat"],
                            "latest": datapoints[-1][metric["stat"]] if datapoints else None,
                            "average": sum(point[metric["stat"]] for point in datapoints) / len(datapoints) if datapoints else None
                        }
                    else:
                        metrics_result["metrics"][metric["name"]] = {
                            "values": [],
                            "timestamps": [],
                            "unit": metric["unit"],
                            "stat": metric["stat"],
                            "latest": None,
                            "average": None
                        }
                        
                except Exception as e:
                    self.logger.error(f"Error getting {metric['name']} for cluster {cluster_id}: {str(e)}")
                    metrics_result["metrics"][metric["name"]] = {
                        "error": str(e)
                    }
                    
            # Get cluster steps if available
            try:
                steps_response = self.clients["emr"].list_steps(
                    ClusterId=cluster_id
                )
                
                if "Steps" in steps_response:
                    metrics_result["steps"] = []
                    
                    for step in steps_response["Steps"]:
                        metrics_result["steps"].append({
                            "id": step.get("Id", ""),
                            "name": step.get("Name", ""),
                            "status": step.get("Status", {}).get("State", ""),
                            "created_time": step.get("Status", {}).get("Timeline", {}).get("CreationDateTime", "").isoformat() if step.get("Status", {}).get("Timeline", {}).get("CreationDateTime") else None,
                            "start_time": step.get("Status", {}).get("Timeline", {}).get("StartDateTime", "").isoformat() if step.get("Status", {}).get("Timeline", {}).get("StartDateTime") else None,
                            "end_time": step.get("Status", {}).get("Timeline", {}).get("EndDateTime", "").isoformat() if step.get("Status", {}).get("Timeline", {}).get("EndDateTime") else None,
                            "action_on_failure": step.get("ActionOnFailure", "")
                        })
                        
            except Exception as e:
                self.logger.error(f"Error getting steps for cluster {cluster_id}: {str(e)}")
                metrics_result["steps_error"] = str(e)
                
        except Exception as e:
            self.logger.error(f"Error getting EMR metrics for cluster {cluster_id}: {str(e)}")
            metrics_result["error"] = str(e)
            
        return metrics_result
    
    def _get_custom_cloudwatch_metrics(self, metric_configs: List[Dict[str, Any]], start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Get custom CloudWatch metrics.
        
        Args:
            metric_configs: List of custom metric configurations
                           Each config should have: namespace, name, dimensions, unit, stat
            start_time: Start time for metrics
            end_time: End time for metrics
            
        Returns:
            Dictionary of custom metrics
        """
        metrics_result = {}
        
        for config in metric_configs:
            if not all(k in config for k in ["namespace", "name"]):
                continue
                
            metric_name = config["name"]
            namespace = config["namespace"]
            dimensions = config.get("dimensions", [])
            unit = config.get("unit", "None")
            stat = config.get("stat", "Average")
            
            try:
                response = self.clients["cloudwatch"].get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric_name,
                    Dimensions=dimensions,
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=300,  # 5-minute periods
                    Statistics=[stat],
                    Unit=unit
                )
                
                # Extract datapoints
                datapoints = response.get("Datapoints", [])
                if datapoints:
                    # Sort by timestamp
                    datapoints = sorted(datapoints, key=lambda x: x["Timestamp"])
                    
                    # Extract metric values
                    metrics_result[metric_name] = {
                        "values": [point[stat] for point in datapoints],
                        "timestamps": [point["Timestamp"].isoformat() for point in datapoints],
                        "unit": unit,
                        "stat": stat,
                        "namespace": namespace,
                        "dimensions": dimensions,
                        "latest": datapoints[-1][stat] if datapoints else None,
                        "average": sum(point[stat] for point in datapoints) / len(datapoints) if datapoints else None
                    }
                else:
                    metrics_result[metric_name] = {
                        "values": [],
                        "timestamps": [],
                        "unit": unit,
                        "stat": stat,
                        "namespace": namespace,
                        "dimensions": dimensions,
                        "latest": None,
                        "average": None
                    }
                    
            except Exception as e:
                self.logger.error(f"Error getting custom metric {metric_name}: {str(e)}")
                metrics_result[metric_name] = {
                    "error": str(e)
                }
                
        return metrics_result
    
    def _collect_ganglia_metrics(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Collect metrics from Ganglia.
        
        Args:
            context: Collection context
                     Optional: metrics, cluster_name, etc.
                     
        Returns:
            Dictionary of Ganglia metrics
        """
        metrics = {
            "ganglia": {}
        }
        
        if "ganglia_host" not in self.configs:
            metrics["ganglia"]["error"] = "Ganglia host not configured"
            return metrics
            
        ganglia_host = self.configs["ganglia_host"]
        ganglia_port = self.configs.get("ganglia_port", 8652)
        
        # Construct Ganglia XML URL
        ganglia_url = f"http://{ganglia_host}:{ganglia_port}/ganglia/xml"
        
        try:
            # Fetch Ganglia XML
            response = requests.get(ganglia_url, timeout=10)
            
            if response.status_code != 200:
                metrics["ganglia"]["error"] = f"Failed to fetch Ganglia XML: {response.status_code} {response.text}"
                return metrics
                
            # Parse XML
            root = ET.fromstring(response.text)
            
            # Extract cluster info
            clusters = {}
            for cluster in root.findall("./CLUSTER"):
                cluster_name = cluster.get("NAME", "unknown")
                clusters[cluster_name] = {
                    "name": cluster_name,
                    "localtime": cluster.get("LOCALTIME", ""),
                    "owner": cluster.get("OWNER", ""),
                    "hosts": {}
                }
                
                # Extract host info
                for host in cluster.findall("./HOST"):
                    host_name = host.get("NAME", "unknown")
                    host_ip = host.get("IP", "")
                    
                    clusters[cluster_name]["hosts"][host_name] = {
                        "name": host_name,
                        "ip": host_ip,
                        "reported": host.get("REPORTED", ""),
                        "metrics": {}
                    }
                    
                    # Extract metrics for this host
                    for metric in host.findall("./METRIC"):
                        metric_name = metric.get("NAME", "unknown")
                        
                        clusters[cluster_name]["hosts"][host_name]["metrics"][metric_name] = {
                            "name": metric_name,
                            "value": metric.get("VAL", ""),
                            "type": metric.get("TYPE", ""),
                            "units": metric.get("UNITS", ""),
                            "slope": metric.get("SLOPE", ""),
                            "tn": int(metric.get("TN", 0)),
                            "tmax": int(metric.get("TMAX", 0)),
                            "dmax": int(metric.get("DMAX", 0))
                        }
            
            # Filter by specific cluster if provided
            cluster_filter = context.get("cluster_name")
            if cluster_filter and cluster_filter in clusters:
                metrics["ganglia"]["clusters"] = {cluster_filter: clusters[cluster_filter]}
            else:
                metrics["ganglia"]["clusters"] = clusters
                
            # Extract Spark-related metrics if available
            spark_metrics = self._extract_spark_metrics_from_ganglia(clusters)
            if spark_metrics:
                metrics["ganglia"]["spark"] = spark_metrics
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching Ganglia metrics: {str(e)}")
            metrics["ganglia"]["error"] = f"Failed to connect to Ganglia: {str(e)}"
        except ET.ParseError as e:
            self.logger.error(f"Error parsing Ganglia XML: {str(e)}")
            metrics["ganglia"]["error"] = f"Failed to parse Ganglia XML: {str(e)}"
        except Exception as e:
            self.logger.error(f"Error processing Ganglia data: {str(e)}")
            metrics["ganglia"]["error"] = str(e)
            
        return metrics
    
    def _extract_spark_metrics_from_ganglia(self, clusters: Dict[str, Any]) -> Dict[str, Any]:
        """Extract Spark-related metrics from Ganglia data.
        
        Args:
            clusters: Dictionary of Ganglia clusters and hosts
            
        Returns:
            Dictionary of Spark metrics
        """
        spark_metrics = {}
        
        # Check for Spark JVM metrics
        spark_metric_patterns = [
            r"^spark\..*",
            r".*\.driver\..*",
            r".*\.executor\..*",
            r"^jvm\..*"
        ]
        
        for cluster_name, cluster in clusters.items():
            for host_name, host in cluster.get("hosts", {}).items():
                for metric_name, metric in host.get("metrics", {}).items():
                    # Check if metric is related to Spark
                    is_spark_metric = any(re.match(pattern, metric_name) for pattern in spark_metric_patterns)
                    
                    if is_spark_metric:
                        # Organize by host and metric name
                        if host_name not in spark_metrics:
                            spark_metrics[host_name] = {}
                            
                        spark_metrics[host_name][metric_name] = metric
                        
        return spark_metrics
    
    def _collect_grafana_metrics(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Collect metrics from Grafana.
        
        Args:
            context: Collection context
                     Optional: dashboard_uid, panel_id, etc.
                     
        Returns:
            Dictionary of Grafana metrics
        """
        metrics = {
            "grafana": {}
        }
        
        if "grafana_url" not in self.configs or "api_key" not in self.configs:
            metrics["grafana"]["error"] = "Grafana URL or API key not configured"
            return metrics
            
        grafana_url = self.configs["grafana_url"]
        api_key = self.configs["api_key"]
        
        # Set up headers for Grafana API
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        try:
            # Get dashboard info if dashboard_uid is provided
            if "dashboard_uid" in context:
                dashboard_uid = context["dashboard_uid"]
                dashboard_url = f"{grafana_url}/api/dashboards/uid/{dashboard_uid}"
                
                dashboard_response = requests.get(dashboard_url, headers=headers)
                
                if dashboard_response.status_code == 200:
                    dashboard_data = dashboard_response.json()
                    
                    if "dashboard" in dashboard_data:
                        metrics["grafana"]["dashboard"] = {
                            "uid": dashboard_data["dashboard"].get("uid", ""),
                            "title": dashboard_data["dashboard"].get("title", ""),
                            "description": dashboard_data["dashboard"].get("description", ""),
                            "panels": []
                        }
                        
                        # Extract panel info
                        for panel in dashboard_data["dashboard"].get("panels", []):
                            panel_info = {
                                "id": panel.get("id", ""),
                                "title": panel.get("title", ""),
                                "type": panel.get("type", ""),
                                "description": panel.get("description", "")
                            }
                            
                            # Extract targets (queries)
                            if "targets" in panel:
                                panel_info["targets"] = []
                                
                                for target in panel["targets"]:
                                    panel_info["targets"].append({
                                        "refId": target.get("refId", ""),
                                        "expr": target.get("expr", ""),
                                        "datasource": target.get("datasource", {}).get("uid", "") if isinstance(target.get("datasource"), dict) else target.get("datasource", "")
                                    })
                                    
                            metrics["grafana"]["dashboard"]["panels"].append(panel_info)
                            
                    else:
                        metrics["grafana"]["dashboard"] = {"error": "Dashboard data not found in response"}
                else:
                    metrics["grafana"]["dashboard"] = {
                        "error": f"Failed to fetch dashboard: {dashboard_response.status_code} {dashboard_response.text}"
                    }
                    
            # Get panel data if panel_id is provided
            if "dashboard_uid" in context and "panel_id" in context:
                dashboard_uid = context["dashboard_uid"]
                panel_id = context["panel_id"]
                
                # Get time range
                from_time = context.get("from_time", "now-1h")
                to_time = context.get("to_time", "now")
                
                panel_url = f"{grafana_url}/api/dashboards/uid/{dashboard_uid}/panels/{panel_id}/query"
                panel_data = {
                    "from": from_time,
                    "to": to_time,
                    "queries": [
                        {
                            "refId": "A",
                            "datasourceId": 1,
                            "datasourceName": "Prometheus",
                            "expr": context.get("query", ""),
                            "intervalMs": 1000
                        }
                    ]
                }
                
                panel_response = requests.post(panel_url, headers=headers, json=panel_data)
                
                if panel_response.status_code == 200:
                    panel_result = panel_response.json()
                    metrics["grafana"]["panel_data"] = panel_result
                else:
                    metrics["grafana"]["panel_data"] = {
                        "error": f"Failed to fetch panel data: {panel_response.status_code} {panel_response.text}"
                    }
                    
            # Get datasources
            datasources_url = f"{grafana_url}/api/datasources"
            datasources_response = requests.get(datasources_url, headers=headers)
            
            if datasources_response.status_code == 200:
                metrics["grafana"]["datasources"] = datasources_response.json()
            else:
                metrics["grafana"]["datasources"] = {
                    "error": f"Failed to fetch datasources: {datasources_response.status_code} {datasources_response.text}"
                }
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching Grafana metrics: {str(e)}")
            metrics["grafana"]["error"] = f"Failed to connect to Grafana: {str(e)}"
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing Grafana response: {str(e)}")
            metrics["grafana"]["error"] = f"Failed to parse Grafana response: {str(e)}"
        except Exception as e:
            self.logger.error(f"Error processing Grafana data: {str(e)}")
            metrics["grafana"]["error"] = str(e)
            
        return metrics
    
    def _collect_prometheus_metrics(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Collect metrics from Prometheus.
        
        Args:
            context: Collection context
                     Required: queries (list of Prometheus queries)
                     
        Returns:
            Dictionary of Prometheus metrics
        """
        metrics = {
            "prometheus": {}
        }
        
        if "prometheus_url" not in self.configs:
            metrics["prometheus"]["error"] = "Prometheus URL not configured"
            return metrics
            
        prometheus_url = self.configs["prometheus_url"]
        
        # Get queries from context
        queries = context.get("queries", [])
        if not queries:
            metrics["prometheus"]["error"] = "No queries provided"
            return metrics
            
        # Get time range
        end_time = int(time.time())
        start_time = end_time - 3600  # Default: last hour
        step = context.get("step", "15s")
        
        if "start_time" in context:
            if isinstance(context["start_time"], (int, float)):
                start_time = int(context["start_time"])
            elif isinstance(context["start_time"], str):
                try:
                    # Try to parse as ISO format
                    start_time = int(datetime.fromisoformat(context["start_time"].replace('Z', '+00:00')).timestamp())
                except ValueError:
                    # If parsing fails, use default
                    pass
                    
        if "end_time" in context:
            if isinstance(context["end_time"], (int, float)):
                end_time = int(context["end_time"])
            elif isinstance(context["end_time"], str):
                try:
                    # Try to parse as ISO format
                    end_time = int(datetime.fromisoformat(context["end_time"].replace('Z', '+00:00')).timestamp())
                except ValueError:
                    # If parsing fails, use default
                    pass
                    
        # Execute queries
        try:
            metrics["prometheus"]["results"] = {}
            
            for query in queries:
                query_name = query.get("name", "")
                query_expr = query.get("expr", "")
                
                if not query_expr:
                    continue
                    
                # Execute query
                query_url = f"{prometheus_url}/api/v1/query_range"
                params = {
                    "query": query_expr,
                    "start": start_time,
                    "end": end_time,
                    "step": step
                }
                
                response = requests.get(query_url, params=params)
                
                if response.status_code == 200:
                    query_result = response.json()
                    
                    if query_result["status"] == "success":
                        metrics["prometheus"]["results"][query_name or query_expr] = query_result["data"]
                    else:
                        metrics["prometheus"]["results"][query_name or query_expr] = {
                            "error": query_result.get("error", "Unknown error")
                        }
                else:
                    metrics["prometheus"]["results"][query_name or query_expr] = {
                        "error": f"Failed to execute query: {response.status_code} {response.text}"
                    }
                    
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching Prometheus metrics: {str(e)}")
            metrics["prometheus"]["error"] = f"Failed to connect to Prometheus: {str(e)}"
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing Prometheus response: {str(e)}")
            metrics["prometheus"]["error"] = f"Failed to parse Prometheus response: {str(e)}"
        except Exception as e:
            self.logger.error(f"Error processing Prometheus data: {str(e)}")
            metrics["prometheus"]["error"] = str(e)
            
        return metrics