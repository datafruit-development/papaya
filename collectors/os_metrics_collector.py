#!/usr/bin/env python3
# os_metrics_collector.py - OS-level metrics collector using Prometheus/node_exporter

import time
import logging
import json
import requests
from typing import Dict, List, Any, Optional, Union
import subprocess
import socket
import platform
import psutil

from base_collector import BaseCollector

class OSMetricsCollector(BaseCollector):
    """Collects OS-level metrics for Spark nodes using Prometheus or direct system calls.
    
    This collector provides information about CPU, memory, disk I/O, and network
    utilization on the nodes where Spark executors are running.
    """
    
    def __init__(self, prometheus_url: Optional[str] = None):
        """Initialize the OS metrics collector.
        
        Args:
            prometheus_url: URL of Prometheus server. If None, will use direct system calls.
        """
        super().__init__(name="os_metrics")
        self.prometheus_url = prometheus_url
        self.use_prometheus = prometheus_url is not None
        self.node_exporter_metrics = [
            # CPU metrics
            "node_cpu_seconds_total",
            "node_load1",
            "node_load5",
            "node_load15",
            # Memory metrics
            "node_memory_MemTotal_bytes",
            "node_memory_MemFree_bytes",
            "node_memory_Cached_bytes",
            "node_memory_Buffers_bytes",
            "node_memory_SwapTotal_bytes",
            "node_memory_SwapFree_bytes",
            # Disk metrics
            "node_disk_io_time_seconds_total",
            "node_disk_read_bytes_total",
            "node_disk_written_bytes_total",
            "node_filesystem_avail_bytes",
            "node_filesystem_size_bytes",
            # Network metrics
            "node_network_receive_bytes_total",
            "node_network_transmit_bytes_total",
            "node_network_receive_packets_total",
            "node_network_transmit_packets_total",
        ]
        # Store previous measurements for rate calculations
        self.previous_metrics = {}
        self.previous_timestamp = 0
        # Get hostname for local metrics
        self.hostname = socket.gethostname()
        
    def get_supported_metrics(self) -> List[str]:
        """Return a list of metrics that this collector can provide."""
        return [
            # CPU metrics
            "os.cpu.usage", "os.cpu.load", "os.cpu.cores",
            # Memory metrics
            "os.memory.total", "os.memory.free", "os.memory.used",
            "os.memory.cached", "os.memory.buffers", 
            "os.memory.swap.total", "os.memory.swap.free", "os.memory.swap.used",
            # Disk metrics
            "os.disk.io.time", "os.disk.read.bytes", "os.disk.write.bytes",
            "os.disk.usage", "os.disk.free",
            # Network metrics
            "os.network.receive.bytes", "os.network.transmit.bytes",
            "os.network.receive.packets", "os.network.transmit.packets",
            "os.network.receive.errors", "os.network.transmit.errors",
            # Process metrics
            "os.process.count", "os.process.spark.count",
            # System metrics
            "os.uptime", "os.users"
        ]
    
    def setup(self, context: Dict[str, Any]) -> None:
        """Set up the collector with context information.
        
        Args:
            context: Dictionary containing collection context.
                     May include prometheus_url to override the default.
        """
        if "prometheus_url" in context:
            self.prometheus_url = context["prometheus_url"]
            self.use_prometheus = self.prometheus_url is not None
            
        self.logger.info(f"OS Metrics collector initialized with Prometheus: {self.use_prometheus}")
    
    def collect(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Collect OS-level metrics.
        
        Args:
            context: Dictionary containing collection context.
                     May include node_hostnames if collecting from remote nodes.
                     
        Returns:
            Dictionary of collected metrics.
        """
        metrics = {
            "timestamp": time.time(),
            "hostname": self.hostname,
            "nodes": {}
        }
        
        # Update context if needed
        if "prometheus_url" in context:
            self.prometheus_url = context["prometheus_url"]
            self.use_prometheus = self.prometheus_url is not None
            
        # Get node hostnames to collect metrics for
        nodes = context.get("node_hostnames", [self.hostname])
        
        if self.use_prometheus:
            # Collect metrics from Prometheus
            metrics["nodes"] = self._collect_from_prometheus(nodes)
        else:
            # Collect metrics from direct system calls
            for node in nodes:
                if node == self.hostname:
                    # Local node
                    metrics["nodes"][node] = self._collect_local_metrics()
                else:
                    # Remote node - would require SSH or agent
                    self.logger.warning(f"Direct metrics collection from remote node {node} not supported. Use Prometheus.")
        
        # Calculate rates for cumulative metrics
        current_timestamp = time.time()
        if self.previous_timestamp > 0:
            time_diff = current_timestamp - self.previous_timestamp
            metrics = self._calculate_rates(metrics, time_diff)
            
        # Store current metrics for next rate calculation
        self.previous_metrics = metrics.copy()
        self.previous_timestamp = current_timestamp
        
        return metrics
    
    def _collect_from_prometheus(self, nodes: List[str]) -> Dict[str, Any]:
        """Collect metrics from Prometheus.
        
        Args:
            nodes: List of node hostnames to collect metrics for.
            
        Returns:
            Dictionary of node metrics.
        """
        node_metrics = {}
        
        try:
            for node in nodes:
                node_metrics[node] = {
                    "cpu": {},
                    "memory": {},
                    "disk": {},
                    "network": {},
                    "system": {}
                }
                
                # Collect metrics for this node
                for metric in self.node_exporter_metrics:
                    query = f'{metric}{{instance=~".*{node}.*"}}'
                    response = requests.get(f"{self.prometheus_url}/api/v1/query", params={"query": query})
                    
                    if response.status_code == 200:
                        result = response.json()
                        if result["status"] == "success" and result["data"]["result"]:
                            # Process results based on metric type
                            self._process_prometheus_metric(node_metrics[node], metric, result["data"]["result"])
                    else:
                        self.logger.warning(f"Failed to query Prometheus: {response.status_code} {response.text}")
                
                # Get process information
                query = 'process_cpu_seconds_total{job="spark"}'
                response = requests.get(f"{self.prometheus_url}/api/v1/query", params={"query": query})
                if response.status_code == 200:
                    result = response.json()
                    if result["status"] == "success":
                        node_metrics[node]["system"]["spark_processes"] = len(result["data"]["result"])
                
        except Exception as e:
            self.logger.error(f"Error collecting from Prometheus: {str(e)}")
            
        return node_metrics
    
    def _process_prometheus_metric(self, node_metrics: Dict[str, Any], metric: str, results: List[Dict[str, Any]]) -> None:
        """Process a Prometheus metric and add it to the node metrics.
        
        Args:
            node_metrics: Dictionary to add metrics to.
            metric: Metric name.
            results: List of metric results from Prometheus.
        """
        # CPU metrics
        if metric == "node_cpu_seconds_total":
            # Group by mode (user, system, idle, etc.)
            cpu_times = {}
            for result in results:
                mode = result["metric"].get("mode", "unknown")
                value = float(result["value"][1])
                if mode in cpu_times:
                    cpu_times[mode] += value
                else:
                    cpu_times[mode] = value
            
            node_metrics["cpu"]["cpu_times"] = cpu_times
            
            # Calculate CPU usage if we have idle time
            if "idle" in cpu_times:
                total_time = sum(cpu_times.values())
                idle_time = cpu_times["idle"]
                node_metrics["cpu"]["usage"] = 100.0 * (1.0 - (idle_time / total_time)) if total_time > 0 else 0
                
        elif metric == "node_load1":
            node_metrics["cpu"]["load1"] = float(results[0]["value"][1]) if results else 0
        elif metric == "node_load5":
            node_metrics["cpu"]["load5"] = float(results[0]["value"][1]) if results else 0
        elif metric == "node_load15":
            node_metrics["cpu"]["load15"] = float(results[0]["value"][1]) if results else 0
            
        # Memory metrics
        elif metric == "node_memory_MemTotal_bytes":
            node_metrics["memory"]["total"] = float(results[0]["value"][1]) if results else 0
        elif metric == "node_memory_MemFree_bytes":
            node_metrics["memory"]["free"] = float(results[0]["value"][1]) if results else 0
        elif metric == "node_memory_Cached_bytes":
            node_metrics["memory"]["cached"] = float(results[0]["value"][1]) if results else 0
        elif metric == "node_memory_Buffers_bytes":
            node_metrics["memory"]["buffers"] = float(results[0]["value"][1]) if results else 0
        elif metric == "node_memory_SwapTotal_bytes":
            node_metrics["memory"]["swap_total"] = float(results[0]["value"][1]) if results else 0
        elif metric == "node_memory_SwapFree_bytes":
            node_metrics["memory"]["swap_free"] = float(results[0]["value"][1]) if results else 0
            
        # Disk metrics
        elif metric == "node_disk_io_time_seconds_total":
            total_io_time = sum(float(r["value"][1]) for r in results)
            node_metrics["disk"]["io_time_total"] = total_io_time
        elif metric == "node_disk_read_bytes_total":
            total_read_bytes = sum(float(r["value"][1]) for r in results)
            node_metrics["disk"]["read_bytes_total"] = total_read_bytes
        elif metric == "node_disk_written_bytes_total":
            total_written_bytes = sum(float(r["value"][1]) for r in results)
            node_metrics["disk"]["written_bytes_total"] = total_written_bytes
        elif metric == "node_filesystem_avail_bytes":
            # Sum available space across all filesystems
            total_avail = sum(float(r["value"][1]) for r in results if r["metric"].get("fstype") not in ["tmpfs", "devtmpfs"])
            node_metrics["disk"]["available_bytes"] = total_avail
        elif metric == "node_filesystem_size_bytes":
            # Sum total size across all filesystems
            total_size = sum(float(r["value"][1]) for r in results if r["metric"].get("fstype") not in ["tmpfs", "devtmpfs"])
            node_metrics["disk"]["total_bytes"] = total_size
            
            # Calculate usage percentage if we have both metrics
            if "available_bytes" in node_metrics["disk"] and total_size > 0:
                used_bytes = total_size - node_metrics["disk"]["available_bytes"]
                node_metrics["disk"]["usage_percent"] = 100.0 * used_bytes / total_size
            
        # Network metrics
        elif metric == "node_network_receive_bytes_total":
            # Sum across all interfaces except lo
            total_rx_bytes = sum(float(r["value"][1]) for r in results if r["metric"].get("device") != "lo")
            node_metrics["network"]["receive_bytes_total"] = total_rx_bytes
        elif metric == "node_network_transmit_bytes_total":
            # Sum across all interfaces except lo
            total_tx_bytes = sum(float(r["value"][1]) for r in results if r["metric"].get("device") != "lo")
            node_metrics["network"]["transmit_bytes_total"] = total_tx_bytes
        elif metric == "node_network_receive_packets_total":
            total_rx_packets = sum(float(r["value"][1]) for r in results if r["metric"].get("device") != "lo")
            node_metrics["network"]["receive_packets_total"] = total_rx_packets
        elif metric == "node_network_transmit_packets_total":
            total_tx_packets = sum(float(r["value"][1]) for r in results if r["metric"].get("device") != "lo")
            node_metrics["network"]["transmit_packets_total"] = total_tx_packets
    
    def _collect_local_metrics(self) -> Dict[str, Any]:
        """Collect metrics from the local system using direct system calls.
        
        Returns:
            Dictionary of OS metrics.
        """
        metrics = {
            "cpu": {},
            "memory": {},
            "disk": {},
            "network": {},
            "system": {}
        }
        
        try:
            # CPU metrics
            cpu_times = psutil.cpu_times()
            cpu_percent = psutil.cpu_percent(interval=None, percpu=False)
            load_avg = psutil.getloadavg()
            
            metrics["cpu"] = {
                "usage": cpu_percent,
                "load1": load_avg[0],
                "load5": load_avg[1],
                "load15": load_avg[2],
                "cores": psutil.cpu_count(logical=True),
                "physical_cores": psutil.cpu_count(logical=False),
                "times": {
                    "user": cpu_times.user,
                    "system": cpu_times.system,
                    "idle": cpu_times.idle,
                    "iowait": getattr(cpu_times, "iowait", 0)
                }
            }
            
            # Memory metrics
            mem = psutil.virtual_memory()
            swap = psutil.swap_memory()
            
            metrics["memory"] = {
                "total": mem.total,
                "available": mem.available,
                "used": mem.used,
                "free": mem.free,
                "cached": getattr(mem, "cached", 0),
                "buffers": getattr(mem, "buffers", 0),
                "percent": mem.percent,
                "swap_total": swap.total,
                "swap_used": swap.used,
                "swap_free": swap.free,
                "swap_percent": swap.percent
            }
            
            # Disk metrics
            io_counters = psutil.disk_io_counters()
            
            metrics["disk"] = {
                "read_count": io_counters.read_count,
                "write_count": io_counters.write_count,
                "read_bytes": io_counters.read_bytes,
                "write_bytes": io_counters.write_bytes,
                "read_time": io_counters.read_time,
                "write_time": io_counters.write_time,
                "busy_time": getattr(io_counters, "busy_time", 0),
                "partitions": {}
            }
            
            # Disk usage for each partition
            for partition in psutil.disk_partitions():
                try:
                    usage = psutil.disk_usage(partition.mountpoint)
                    metrics["disk"]["partitions"][partition.mountpoint] = {
                        "total": usage.total,
                        "used": usage.used,
                        "free": usage.free,
                        "percent": usage.percent,
                        "fstype": partition.fstype
                    }
                except (PermissionError, OSError):
                    # Skip partitions that can't be accessed
                    pass
            
            # Network metrics
            net_io = psutil.net_io_counters(pernic=True)
            
            metrics["network"] = {
                "interfaces": {},
                "total": {
                    "bytes_sent": 0,
                    "bytes_recv": 0,
                    "packets_sent": 0,
                    "packets_recv": 0,
                    "errin": 0,
                    "errout": 0,
                    "dropin": 0,
                    "dropout": 0
                }
            }
            
            # Per-interface metrics
            for interface, counters in net_io.items():
                if interface != "lo":  # Skip loopback
                    metrics["network"]["interfaces"][interface] = {
                        "bytes_sent": counters.bytes_sent,
                        "bytes_recv": counters.bytes_recv,
                        "packets_sent": counters.packets_sent,
                        "packets_recv": counters.packets_recv,
                        "errin": counters.errin,
                        "errout": counters.errout,
                        "dropin": counters.dropin,
                        "dropout": counters.dropout
                    }
                    
                    # Update totals
                    for key in metrics["network"]["total"]:
                        metrics["network"]["total"][key] += getattr(counters, key)
            
            # System metrics
            boot_time = psutil.boot_time()
            
            metrics["system"] = {
                "uptime": time.time() - boot_time,
                "boot_time": boot_time,
                "users": len(psutil.users()),
                "process_count": len(psutil.pids())
            }
            
            # Count Spark processes
            spark_processes = [p for p in psutil.process_iter(['name', 'cmdline']) 
                              if any('spark' in cmd.lower() for cmd in p.info['cmdline'] 
                                     if isinstance(cmd, str)) 
                              if p.info['cmdline']]
            
            metrics["system"]["spark_processes"] = len(spark_processes)
            
        except Exception as e:
            self.logger.error(f"Error collecting local metrics: {str(e)}")
            
        return metrics
    
    def _calculate_rates(self, metrics: Dict[str, Any], time_diff: float) -> Dict[str, Any]:
        """Calculate rates for cumulative metrics.
        
        Args:
            metrics: Current metrics.
            time_diff: Time difference in seconds since last collection.
            
        Returns:
            Metrics with rates added.
        """
        if not self.previous_metrics or "nodes" not in self.previous_metrics:
            return metrics
            
        for node, node_metrics in metrics.get("nodes", {}).items():
            if node not in self.previous_metrics.get("nodes", {}):
                continue
                
            prev_node_metrics = self.previous_metrics["nodes"][node]
            
            # Disk I/O rates
            if "disk" in node_metrics and "disk" in prev_node_metrics:
                current_disk = node_metrics["disk"]
                prev_disk = prev_node_metrics["disk"]
                
                # Add rate metrics
                if "read_bytes" in current_disk and "read_bytes" in prev_disk:
                    read_bytes_diff = current_disk["read_bytes"] - prev_disk["read_bytes"]
                    current_disk["read_bytes_per_sec"] = read_bytes_diff / time_diff
                    
                if "write_bytes" in current_disk and "write_bytes" in prev_disk:
                    write_bytes_diff = current_disk["write_bytes"] - prev_disk["write_bytes"]
                    current_disk["write_bytes_per_sec"] = write_bytes_diff / time_diff
                    
                # Prometheus specific metrics
                if "read_bytes_total" in current_disk and "read_bytes_total" in prev_disk:
                    read_bytes_diff = current_disk["read_bytes_total"] - prev_disk["read_bytes_total"]
                    current_disk["read_bytes_per_sec"] = read_bytes_diff / time_diff
                    
                if "written_bytes_total" in current_disk and "written_bytes_total" in prev_disk:
                    write_bytes_diff = current_disk["written_bytes_total"] - prev_disk["written_bytes_total"]
                    current_disk["write_bytes_per_sec"] = write_bytes_diff / time_diff
            
            # Network I/O rates
            if "network" in node_metrics and "network" in prev_node_metrics:
                current_net = node_metrics["network"]
                prev_net = prev_node_metrics["network"]
                
                # Direct metrics
                if "total" in current_net and "total" in prev_net:
                    current_total = current_net["total"]
                    prev_total = prev_net["total"]
                    
                    if "bytes_sent" in current_total and "bytes_sent" in prev_total:
                        bytes_sent_diff = current_total["bytes_sent"] - prev_total["bytes_sent"]
                        current_total["bytes_sent_per_sec"] = bytes_sent_diff / time_diff
                        
                    if "bytes_recv" in current_total and "bytes_recv" in prev_total:
                        bytes_recv_diff = current_total["bytes_recv"] - prev_total["bytes_recv"]
                        current_total["bytes_recv_per_sec"] = bytes_recv_diff / time_diff
                
                # Prometheus metrics
                if "transmit_bytes_total" in current_net and "transmit_bytes_total" in prev_net:
                    tx_bytes_diff = current_net["transmit_bytes_total"] - prev_net["transmit_bytes_total"]
                    current_net["transmit_bytes_per_sec"] = tx_bytes_diff / time_diff
                    
                if "receive_bytes_total" in current_net and "receive_bytes_total" in prev_net:
                    rx_bytes_diff = current_net["receive_bytes_total"] - prev_net["receive_bytes_total"]
                    current_net["receive_bytes_per_sec"] = rx_bytes_diff / time_diff
        
        return metrics