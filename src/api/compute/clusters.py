"""
API for managing Databricks clusters.

This module provides functions for interacting with the Databricks Clusters API.
It is part of the compute API group that includes clusters and libraries.
"""

import logging
from typing import Any, Dict, List, Optional, Union

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def create_cluster(
    cluster_name: str,
    spark_version: str,
    node_type_id: str,
    num_workers: int = 0,
    autoscale: Optional[Dict[str, int]] = None,
    spark_conf: Optional[Dict[str, str]] = None,
    aws_attributes: Optional[Dict[str, Any]] = None,
    ssh_public_keys: Optional[List[str]] = None,
    custom_tags: Optional[Dict[str, str]] = None,
    cluster_log_conf: Optional[Dict[str, str]] = None,
    init_scripts: Optional[List[Dict[str, str]]] = None,
    spark_env_vars: Optional[Dict[str, str]] = None,
    enable_elastic_disk: Optional[bool] = None,
    driver_node_type_id: Optional[str] = None,
    runtime_engine: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Create a new Databricks cluster.
    
    Args:
        cluster_name: Name of the cluster
        spark_version: Spark version to use
        node_type_id: Type of nodes in the cluster
        num_workers: Number of worker nodes
        autoscale: Autoscaling configuration
        spark_conf: Spark configuration
        aws_attributes: AWS-specific attributes
        ssh_public_keys: SSH public keys for cluster access
        custom_tags: Custom tags for the cluster
        cluster_log_conf: Cluster log configuration
        init_scripts: Initialization scripts
        spark_env_vars: Spark environment variables
        enable_elastic_disk: Whether to enable elastic disk
        driver_node_type_id: Type of driver node
        runtime_engine: Runtime engine (photon or standard)
        **kwargs: Additional parameters for cluster creation
        
    Returns:
        Response containing the cluster ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new cluster: {cluster_name}")
    
    # Build the cluster configuration
    cluster_config = {
        "cluster_name": cluster_name,
        "spark_version": spark_version,
        "node_type_id": node_type_id,
    }
    
    # Configure autoscaling or fixed size
    if autoscale:
        cluster_config["autoscale"] = autoscale
    else:
        cluster_config["num_workers"] = num_workers
    
    # Add optional configurations
    optional_configs = {
        "spark_conf": spark_conf,
        "aws_attributes": aws_attributes,
        "ssh_public_keys": ssh_public_keys,
        "custom_tags": custom_tags,
        "cluster_log_conf": cluster_log_conf,
        "init_scripts": init_scripts,
        "spark_env_vars": spark_env_vars,
        "enable_elastic_disk": enable_elastic_disk,
        "driver_node_type_id": driver_node_type_id,
        "runtime_engine": runtime_engine,
    }
    
    for key, value in optional_configs.items():
        if value is not None:
            cluster_config[key] = value
    
    # Add any additional parameters
    for key, value in kwargs.items():
        cluster_config[key] = value
    
    return make_api_request("POST", "/api/2.0/clusters/create", data=cluster_config)


async def list_clusters() -> Dict[str, Any]:
    """
    List all clusters in the workspace.
    
    Returns:
        Response containing the list of clusters
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing all clusters")
    return make_api_request("GET", "/api/2.0/clusters/list")


async def get_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Get information about a specific cluster.
    
    Args:
        cluster_id: ID of the cluster
        
    Returns:
        Response containing the cluster information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting information for cluster: {cluster_id}")
    return make_api_request("GET", "/api/2.0/clusters/get", params={"cluster_id": cluster_id})


async def resize_cluster(cluster_id: str, num_workers: int) -> Dict[str, Any]:
    """
    Resize a cluster by changing the number of workers.
    
    Args:
        cluster_id: ID of the cluster to resize
        num_workers: New number of workers
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Resizing cluster {cluster_id} to {num_workers} workers")
    
    resize_data = {
        "cluster_id": cluster_id,
        "num_workers": num_workers
    }
    
    return make_api_request("POST", "/api/2.0/clusters/resize", data=resize_data)


async def start_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Start a terminated cluster.
    
    Args:
        cluster_id: ID of the cluster to start
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Starting cluster: {cluster_id}")
    return make_api_request("POST", "/api/2.0/clusters/start", data={"cluster_id": cluster_id})


async def restart_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Restart a cluster.
    
    Args:
        cluster_id: ID of the cluster to restart
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Restarting cluster: {cluster_id}")
    return make_api_request("POST", "/api/2.0/clusters/restart", data={"cluster_id": cluster_id})


async def terminate_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Terminate a cluster.
    
    Args:
        cluster_id: ID of the cluster to terminate
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Terminating cluster: {cluster_id}")
    return make_api_request(
        "POST", "/api/2.0/clusters/delete", data={"cluster_id": cluster_id}
    )


async def delete_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Permanently delete a cluster.
    
    Args:
        cluster_id: ID of the cluster to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Permanently deleting cluster: {cluster_id}")
    return make_api_request(
        "POST", "/api/2.1/clusters/permanent-delete", data={"cluster_id": cluster_id}
    )


async def get_cluster_events(
    cluster_id: str,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    order: str = "DESC",
    event_types: Optional[List[str]] = None,
    offset: int = 0,
    limit: int = 50,
) -> Dict[str, Any]:
    """
    Get events for a specific cluster.
    
    Args:
        cluster_id: ID of the cluster
        start_time: Optional start time (in ms since epoch)
        end_time: Optional end time (in ms since epoch)
        order: Order of events (ASC or DESC)
        event_types: Optional list of event types to filter by
        offset: Offset for pagination
        limit: Maximum number of events to return
        
    Returns:
        Response containing the list of events
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting events for cluster: {cluster_id}")
    
    data = {
        "cluster_id": cluster_id,
        "order": order,
        "offset": offset,
        "limit": limit
    }
    
    if start_time:
        data["start_time"] = start_time
    
    if end_time:
        data["end_time"] = end_time
    
    if event_types:
        data["event_types"] = event_types
    
    return make_api_request("POST", "/api/2.0/clusters/events", data=data)


async def list_node_types() -> Dict[str, Any]:
    """
    List available node types for clusters.
    
    Returns:
        Response containing the list of node types
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing available node types")
    return make_api_request("GET", "/api/2.0/clusters/list-node-types")


async def list_spark_versions() -> Dict[str, Any]:
    """
    List available Spark versions for clusters.
    
    Returns:
        Response containing the list of Spark versions
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing available Spark versions")
    return make_api_request("GET", "/api/2.0/clusters/spark-versions")


async def pin_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Pin a cluster.
    
    Args:
        cluster_id: ID of the cluster to pin
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Pinning cluster: {cluster_id}")
    return make_api_request("POST", "/api/2.0/clusters/pin", data={"cluster_id": cluster_id})


async def unpin_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Unpin a cluster.
    
    Args:
        cluster_id: ID of the cluster to unpin
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Unpinning cluster: {cluster_id}")
    return make_api_request("POST", "/api/2.0/clusters/unpin", data={"cluster_id": cluster_id}) 