"""
API for managing Databricks clusters.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def create_cluster(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new Databricks cluster.
    
    Args:
        cluster_config: Cluster configuration
        
    Returns:
        Response containing the cluster ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Creating new cluster")
    return make_api_request("POST", "/api/2.0/clusters/create", data=cluster_config)


async def terminate_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Terminate a Databricks cluster.
    
    Args:
        cluster_id: ID of the cluster to terminate
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Terminating cluster: {cluster_id}")
    return make_api_request("POST", "/api/2.0/clusters/delete", data={"cluster_id": cluster_id})


async def list_clusters() -> Dict[str, Any]:
    """
    List all Databricks clusters.
    
    Returns:
        Response containing a list of clusters
        
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
        Response containing cluster information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting information for cluster: {cluster_id}")
    return make_api_request("GET", "/api/2.0/clusters/get", params={"cluster_id": cluster_id})


async def start_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Start a terminated Databricks cluster.
    
    Args:
        cluster_id: ID of the cluster to start
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Starting cluster: {cluster_id}")
    return make_api_request("POST", "/api/2.0/clusters/start", data={"cluster_id": cluster_id})


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
    return make_api_request(
        "POST", 
        "/api/2.0/clusters/resize", 
        data={"cluster_id": cluster_id, "num_workers": num_workers}
    )


async def restart_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Restart a Databricks cluster.
    
    Args:
        cluster_id: ID of the cluster to restart
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Restarting cluster: {cluster_id}")
    return make_api_request("POST", "/api/2.0/clusters/restart", data={"cluster_id": cluster_id})


async def permanent_delete_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Permanently delete a Databricks cluster.
    
    Args:
        cluster_id: ID of the cluster to permanently delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Permanently deleting cluster: {cluster_id}")
    return make_api_request("POST", "/api/2.1/clusters/permanent-delete", data={"cluster_id": cluster_id}) 