"""
API for managing libraries in Databricks clusters.

This module provides functions for interacting with the Databricks Libraries API.
It is part of the compute API group that includes clusters and libraries.
"""

import logging
from typing import Any, Dict, List, Optional, Union

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def install_libraries(
    cluster_id: str,
    libraries: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Install libraries on a cluster.
    
    Args:
        cluster_id: ID of the cluster to install libraries on
        libraries: List of library specifications
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Installing libraries on cluster: {cluster_id}")
    
    data = {
        "cluster_id": cluster_id,
        "libraries": libraries,
    }
    
    return make_api_request("POST", "/api/2.0/libraries/install", data=data)


async def uninstall_libraries(
    cluster_id: str,
    libraries: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Uninstall libraries from a cluster.
    
    Args:
        cluster_id: ID of the cluster to uninstall libraries from
        libraries: List of library specifications to uninstall
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Uninstalling libraries from cluster: {cluster_id}")
    
    data = {
        "cluster_id": cluster_id,
        "libraries": libraries,
    }
    
    return make_api_request("POST", "/api/2.0/libraries/uninstall", data=data)


async def get_library_status(cluster_id: str) -> Dict[str, Any]:
    """
    Get the status of all libraries on a cluster.
    
    Args:
        cluster_id: ID of the cluster
        
    Returns:
        Response containing library status information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting library status for cluster: {cluster_id}")
    return make_api_request(
        "GET", "/api/2.0/libraries/cluster-status", params={"cluster_id": cluster_id}
    )


async def get_all_libraries() -> Dict[str, Any]:
    """
    Get the status of all libraries on all clusters.
    
    Returns:
        Response containing library status information for all clusters
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Getting status of all libraries on all clusters")
    return make_api_request("GET", "/api/2.0/libraries/all-cluster-statuses")


# Alias for get_all_libraries for compatibility
async def list_libraries() -> Dict[str, Any]:
    """
    Get the status of all libraries on all clusters.
    
    Returns:
        Response containing library status information for all clusters
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await get_all_libraries()


# Alias for get_library_status for compatibility
async def list_library_statuses(cluster_id: str) -> Dict[str, Any]:
    """
    Get the status of all libraries on a cluster.
    
    Args:
        cluster_id: ID of the cluster
        
    Returns:
        Response containing library status information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await get_library_status(cluster_id) 