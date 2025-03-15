"""
API for managing libraries on Databricks clusters.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def install_libraries(cluster_id: str, libraries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Install libraries on a Databricks cluster.
    
    Args:
        cluster_id: ID of the cluster to install libraries on
        libraries: List of libraries to install
        
    Returns:
        Response indicating success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Installing libraries on cluster: {cluster_id}")
    
    data = {
        "cluster_id": cluster_id,
        "libraries": libraries
    }
    
    return make_api_request("POST", "/api/2.0/libraries/install", data=data)


async def uninstall_libraries(cluster_id: str, libraries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Uninstall libraries from a Databricks cluster.
    
    Args:
        cluster_id: ID of the cluster to uninstall libraries from
        libraries: List of libraries to uninstall
        
    Returns:
        Response indicating success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Uninstalling libraries from cluster: {cluster_id}")
    
    data = {
        "cluster_id": cluster_id,
        "libraries": libraries
    }
    
    return make_api_request("POST", "/api/2.0/libraries/uninstall", data=data)


async def list_libraries(cluster_id: str) -> Dict[str, Any]:
    """
    List libraries installed on a Databricks cluster.
    
    Args:
        cluster_id: ID of the cluster to list libraries for
        
    Returns:
        Response containing a list of installed libraries
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Listing libraries for cluster: {cluster_id}")
    return make_api_request("GET", f"/api/2.0/libraries/list?cluster_id={cluster_id}")


async def list_library_statuses(cluster_id: str) -> Dict[str, Any]:
    """
    Get the status of libraries on a Databricks cluster.
    
    Args:
        cluster_id: ID of the cluster to get library statuses for
        
    Returns:
        Response containing library statuses
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting library statuses for cluster: {cluster_id}")
    return make_api_request("GET", f"/api/2.0/libraries/cluster-status?cluster_id={cluster_id}") 