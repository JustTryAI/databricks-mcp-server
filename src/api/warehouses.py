"""
API for managing Databricks SQL Warehouses.
"""

import logging
from typing import Any, Dict, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

async def list_warehouses() -> Dict[str, Any]:
    """
    List all SQL warehouses.
    
    Returns:
        Response containing a list of SQL warehouses
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing SQL warehouses")
    return make_api_request("GET", "/api/2.0/sql/warehouses")


async def create_warehouse(
    name: str,
    cluster_size: str,
    auto_stop_mins: Optional[int] = None,
    min_num_clusters: Optional[int] = None,
    max_num_clusters: Optional[int] = None,
    enable_photon: Optional[bool] = None,
    additional_params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Create a new SQL warehouse.
    
    Args:
        name: Name for the SQL warehouse
        cluster_size: Size of the warehouse cluster
        auto_stop_mins: Optional minutes of inactivity before stopping
        min_num_clusters: Optional minimum number of clusters
        max_num_clusters: Optional maximum number of clusters
        enable_photon: Optional flag to enable Photon acceleration
        additional_params: Optional additional parameters
        
    Returns:
        Response containing the created SQL warehouse information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new SQL warehouse: {name}")
    
    data = {
        "name": name,
        "cluster_size": cluster_size,
    }
    
    if auto_stop_mins is not None:
        data["auto_stop_mins"] = auto_stop_mins
        
    if min_num_clusters is not None:
        data["min_num_clusters"] = min_num_clusters
        
    if max_num_clusters is not None:
        data["max_num_clusters"] = max_num_clusters
        
    if enable_photon is not None:
        data["enable_photon"] = enable_photon
        
    if additional_params:
        data.update(additional_params)
    
    return make_api_request("POST", "/api/2.0/sql/warehouses", data=data)


async def get_warehouse(id: str) -> Dict[str, Any]:
    """
    Get information about a specific SQL warehouse.
    
    Args:
        id: ID of the SQL warehouse to get
        
    Returns:
        Response containing the SQL warehouse information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting SQL warehouse with ID: {id}")
    return make_api_request("GET", f"/api/2.0/sql/warehouses/{id}")


async def delete_warehouse(id: str) -> Dict[str, Any]:
    """
    Delete a SQL warehouse.
    
    Args:
        id: ID of the SQL warehouse to delete
        
    Returns:
        Empty response if successful
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting SQL warehouse with ID: {id}")
    return make_api_request("DELETE", f"/api/2.0/sql/warehouses/{id}")


async def update_warehouse(
    id: str,
    name: Optional[str] = None,
    cluster_size: Optional[str] = None,
    auto_stop_mins: Optional[int] = None,
    min_num_clusters: Optional[int] = None,
    max_num_clusters: Optional[int] = None,
    enable_photon: Optional[bool] = None,
    additional_params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Update a SQL warehouse.
    
    Args:
        id: ID of the SQL warehouse to update
        name: Optional new name for the SQL warehouse
        cluster_size: Optional new size for the SQL warehouse cluster
        auto_stop_mins: Optional new auto-stop minutes value
        min_num_clusters: Optional new minimum number of clusters
        max_num_clusters: Optional new maximum number of clusters
        enable_photon: Optional flag to enable Photon acceleration
        additional_params: Optional additional parameters
        
    Returns:
        Response containing the updated SQL warehouse information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating SQL warehouse with ID: {id}")
    
    data = {}
    
    if name is not None:
        data["name"] = name
        
    if cluster_size is not None:
        data["cluster_size"] = cluster_size
        
    if auto_stop_mins is not None:
        data["auto_stop_mins"] = auto_stop_mins
        
    if min_num_clusters is not None:
        data["min_num_clusters"] = min_num_clusters
        
    if max_num_clusters is not None:
        data["max_num_clusters"] = max_num_clusters
        
    if enable_photon is not None:
        data["enable_photon"] = enable_photon
        
    if additional_params:
        data.update(additional_params)
    
    return make_api_request("POST", f"/api/2.0/sql/warehouses/{id}/edit", data=data)


async def start_warehouse(id: str) -> Dict[str, Any]:
    """
    Start a SQL warehouse.
    
    Args:
        id: ID of the SQL warehouse to start
        
    Returns:
        Response containing the start operation result
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Starting SQL warehouse with ID: {id}")
    return make_api_request("POST", f"/api/2.0/sql/warehouses/{id}/start")


async def stop_warehouse(id: str) -> Dict[str, Any]:
    """
    Stop a SQL warehouse.
    
    Args:
        id: ID of the SQL warehouse to stop
        
    Returns:
        Response containing the stop operation result
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Stopping SQL warehouse with ID: {id}")
    return make_api_request("POST", f"/api/2.0/sql/warehouses/{id}/stop") 