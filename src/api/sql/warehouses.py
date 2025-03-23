"""
API for managing Databricks SQL Warehouses.

This module provides functions for managing SQL Warehouses in Databricks.
It is part of the SQL API group that includes Warehouses, Queries,
Dashboards, and Alerts.
"""

import logging
from typing import Any, Dict, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

async def list_warehouses() -> Dict[str, Any]:
    """List all SQL warehouses.
    
    Returns:
        Response containing a list of SQL warehouses
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing SQL warehouses")
    return await make_api_request("GET", "/api/2.0/sql/warehouses")


async def create_warehouse(
    name: str,
    cluster_size: str,
    auto_stop_mins: Optional[int] = None,
    min_num_clusters: Optional[int] = None,
    max_num_clusters: Optional[int] = None,
    enable_photon: Optional[bool] = None,
    additional_params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Create a new SQL warehouse.
    
    Args:
        name: Name for the SQL warehouse
        cluster_size: Size of the clusters in the warehouse (e.g., "2X-Small", "Small", "Medium", etc.)
        auto_stop_mins: Minutes of inactivity after which the warehouse will be automatically stopped
        min_num_clusters: Minimum number of clusters for auto-scaling
        max_num_clusters: Maximum number of clusters for auto-scaling
        enable_photon: Whether to enable Photon acceleration
        additional_params: Additional parameters for warehouse creation
        
    Returns:
        Response containing the created SQL warehouse information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating SQL warehouse: {name}")
    
    data = {
        "name": name,
        "cluster_size": cluster_size
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
    
    try:
        return await make_api_request("POST", "/api/2.0/sql/warehouses", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to create SQL warehouse: {str(e)}")
        raise


async def get_warehouse(id: str) -> Dict[str, Any]:
    """Get a specific SQL warehouse.
    
    Args:
        id: ID of the SQL warehouse
        
    Returns:
        Response containing the SQL warehouse information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting SQL warehouse: {id}")
    
    try:
        return await make_api_request("GET", f"/api/2.0/sql/warehouses/{id}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to get SQL warehouse: {str(e)}")
        raise


async def delete_warehouse(id: str) -> Dict[str, Any]:
    """Delete a SQL warehouse.
    
    Args:
        id: ID of the SQL warehouse to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting SQL warehouse: {id}")
    
    try:
        return await make_api_request("DELETE", f"/api/2.0/sql/warehouses/{id}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to delete SQL warehouse: {str(e)}")
        raise


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
    """Update a SQL warehouse.
    
    Args:
        id: ID of the SQL warehouse to update
        name: New name for the SQL warehouse
        cluster_size: New size of the clusters in the warehouse
        auto_stop_mins: New auto-stop time in minutes
        min_num_clusters: New minimum number of clusters for auto-scaling
        max_num_clusters: New maximum number of clusters for auto-scaling
        enable_photon: Whether to enable Photon acceleration
        additional_params: Additional parameters to update
        
    Returns:
        Response containing the updated SQL warehouse information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating SQL warehouse: {id}")
    
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
    
    try:
        return await make_api_request("PATCH", f"/api/2.0/sql/warehouses/{id}", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to update SQL warehouse: {str(e)}")
        raise


async def start_warehouse(id: str) -> Dict[str, Any]:
    """Start a SQL warehouse.
    
    Args:
        id: ID of the SQL warehouse to start
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Starting SQL warehouse: {id}")
    
    try:
        return await make_api_request("POST", f"/api/2.0/sql/warehouses/{id}/start")
    except DatabricksAPIError as e:
        logger.error(f"Failed to start SQL warehouse: {str(e)}")
        raise


async def stop_warehouse(id: str) -> Dict[str, Any]:
    """Stop a SQL warehouse.
    
    Args:
        id: ID of the SQL warehouse to stop
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Stopping SQL warehouse: {id}")
    
    try:
        return await make_api_request("POST", f"/api/2.0/sql/warehouses/{id}/stop")
    except DatabricksAPIError as e:
        logger.error(f"Failed to stop SQL warehouse: {str(e)}")
        raise 