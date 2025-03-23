"""
API for managing Databricks Unity Catalog Catalogs.

This module provides functions for managing catalogs in Databricks Unity Catalog.
It is part of the Catalog API group.
"""

import logging
from typing import Any, Dict, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

async def create_catalog(name: str, comment: Optional[str] = None) -> Dict[str, Any]:
    """Create a new catalog in the Unity Catalog.
    
    Args:
        name: Name of the catalog
        comment: Optional comment for the catalog
        
    Returns:
        Response containing the created catalog info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new catalog: {name}")
    
    data = {"name": name}
    
    if comment is not None:
        data["comment"] = comment
    
    try:
        return await make_api_request("POST", "/api/2.1/unity-catalog/catalogs", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to create catalog: {str(e)}")
        raise


async def list_catalogs(max_results: Optional[int] = None) -> Dict[str, Any]:
    """List catalogs in the Unity Catalog.
    
    Args:
        max_results: Optional maximum number of results to return
        
    Returns:
        Response containing the list of catalogs
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing catalogs")
    
    params = {}
    
    if max_results is not None:
        params["max_results"] = max_results
    
    try:
        return await make_api_request("GET", "/api/2.1/unity-catalog/catalogs", params=params)
    except DatabricksAPIError as e:
        logger.error(f"Failed to list catalogs: {str(e)}")
        raise


async def get_catalog(name: str) -> Dict[str, Any]:
    """Get a specific catalog from the Unity Catalog.
    
    Args:
        name: Name of the catalog
        
    Returns:
        Response containing the catalog info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting catalog: {name}")
    
    try:
        return await make_api_request("GET", f"/api/2.1/unity-catalog/catalogs/{name}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to get catalog: {str(e)}")
        raise


async def update_catalog(name: str, new_name: Optional[str] = None, comment: Optional[str] = None) -> Dict[str, Any]:
    """Update a catalog in the Unity Catalog.
    
    Args:
        name: Name of the catalog to update
        new_name: Optional new name for the catalog
        comment: Optional new comment for the catalog
        
    Returns:
        Response containing the updated catalog info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating catalog: {name}")
    
    data = {}
    
    if new_name is not None:
        data["name"] = new_name
        
    if comment is not None:
        data["comment"] = comment
    
    try:
        return await make_api_request("PATCH", f"/api/2.1/unity-catalog/catalogs/{name}", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to update catalog: {str(e)}")
        raise


async def delete_catalog(name: str) -> Dict[str, Any]:
    """Delete a catalog from the Unity Catalog.
    
    Args:
        name: Name of the catalog to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting catalog: {name}")
    
    try:
        return await make_api_request("DELETE", f"/api/2.1/unity-catalog/catalogs/{name}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to delete catalog: {str(e)}")
        raise 