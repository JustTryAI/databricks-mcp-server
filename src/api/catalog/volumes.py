"""
API for managing Databricks Unity Catalog Volumes.

This module provides functions for managing volumes in Databricks Unity Catalog.
It is part of the Catalog API group.
"""

import logging
from typing import Any, Dict, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

async def create_volume(
    catalog_name: str,
    schema_name: str,
    name: str,
    volume_type: str,
    storage_location: Optional[str] = None,
    comment: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a new volume in the Unity Catalog.
    
    Args:
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        name: Name of the volume
        volume_type: Type of volume (e.g., 'EXTERNAL', 'MANAGED')
        storage_location: Optional storage location for EXTERNAL volumes
        comment: Optional comment for the volume
        
    Returns:
        Response containing the created volume info
        
    Raises:
        DatabricksAPIError: If the API request fails
        ValueError: If storage_location is missing for EXTERNAL volumes
    """
    logger.info(f"Creating new volume: {catalog_name}.{schema_name}.{name}")
    
    # Check if storage_location is provided for EXTERNAL volumes
    if volume_type == "EXTERNAL" and storage_location is None:
        raise ValueError("storage_location is required for EXTERNAL volumes")
    
    data = {
        "name": name,
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "volume_type": volume_type
    }
    
    if storage_location is not None:
        data["storage_location"] = storage_location
        
    if comment is not None:
        data["comment"] = comment
    
    try:
        return await make_api_request("POST", "/api/2.1/unity-catalog/volumes", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to create volume: {str(e)}")
        raise


async def list_volumes(
    catalog_name: str,
    schema_name: str,
    max_results: Optional[int] = None,
    page_token: Optional[str] = None
) -> Dict[str, Any]:
    """List volumes in a schema.
    
    Args:
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        max_results: Optional maximum number of results to return
        page_token: Optional page token for pagination
        
    Returns:
        Response containing the list of volumes
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Listing volumes in schema: {catalog_name}.{schema_name}")
    
    params = {
        "catalog_name": catalog_name,
        "schema_name": schema_name
    }
    
    if max_results is not None:
        params["max_results"] = max_results
        
    if page_token is not None:
        params["page_token"] = page_token
    
    try:
        return await make_api_request("GET", "/api/2.1/unity-catalog/volumes", params=params)
    except DatabricksAPIError as e:
        logger.error(f"Failed to list volumes: {str(e)}")
        raise


async def get_volume(full_name: str) -> Dict[str, Any]:
    """Get a specific volume from the Unity Catalog.
    
    Args:
        full_name: Full name of the volume in the format 'catalog_name.schema_name.volume_name'
        
    Returns:
        Response containing the volume info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting volume: {full_name}")
    
    try:
        return await make_api_request("GET", f"/api/2.1/unity-catalog/volumes/{full_name}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to get volume: {str(e)}")
        raise


async def update_volume(
    full_name: str,
    new_name: Optional[str] = None,
    comment: Optional[str] = None,
    owner: Optional[str] = None
) -> Dict[str, Any]:
    """Update a volume in the Unity Catalog.
    
    Args:
        full_name: Full name of the volume in the format 'catalog_name.schema_name.volume_name'
        new_name: Optional new name for the volume
        comment: Optional new comment for the volume
        owner: Optional new owner for the volume
        
    Returns:
        Response containing the updated volume info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating volume: {full_name}")
    
    data = {}
    
    if new_name is not None:
        data["name"] = new_name
        
    if comment is not None:
        data["comment"] = comment
        
    if owner is not None:
        data["owner"] = owner
    
    try:
        return await make_api_request("PATCH", f"/api/2.1/unity-catalog/volumes/{full_name}", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to update volume: {str(e)}")
        raise


async def delete_volume(full_name: str) -> Dict[str, Any]:
    """Delete a volume from the Unity Catalog.
    
    Args:
        full_name: Full name of the volume in the format 'catalog_name.schema_name.volume_name'
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting volume: {full_name}")
    
    try:
        return await make_api_request("DELETE", f"/api/2.1/unity-catalog/volumes/{full_name}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to delete volume: {str(e)}")
        raise 