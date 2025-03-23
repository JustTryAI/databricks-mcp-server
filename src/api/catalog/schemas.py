"""
API for managing Databricks Unity Catalog Schemas.

This module provides functions for managing schemas in Databricks Unity Catalog.
It is part of the Catalog API group.
"""

import logging
from typing import Any, Dict, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

async def create_schema(catalog_name: str, name: str, comment: Optional[str] = None) -> Dict[str, Any]:
    """Create a new schema in the Unity Catalog.
    
    Args:
        catalog_name: Name of the catalog to create the schema in
        name: Name of the schema
        comment: Optional comment for the schema
        
    Returns:
        Response containing the created schema info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new schema: {catalog_name}.{name}")
    
    data = {
        "name": name,
        "catalog_name": catalog_name
    }
    
    if comment is not None:
        data["comment"] = comment
    
    try:
        return await make_api_request("POST", "/api/2.1/unity-catalog/schemas", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to create schema: {str(e)}")
        raise


async def list_schemas(catalog_name: str, max_results: Optional[int] = None) -> Dict[str, Any]:
    """List schemas in a catalog.
    
    Args:
        catalog_name: Name of the catalog to list schemas from
        max_results: Optional maximum number of results to return
        
    Returns:
        Response containing the list of schemas
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Listing schemas in catalog: {catalog_name}")
    
    params = {"catalog_name": catalog_name}
    
    if max_results is not None:
        params["max_results"] = max_results
    
    try:
        return await make_api_request("GET", "/api/2.1/unity-catalog/schemas", params=params)
    except DatabricksAPIError as e:
        logger.error(f"Failed to list schemas: {str(e)}")
        raise


async def get_schema(full_name: str) -> Dict[str, Any]:
    """Get a specific schema from the Unity Catalog.
    
    Args:
        full_name: Full name of the schema in the format 'catalog_name.schema_name'
        
    Returns:
        Response containing the schema info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting schema: {full_name}")
    
    try:
        return await make_api_request("GET", f"/api/2.1/unity-catalog/schemas/{full_name}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to get schema: {str(e)}")
        raise


async def update_schema(full_name: str, new_name: Optional[str] = None, comment: Optional[str] = None) -> Dict[str, Any]:
    """Update a schema in the Unity Catalog.
    
    Args:
        full_name: Full name of the schema in the format 'catalog_name.schema_name'
        new_name: Optional new name for the schema
        comment: Optional new comment for the schema
        
    Returns:
        Response containing the updated schema info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating schema: {full_name}")
    
    data = {}
    
    if new_name is not None:
        data["name"] = new_name
        
    if comment is not None:
        data["comment"] = comment
    
    try:
        return await make_api_request("PATCH", f"/api/2.1/unity-catalog/schemas/{full_name}", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to update schema: {str(e)}")
        raise


async def delete_schema(full_name: str) -> Dict[str, Any]:
    """Delete a schema from the Unity Catalog.
    
    Args:
        full_name: Full name of the schema in the format 'catalog_name.schema_name'
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting schema: {full_name}")
    
    try:
        return await make_api_request("DELETE", f"/api/2.1/unity-catalog/schemas/{full_name}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to delete schema: {str(e)}")
        raise 