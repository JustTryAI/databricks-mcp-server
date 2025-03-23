"""
API for managing Databricks Unity Catalog Tables.

This module provides functions for managing tables in Databricks Unity Catalog.
It is part of the Catalog API group.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

async def create_table(
    catalog_name: str,
    schema_name: str,
    name: str,
    columns: List[Dict[str, Any]],
    comment: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None,
    storage_location: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a new table in the Unity Catalog.
    
    Args:
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        name: Name of the table
        columns: List of column definitions, each with 'name', 'type_name', and optional 'comment'
        comment: Optional comment for the table
        properties: Optional properties for the table
        storage_location: Optional storage location
        
    Returns:
        Response containing the created table info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new table: {catalog_name}.{schema_name}.{name}")
    
    data = {
        "name": name,
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "columns": columns
    }
    
    if comment is not None:
        data["comment"] = comment
        
    if properties is not None:
        data["properties"] = properties
        
    if storage_location is not None:
        data["storage_location"] = storage_location
    
    try:
        return await make_api_request("POST", "/api/2.1/unity-catalog/tables", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to create table: {str(e)}")
        raise


async def list_tables(
    catalog_name: str,
    schema_name: str,
    max_results: Optional[int] = None,
    page_token: Optional[str] = None
) -> Dict[str, Any]:
    """List tables in a schema.
    
    Args:
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        max_results: Optional maximum number of results to return
        page_token: Optional page token for pagination
        
    Returns:
        Response containing the list of tables
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Listing tables in schema: {catalog_name}.{schema_name}")
    
    params = {
        "catalog_name": catalog_name,
        "schema_name": schema_name
    }
    
    if max_results is not None:
        params["max_results"] = max_results
        
    if page_token is not None:
        params["page_token"] = page_token
    
    try:
        return await make_api_request("GET", "/api/2.1/unity-catalog/tables", params=params)
    except DatabricksAPIError as e:
        logger.error(f"Failed to list tables: {str(e)}")
        raise


async def get_table(full_name: str) -> Dict[str, Any]:
    """Get a specific table from the Unity Catalog.
    
    Args:
        full_name: Full name of the table in the format 'catalog_name.schema_name.table_name'
        
    Returns:
        Response containing the table info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting table: {full_name}")
    
    try:
        return await make_api_request("GET", f"/api/2.1/unity-catalog/tables/{full_name}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to get table: {str(e)}")
        raise


async def update_table(
    full_name: str,
    new_name: Optional[str] = None,
    comment: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None,
    owner: Optional[str] = None
) -> Dict[str, Any]:
    """Update a table in the Unity Catalog.
    
    Args:
        full_name: Full name of the table in the format 'catalog_name.schema_name.table_name'
        new_name: Optional new name for the table
        comment: Optional new comment for the table
        properties: Optional new properties for the table
        owner: Optional new owner for the table
        
    Returns:
        Response containing the updated table info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating table: {full_name}")
    
    data = {}
    
    if new_name is not None:
        data["name"] = new_name
        
    if comment is not None:
        data["comment"] = comment
        
    if properties is not None:
        data["properties"] = properties
        
    if owner is not None:
        data["owner"] = owner
    
    try:
        return await make_api_request("PATCH", f"/api/2.1/unity-catalog/tables/{full_name}", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to update table: {str(e)}")
        raise


async def delete_table(full_name: str) -> Dict[str, Any]:
    """Delete a table from the Unity Catalog.
    
    Args:
        full_name: Full name of the table in the format 'catalog_name.schema_name.table_name'
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting table: {full_name}")
    
    try:
        return await make_api_request("DELETE", f"/api/2.1/unity-catalog/tables/{full_name}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to delete table: {str(e)}")
        raise 