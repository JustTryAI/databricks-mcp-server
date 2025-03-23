"""
API for managing Databricks Unity Catalog Connections.

This module provides functions for managing connections in Databricks Unity Catalog.
It is part of the Catalog API group.
"""

import logging
from typing import Any, Dict, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

async def create_connection(
    name: str,
    connection_type: str,
    options: Dict[str, str],
    comment: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Create a new connection in the Unity Catalog.
    
    Args:
        name: Name of the connection
        connection_type: Type of the connection (e.g., 'MYSQL', 'POSTGRESQL', 'REDSHIFT', etc.)
        options: Connection options specific to the connection type
        comment: Optional comment for the connection
        properties: Optional properties for the connection
        
    Returns:
        Response containing the created connection info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new connection: {name} of type {connection_type}")
    
    data = {
        "name": name,
        "connection_type": connection_type,
        "options": options
    }
    
    if comment is not None:
        data["comment"] = comment
        
    if properties is not None:
        data["properties"] = properties
    
    try:
        return await make_api_request("POST", "/api/2.1/unity-catalog/connections", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to create connection: {str(e)}")
        raise


async def list_connections(max_results: Optional[int] = None) -> Dict[str, Any]:
    """List connections in the Unity Catalog.
    
    Args:
        max_results: Optional maximum number of results to return
        
    Returns:
        Response containing the list of connections
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing connections")
    
    params = {}
    
    if max_results is not None:
        params["max_results"] = max_results
    
    try:
        return await make_api_request("GET", "/api/2.1/unity-catalog/connections", params=params)
    except DatabricksAPIError as e:
        logger.error(f"Failed to list connections: {str(e)}")
        raise


async def get_connection(name: str) -> Dict[str, Any]:
    """Get a specific connection from the Unity Catalog.
    
    Args:
        name: Name of the connection
        
    Returns:
        Response containing the connection info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting connection: {name}")
    
    try:
        return await make_api_request("GET", f"/api/2.1/unity-catalog/connections/{name}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to get connection: {str(e)}")
        raise


async def update_connection(
    name: str,
    new_name: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    comment: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None,
    owner: Optional[str] = None,
) -> Dict[str, Any]:
    """Update a connection in the Unity Catalog.
    
    Args:
        name: Name of the connection to update
        new_name: Optional new name for the connection
        options: Optional new options for the connection
        comment: Optional new comment for the connection
        properties: Optional new properties for the connection
        owner: Optional new owner for the connection
        
    Returns:
        Response containing the updated connection info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating connection: {name}")
    
    data = {}
    
    if new_name is not None:
        data["name"] = new_name
        
    if options is not None:
        data["options"] = options
        
    if comment is not None:
        data["comment"] = comment
        
    if properties is not None:
        data["properties"] = properties
        
    if owner is not None:
        data["owner"] = owner
    
    try:
        return await make_api_request("PATCH", f"/api/2.1/unity-catalog/connections/{name}", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to update connection: {str(e)}")
        raise


async def delete_connection(name: str) -> Dict[str, Any]:
    """Delete a connection from the Unity Catalog.
    
    Args:
        name: Name of the connection to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting connection: {name}")
    
    try:
        return await make_api_request("DELETE", f"/api/2.1/unity-catalog/connections/{name}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to delete connection: {str(e)}")
        raise 