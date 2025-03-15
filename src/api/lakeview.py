"""
API for managing Databricks Lakeviews.
"""

import logging
from typing import Any, Dict, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

async def list_lakeviews() -> Dict[str, Any]:
    """
    List all Lakeviews.
    
    Returns:
        Response containing a list of Lakeviews
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing Lakeviews")
    return make_api_request("GET", "/api/2.0/lakeview/lakeviews")


async def create_lakeview(
    name: str,
    definition: Dict[str, Any],
    description: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create a new Lakeview.
    
    Args:
        name: Name for the Lakeview
        definition: Lakeview definition
        description: Optional description for the Lakeview
        
    Returns:
        Response containing the created Lakeview information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new Lakeview: {name}")
    
    data = {
        "name": name,
        "definition": definition
    }
    
    if description:
        data["description"] = description
    
    return make_api_request("POST", "/api/2.0/lakeview/lakeviews", data=data)


async def get_lakeview(id: str) -> Dict[str, Any]:
    """
    Get information about a specific Lakeview.
    
    Args:
        id: ID of the Lakeview to get
        
    Returns:
        Response containing the Lakeview information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting Lakeview with ID: {id}")
    return make_api_request("GET", f"/api/2.0/lakeview/lakeviews/{id}")


async def update_lakeview(
    id: str,
    name: Optional[str] = None,
    definition: Optional[Dict[str, Any]] = None,
    description: Optional[str] = None
) -> Dict[str, Any]:
    """
    Update a Lakeview.
    
    Args:
        id: ID of the Lakeview to update
        name: Optional new name for the Lakeview
        definition: Optional new definition for the Lakeview
        description: Optional new description for the Lakeview
        
    Returns:
        Response containing the updated Lakeview information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating Lakeview with ID: {id}")
    
    data = {}
    
    if name is not None:
        data["name"] = name
        
    if definition is not None:
        data["definition"] = definition
        
    if description is not None:
        data["description"] = description
    
    return make_api_request("PATCH", f"/api/2.0/lakeview/lakeviews/{id}", data=data)


async def delete_lakeview(id: str) -> Dict[str, Any]:
    """
    Delete a Lakeview.
    
    Args:
        id: ID of the Lakeview to delete
        
    Returns:
        Empty response if successful
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting Lakeview with ID: {id}")
    return make_api_request("DELETE", f"/api/2.0/lakeview/lakeviews/{id}") 