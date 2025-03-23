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
    try:
        return await make_api_request("GET", "/api/2.0/lakeview/lakeviews")
    except Exception as e:
        logger.error(f"Failed to list Lakeviews: {str(e)}")
        raise DatabricksAPIError(f"Failed to list Lakeviews: {str(e)}")


async def create_lakeview(
    name: str,
    definition: Dict[str, Any],
    description: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create a new Lakeview.
    
    Args:
        name: Name of the Lakeview
        definition: Definition of the Lakeview (JSON structure)
        description: Optional description of the Lakeview
        
    Returns:
        Response containing the created Lakeview details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating Lakeview: {name}")
    
    data = {
        "name": name,
        "definition": definition,
    }
    
    if description:
        data["description"] = description
    
    try:
        return await make_api_request("POST", "/api/2.0/lakeview/lakeviews", data=data)
    except Exception as e:
        logger.error(f"Failed to create Lakeview: {str(e)}")
        raise DatabricksAPIError(f"Failed to create Lakeview: {str(e)}")


async def get_lakeview(id: str) -> Dict[str, Any]:
    """
    Get a Lakeview by ID.
    
    Args:
        id: ID of the Lakeview to retrieve
        
    Returns:
        Response containing the Lakeview details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting Lakeview: {id}")
    try:
        return await make_api_request("GET", f"/api/2.0/lakeview/lakeviews/{id}")
    except Exception as e:
        logger.error(f"Failed to get Lakeview: {str(e)}")
        raise DatabricksAPIError(f"Failed to get Lakeview: {str(e)}")


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
        name: New name for the Lakeview (optional)
        definition: Updated definition for the Lakeview (optional)
        description: Updated description for the Lakeview (optional)
        
    Returns:
        Response containing the updated Lakeview details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating Lakeview: {id}")
    
    data = {}
    if name:
        data["name"] = name
    if definition:
        data["definition"] = definition
    if description:
        data["description"] = description
        
    if not data:
        logger.warning("No updates provided for Lakeview update operation")
        return {"message": "No updates provided"}
    
    try:
        return await make_api_request("PATCH", f"/api/2.0/lakeview/lakeviews/{id}", data=data)
    except Exception as e:
        logger.error(f"Failed to update Lakeview: {str(e)}")
        raise DatabricksAPIError(f"Failed to update Lakeview: {str(e)}")


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
    logger.info(f"Deleting Lakeview: {id}")
    try:
        return await make_api_request("DELETE", f"/api/2.0/lakeview/lakeviews/{id}")
    except Exception as e:
        logger.error(f"Failed to delete Lakeview: {str(e)}")
        raise DatabricksAPIError(f"Failed to delete Lakeview: {str(e)}") 