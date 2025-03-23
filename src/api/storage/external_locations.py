"""
API for managing Databricks External Locations.

This module provides functions for managing external locations in Databricks Unity Catalog.
It is part of the storage API group that includes DBFS, Volumes, External Locations, 
and Storage Credentials.
"""

import logging
from typing import Dict, Any, Optional

from src.core.utils import make_api_request

logger = logging.getLogger(__name__)

async def create_external_location(
    name: str,
    url: str,
    credential_name: str,
    comment: Optional[str] = None,
    read_only: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Create an external location.
    
    Args:
        name: Name of the external location
        url: URL of the external location
        credential_name: Name of the storage credential to use
        comment: Optional comment
        read_only: Optional flag to indicate if the location is read-only
        
    Returns:
        Response containing the created external location
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating external location: {name}")
    
    data = {
        "name": name,
        "url": url,
        "credential_name": credential_name,
    }
    
    if comment is not None:
        data["comment"] = comment
    
    if read_only is not None:
        data["read_only"] = read_only
    
    return await make_api_request("POST", "/api/2.1/unity-catalog/external-locations", data)


def mcp_list_external_locations(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    List external locations via Unity Catalog API.
    
    Args:
        params: Parameters for listing external locations
        
    Returns:
        Response containing the list of external locations
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    page_token = params.get("page_token", None)
    max_results = params.get("max_results", None)
    include_browse = params.get("include_browse", None)
    
    request_params = {}
    if page_token:
        request_params["page_token"] = page_token
    if max_results:
        request_params["max_results"] = max_results
    if include_browse:
        request_params["include_browse"] = include_browse
    
    return make_api_request("GET", "/api/2.1/unity-catalog/external-locations", request_params)


async def list_external_locations_with_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    List external locations with detailed parameters.
    
    Args:
        params: Parameters for listing external locations
        
    Returns:
        Response containing the list of external locations
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await mcp_list_external_locations(params)


async def list_external_locations() -> Dict[str, Any]:
    """
    List all external locations.
    
    Returns:
        Response containing the list of external locations
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await list_external_locations_with_params({})


async def get_external_location(name: str) -> Dict[str, Any]:
    """
    Get details of an external location.
    
    Args:
        name: Name of the external location
        
    Returns:
        Response containing the external location details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting external location: {name}")
    return await make_api_request("GET", f"/api/2.1/unity-catalog/external-locations/{name}")


async def update_external_location(
    name: str,
    new_name: Optional[str] = None,
    url: Optional[str] = None,
    credential_name: Optional[str] = None,
    comment: Optional[str] = None,
    owner: Optional[str] = None,
    read_only: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Update an external location.
    
    Args:
        name: Current name of the external location
        new_name: Optional new name for the external location
        url: Optional new URL for the external location
        credential_name: Optional new credential name
        comment: Optional new comment
        owner: Optional new owner
        read_only: Optional new read_only flag
        
    Returns:
        Response containing the updated external location
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating external location: {name}")
    
    data = {}
    
    if new_name is not None:
        data["name"] = new_name
        
    if url is not None:
        data["url"] = url
        
    if credential_name is not None:
        data["credential_name"] = credential_name
        
    if comment is not None:
        data["comment"] = comment
        
    if owner is not None:
        data["owner"] = owner
        
    if read_only is not None:
        data["read_only"] = read_only
    
    if not data:
        logger.warning("No updates provided for external location")
        return await get_external_location(name)
    
    return await make_api_request("PATCH", f"/api/2.1/unity-catalog/external-locations/{name}", data)


async def delete_external_location(name: str) -> Dict[str, Any]:
    """
    Delete an external location.
    
    Args:
        name: Name of the external location to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting external location: {name}")
    return await make_api_request("DELETE", f"/api/2.1/unity-catalog/external-locations/{name}") 