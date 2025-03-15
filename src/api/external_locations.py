"""
Databricks External Locations API

This module provides functions for managing external locations in Databricks Unity Catalog.
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
    Create a new external location in Unity Catalog.

    Args:
        name: The name of the external location.
        url: The URL of the external location.
        credential_name: The name of the storage credential to use.
        comment: Optional comment for the external location.
        read_only: Optional flag to indicate if the external location is read-only.

    Returns:
        Dict containing the created external location information.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Creating external location: {name}")
    
    data = {
        "name": name,
        "url": url,
        "credential_name": credential_name,
    }
    
    if comment:
        data["comment"] = comment
    
    if read_only is not None:
        data["read_only"] = read_only
    
    return await make_api_request("POST", "/api/2.1/unity-catalog/external-locations", data=data)

async def list_external_locations(max_results: Optional[int] = None) -> Dict[str, Any]:
    """
    List external locations in Unity Catalog.

    Args:
        max_results: Optional maximum number of results to return.

    Returns:
        Dict containing the list of external locations.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info("Listing external locations")
    
    params = {}
    if max_results:
        params["max_results"] = max_results
    
    return await make_api_request("GET", "/api/2.1/unity-catalog/external-locations", params=params)

async def get_external_location(name: str) -> Dict[str, Any]:
    """
    Get details of a specific external location.

    Args:
        name: The name of the external location.

    Returns:
        Dict containing the external location details.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Getting external location details: {name}")
    
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
    Update an existing external location.

    Args:
        name: The name of the external location to update.
        new_name: Optional new name for the external location.
        url: Optional new URL for the external location.
        credential_name: Optional new credential name for the external location.
        comment: Optional new comment for the external location.
        owner: Optional new owner for the external location.
        read_only: Optional new read-only flag for the external location.

    Returns:
        Dict containing the updated external location information.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Updating external location: {name}")
    
    data = {}
    
    if new_name:
        data["name"] = new_name
    
    if url:
        data["url"] = url
    
    if credential_name:
        data["credential_name"] = credential_name
    
    if comment:
        data["comment"] = comment
    
    if owner:
        data["owner"] = owner
    
    if read_only is not None:
        data["read_only"] = read_only
    
    return await make_api_request("PATCH", f"/api/2.1/unity-catalog/external-locations/{name}", data=data)

async def delete_external_location(name: str) -> Dict[str, Any]:
    """
    Delete an external location.

    Args:
        name: The name of the external location to delete.

    Returns:
        Dict containing the response from the API.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Deleting external location: {name}")
    
    return await make_api_request("DELETE", f"/api/2.1/unity-catalog/external-locations/{name}") 