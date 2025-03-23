"""
API for managing Databricks Unity Catalog Volumes.

This module provides functions for interacting with the Databricks Unity Catalog Volumes API.
It is part of the storage API group that includes DBFS, Volumes, External Locations, 
and Storage Credentials.
"""

import json
from typing import Dict, List, Optional, Any

from src.core.utils import make_api_request


async def create_volume(
    catalog_name: str,
    schema_name: str,
    name: str,
    volume_type: str,
    storage_location: Optional[str] = None,
    comment: Optional[str] = None
) -> Dict[str, Any]:
    """Create a new Unity Catalog volume.

    Args:
        catalog_name: Name of the catalog where the volume will be created
        schema_name: Name of the schema where the volume will be created
        name: Name of the volume to create
        volume_type: Type of the volume (MANAGED or EXTERNAL)
        storage_location: Required for EXTERNAL volumes, specifies the location
        comment: Optional comment about the volume

    Returns:
        Dict containing information about the created volume
    """
    data = {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "name": name,
        "volume_type": volume_type
    }
    
    if storage_location is not None:
        data["storage_location"] = storage_location
    if comment is not None:
        data["comment"] = comment

    response = await make_api_request("POST", "/api/2.1/unity-catalog/volumes", data)
    return response


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
        Dict containing list of volumes and pagination information
    """
    params = {
        "catalog_name": catalog_name,
        "schema_name": schema_name
    }
    
    if max_results is not None:
        params["max_results"] = max_results
    if page_token is not None:
        params["page_token"] = page_token

    response = await make_api_request("GET", "/api/2.1/unity-catalog/volumes", params)
    return response


async def get_volume(
    catalog_name: str,
    schema_name: str,
    name: str
) -> Dict[str, Any]:
    """Get details of a specific volume.

    Args:
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        name: Name of the volume

    Returns:
        Dict containing volume information
    """
    path = f"/api/2.1/unity-catalog/volumes/{catalog_name}.{schema_name}.{name}"
    response = await make_api_request("GET", path)
    return response


async def update_volume(
    catalog_name: str,
    schema_name: str,
    name: str,
    new_name: Optional[str] = None,
    comment: Optional[str] = None,
    owner: Optional[str] = None
) -> Dict[str, Any]:
    """Update a Unity Catalog volume.

    Args:
        catalog_name: Name of the catalog 
        schema_name: Name of the schema
        name: Current name of the volume
        new_name: Optional new name for the volume
        comment: Optional updated comment
        owner: Optional new owner for the volume

    Returns:
        Dict containing updated volume information
    """
    path = f"/api/2.1/unity-catalog/volumes/{catalog_name}.{schema_name}.{name}"
    data = {}
    
    if new_name is not None:
        data["name"] = new_name
    if comment is not None:
        data["comment"] = comment
    if owner is not None:
        data["owner"] = owner

    response = await make_api_request("PATCH", path, data)
    return response


async def delete_volume(
    catalog_name: str,
    schema_name: str,
    name: str
) -> None:
    """Delete a Unity Catalog volume.

    Args:
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        name: Name of the volume to delete

    Returns:
        None
    """
    path = f"/api/2.1/unity-catalog/volumes/{catalog_name}.{schema_name}.{name}"
    await make_api_request("DELETE", path) 