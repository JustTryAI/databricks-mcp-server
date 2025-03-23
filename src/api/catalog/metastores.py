"""
API for managing Databricks Unity Catalog Metastores.

This module provides functions for managing metastores in Databricks Unity Catalog.
It is part of the Catalog API group.
"""

import logging
from typing import Any, Dict, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

async def create_metastore(
    name: str,
    storage_root: str,
    region: Optional[str] = None,
    comment: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a new metastore in the Unity Catalog.
    
    Args:
        name: Name of the metastore
        storage_root: Storage root URL (S3, ADLS, etc.)
        region: Optional cloud region where the metastore resides
        comment: Optional comment for the metastore
        
    Returns:
        Response containing the created metastore info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new metastore: {name}")
    
    data = {
        "name": name,
        "storage_root": storage_root
    }
    
    if region is not None:
        data["region"] = region
        
    if comment is not None:
        data["comment"] = comment
    
    try:
        return await make_api_request("POST", "/api/2.1/unity-catalog/metastores", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to create metastore: {str(e)}")
        raise


async def list_metastores() -> Dict[str, Any]:
    """List all metastores accessible by the caller.
    
    Returns:
        Response containing the list of metastores
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing metastores")
    
    try:
        return await make_api_request("GET", "/api/2.1/unity-catalog/metastores")
    except DatabricksAPIError as e:
        logger.error(f"Failed to list metastores: {str(e)}")
        raise


async def get_metastore(id: str) -> Dict[str, Any]:
    """Get a metastore by ID.
    
    Args:
        id: ID of the metastore
        
    Returns:
        Response containing the metastore info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting metastore: {id}")
    
    try:
        return await make_api_request("GET", f"/api/2.1/unity-catalog/metastores/{id}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to get metastore: {str(e)}")
        raise


async def update_metastore(
    id: str,
    new_name: Optional[str] = None,
    storage_root: Optional[str] = None,
    delta_sharing_scope: Optional[str] = None,
    delta_sharing_recipient_token_lifetime_in_seconds: Optional[int] = None,
    delta_sharing_organization_name: Optional[str] = None,
    owner: Optional[str] = None,
    privilege_model_version: Optional[str] = None,
    comment: Optional[str] = None,
) -> Dict[str, Any]:
    """Update a metastore.
    
    Args:
        id: ID of the metastore to update
        new_name: Optional new name for the metastore
        storage_root: Optional new storage root URL
        delta_sharing_scope: Optional Delta Sharing scope
        delta_sharing_recipient_token_lifetime_in_seconds: Optional token lifetime for Delta Sharing
        delta_sharing_organization_name: Optional organization name for Delta Sharing
        owner: Optional new owner for the metastore
        privilege_model_version: Optional privilege model version
        comment: Optional new comment for the metastore
        
    Returns:
        Response containing the updated metastore info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating metastore: {id}")
    
    data = {}
    
    if new_name is not None:
        data["name"] = new_name
        
    if storage_root is not None:
        data["storage_root"] = storage_root
        
    if delta_sharing_scope is not None:
        data["delta_sharing_scope"] = delta_sharing_scope
        
    if delta_sharing_recipient_token_lifetime_in_seconds is not None:
        data["delta_sharing_recipient_token_lifetime_in_seconds"] = delta_sharing_recipient_token_lifetime_in_seconds
        
    if delta_sharing_organization_name is not None:
        data["delta_sharing_organization_name"] = delta_sharing_organization_name
        
    if owner is not None:
        data["owner"] = owner
        
    if privilege_model_version is not None:
        data["privilege_model_version"] = privilege_model_version
        
    if comment is not None:
        data["comment"] = comment
    
    try:
        return await make_api_request("PATCH", f"/api/2.1/unity-catalog/metastores/{id}", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to update metastore: {str(e)}")
        raise


async def delete_metastore(id: str, force: Optional[bool] = None) -> Dict[str, Any]:
    """Delete a metastore.
    
    Args:
        id: ID of the metastore to delete
        force: Optional flag to force deletion even if the metastore contains data
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting metastore: {id}")
    
    params = {}
    if force is not None:
        params["force"] = str(force).lower()
    
    try:
        return await make_api_request("DELETE", f"/api/2.1/unity-catalog/metastores/{id}", params=params)
    except DatabricksAPIError as e:
        logger.error(f"Failed to delete metastore: {str(e)}")
        raise


async def assign_metastore(
    workspace_id: int,
    metastore_id: str,
    default_catalog_name: str
) -> Dict[str, Any]:
    """Assign a metastore to a workspace.
    
    Args:
        workspace_id: ID of the workspace to assign the metastore to
        metastore_id: ID of the metastore to assign
        default_catalog_name: Name of the default catalog to use
        
    Returns:
        Response indicating the result of the assignment
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Assigning metastore {metastore_id} to workspace {workspace_id}")
    
    data = {
        "metastore_id": metastore_id,
        "default_catalog_name": default_catalog_name
    }
    
    try:
        return await make_api_request(
            "PUT", 
            f"/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore", 
            data=data
        )
    except DatabricksAPIError as e:
        logger.error(f"Failed to assign metastore: {str(e)}")
        raise


async def unassign_metastore(workspace_id: int) -> Dict[str, Any]:
    """Unassign a metastore from a workspace.
    
    Args:
        workspace_id: ID of the workspace to unassign the metastore from
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Unassigning metastore from workspace {workspace_id}")
    
    try:
        return await make_api_request(
            "DELETE", 
            f"/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore"
        )
    except DatabricksAPIError as e:
        logger.error(f"Failed to unassign metastore: {str(e)}")
        raise 