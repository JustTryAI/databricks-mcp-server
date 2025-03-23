"""
API for managing Databricks Service Principals.

This module provides functions for managing Service Principals in Databricks.
It is part of the IAM API group that includes Tokens, Secrets,
Credentials, and Service Principals.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

async def create_service_principal(
    display_name: str,
    application_id: Optional[str] = None,
    entitlements: Optional[List[Dict[str, Any]]] = None,
    roles: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """Create a new service principal.
    
    Args:
        display_name: Display name for the service principal
        application_id: Optional application ID for the service principal
        entitlements: Optional list of entitlements for the service principal
        roles: Optional list of roles for the service principal
        
    Returns:
        Response containing the service principal information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new service principal: {display_name}")
    
    data = {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
        "displayName": display_name
    }
    
    if application_id:
        data["applicationId"] = application_id
    
    if entitlements:
        data["entitlements"] = entitlements
    
    if roles:
        data["roles"] = roles
    
    try:
        return await make_api_request("POST", "/api/2.0/preview/scim/v2/ServicePrincipals", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to create service principal: {str(e)}")
        raise


async def list_service_principals(
    filter: Optional[str] = None,
    count: Optional[int] = None,
    starting_index: Optional[int] = None
) -> Dict[str, Any]:
    """List service principals.
    
    Args:
        filter: Optional filter string for filtering service principals
        count: Optional maximum number of service principals to return
        starting_index: Optional starting index for pagination
        
    Returns:
        Response containing the list of service principals
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing service principals")
    
    params = {}
    
    if filter:
        params["filter"] = filter
    
    if count:
        params["count"] = count
    
    if starting_index:
        params["startIndex"] = starting_index
    
    try:
        return await make_api_request("GET", "/api/2.0/preview/scim/v2/ServicePrincipals", params=params)
    except DatabricksAPIError as e:
        logger.error(f"Failed to list service principals: {str(e)}")
        raise


async def get_service_principal(id: str) -> Dict[str, Any]:
    """Get a specific service principal.
    
    Args:
        id: ID of the service principal
        
    Returns:
        Response containing the service principal information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting service principal: {id}")
    
    try:
        return await make_api_request("GET", f"/api/2.0/preview/scim/v2/ServicePrincipals/{id}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to get service principal: {str(e)}")
        raise


async def update_service_principal(id: str, operations: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Update a service principal.
    
    Args:
        id: ID of the service principal to update
        operations: List of operations to perform
        
    Returns:
        Response containing the updated service principal information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating service principal: {id}")
    
    data = {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": operations
    }
    
    try:
        return await make_api_request("PATCH", f"/api/2.0/preview/scim/v2/ServicePrincipals/{id}", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to update service principal: {str(e)}")
        raise


async def delete_service_principal(id: str) -> Dict[str, Any]:
    """Delete a service principal.
    
    Args:
        id: ID of the service principal to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting service principal: {id}")
    
    try:
        return await make_api_request("DELETE", f"/api/2.0/preview/scim/v2/ServicePrincipals/{id}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to delete service principal: {str(e)}")
        raise 