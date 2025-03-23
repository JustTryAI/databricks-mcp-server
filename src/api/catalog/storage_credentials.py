"""
API for managing Databricks Unity Catalog Storage Credentials.

This module provides functions for managing storage credentials in Databricks Unity Catalog.
It is part of the Catalog API group.
"""

import logging
from typing import Any, Dict, List, Optional, Union

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

async def create_storage_credential(
    name: str,
    aws_credentials: Optional[Dict[str, Any]] = None,
    azure_service_principal: Optional[Dict[str, Any]] = None,
    azure_managed_identity: Optional[Dict[str, Any]] = None,
    gcp_service_account_key: Optional[Dict[str, Any]] = None,
    comment: Optional[str] = None,
    read_only: Optional[bool] = None,
) -> Dict[str, Any]:
    """Create a new storage credential in the Unity Catalog.
    
    Args:
        name: Name of the storage credential
        aws_credentials: Optional AWS credentials configuration
        azure_service_principal: Optional Azure service principal configuration
        azure_managed_identity: Optional Azure managed identity configuration
        gcp_service_account_key: Optional GCP service account key configuration
        comment: Optional comment for the storage credential
        read_only: Optional flag to create a read-only storage credential
        
    Returns:
        Response containing the created storage credential info
        
    Raises:
        DatabricksAPIError: If the API request fails
        ValueError: If no cloud credential type is provided
    """
    logger.info(f"Creating new storage credential: {name}")
    
    data = {"name": name}
    
    # Add cloud provider specific credentials - only one should be provided
    credential_count = sum(1 for cred in [
        aws_credentials, 
        azure_service_principal, 
        azure_managed_identity, 
        gcp_service_account_key
    ] if cred is not None)
    
    if credential_count == 0:
        raise ValueError("One cloud credential type must be provided")
    
    if credential_count > 1:
        raise ValueError("Only one cloud credential type can be provided")
    
    if aws_credentials is not None:
        data["aws_credentials"] = aws_credentials
        
    if azure_service_principal is not None:
        data["azure_service_principal"] = azure_service_principal
        
    if azure_managed_identity is not None:
        data["azure_managed_identity"] = azure_managed_identity
        
    if gcp_service_account_key is not None:
        data["gcp_service_account_key"] = gcp_service_account_key
    
    if comment is not None:
        data["comment"] = comment
        
    if read_only is not None:
        data["read_only"] = read_only
    
    try:
        return await make_api_request("POST", "/api/2.1/unity-catalog/storage-credentials", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to create storage credential: {str(e)}")
        raise


async def list_storage_credentials(max_results: Optional[int] = None) -> Dict[str, Any]:
    """List storage credentials in the Unity Catalog.
    
    Args:
        max_results: Optional maximum number of results to return
        
    Returns:
        Response containing the list of storage credentials
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing storage credentials")
    
    params = {}
    
    if max_results is not None:
        params["max_results"] = max_results
    
    try:
        return await make_api_request("GET", "/api/2.1/unity-catalog/storage-credentials", params=params)
    except DatabricksAPIError as e:
        logger.error(f"Failed to list storage credentials: {str(e)}")
        raise


async def get_storage_credential(name: str) -> Dict[str, Any]:
    """Get a specific storage credential from the Unity Catalog.
    
    Args:
        name: Name of the storage credential
        
    Returns:
        Response containing the storage credential info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting storage credential: {name}")
    
    try:
        return await make_api_request("GET", f"/api/2.1/unity-catalog/storage-credentials/{name}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to get storage credential: {str(e)}")
        raise


async def update_storage_credential(
    name: str,
    aws_credentials: Optional[Dict[str, Any]] = None,
    azure_service_principal: Optional[Dict[str, Any]] = None,
    azure_managed_identity: Optional[Dict[str, Any]] = None,
    gcp_service_account_key: Optional[Dict[str, Any]] = None,
    new_name: Optional[str] = None,
    comment: Optional[str] = None,
    owner: Optional[str] = None,
    read_only: Optional[bool] = None,
) -> Dict[str, Any]:
    """Update a storage credential in the Unity Catalog.
    
    Args:
        name: Name of the storage credential to update
        aws_credentials: Optional AWS credentials configuration
        azure_service_principal: Optional Azure service principal configuration
        azure_managed_identity: Optional Azure managed identity configuration
        gcp_service_account_key: Optional GCP service account key configuration
        new_name: Optional new name for the storage credential
        comment: Optional new comment for the storage credential
        owner: Optional new owner for the storage credential
        read_only: Optional flag to update the read-only status
        
    Returns:
        Response containing the updated storage credential info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating storage credential: {name}")
    
    data = {}
    
    # Add cloud provider specific credentials - only one should be provided
    credential_count = sum(1 for cred in [
        aws_credentials, 
        azure_service_principal, 
        azure_managed_identity, 
        gcp_service_account_key
    ] if cred is not None)
    
    if credential_count > 1:
        raise ValueError("Only one cloud credential type can be provided")
    
    if aws_credentials is not None:
        data["aws_credentials"] = aws_credentials
        
    if azure_service_principal is not None:
        data["azure_service_principal"] = azure_service_principal
        
    if azure_managed_identity is not None:
        data["azure_managed_identity"] = azure_managed_identity
        
    if gcp_service_account_key is not None:
        data["gcp_service_account_key"] = gcp_service_account_key
    
    if new_name is not None:
        data["name"] = new_name
        
    if comment is not None:
        data["comment"] = comment
        
    if owner is not None:
        data["owner"] = owner
        
    if read_only is not None:
        data["read_only"] = read_only
    
    try:
        return await make_api_request("PATCH", f"/api/2.1/unity-catalog/storage-credentials/{name}", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to update storage credential: {str(e)}")
        raise


async def delete_storage_credential(name: str) -> Dict[str, Any]:
    """Delete a storage credential from the Unity Catalog.
    
    Args:
        name: Name of the storage credential to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting storage credential: {name}")
    
    try:
        return await make_api_request("DELETE", f"/api/2.1/unity-catalog/storage-credentials/{name}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to delete storage credential: {str(e)}")
        raise 