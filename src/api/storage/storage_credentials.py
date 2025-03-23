"""
API for managing Databricks Storage Credentials.

This module provides functions for managing storage credentials in Databricks Unity Catalog.
It is part of the storage API group that includes DBFS, Volumes, External Locations, 
and Storage Credentials.
"""

import json
from typing import Dict, List, Optional, Any

from src.core.utils import make_api_request


async def create_storage_credential(
    name: str, 
    credential_type: str, 
    comment: Optional[str] = None,
    aws_iam_role: Optional[Dict[str, str]] = None,
    azure_managed_identity: Optional[Dict[str, str]] = None,
    azure_service_principal: Optional[Dict[str, str]] = None,
    **kwargs
) -> Dict[str, Any]:
    """Create a new storage credential.
    
    Args:
        name: The credential name
        credential_type: The credential type, e.g. 'aws_iam_role', 'azure_managed_identity', 'azure_service_principal'
        comment: Optional comment
        aws_iam_role: AWS IAM role configuration, required if credential_type is 'aws_iam_role'
        azure_managed_identity: Azure managed identity configuration, required if credential_type is 'azure_managed_identity'
        azure_service_principal: Azure service principal configuration, required if credential_type is 'azure_service_principal'
        **kwargs: Additional credential configuration options
    
    Returns:
        Dict containing the created storage credential info
    
    Raises:
        DatabricksAPIError: If the API request fails
    """
    data = {
        "name": name,
        "comment": comment
    }
    
    if credential_type == "aws_iam_role" and aws_iam_role:
        data["aws_iam_role"] = aws_iam_role
    elif credential_type == "azure_managed_identity" and azure_managed_identity:
        data["azure_managed_identity"] = azure_managed_identity
    elif credential_type == "azure_service_principal" and azure_service_principal:
        data["azure_service_principal"] = azure_service_principal
        
    # Add any additional configuration options
    for key, value in kwargs.items():
        if value is not None:
            data[key] = value
            
    return await make_api_request(
        "POST", 
        "/api/2.1/unity-catalog/storage-credentials", 
        data
    )


async def list_storage_credentials() -> Dict[str, Any]:
    """List all storage credentials.
    
    Returns:
        Dict containing the list of storage credentials
    
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await make_api_request(
        "GET", 
        "/api/2.1/unity-catalog/storage-credentials"
    )


async def get_storage_credential(name: str) -> Dict[str, Any]:
    """Get a storage credential by name.
    
    Args:
        name: The credential name
    
    Returns:
        Dict containing the storage credential details
    
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await make_api_request(
        "GET", 
        f"/api/2.1/unity-catalog/storage-credentials/{name}"
    )


async def update_storage_credential(
    name: str, 
    updates: Dict[str, Any]
) -> Dict[str, Any]:
    """Update a storage credential.
    
    Args:
        name: The credential name
        updates: Dict containing the fields to update
    
    Returns:
        Dict containing the updated storage credential info
    
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await make_api_request(
        "PATCH", 
        f"/api/2.1/unity-catalog/storage-credentials/{name}", 
        updates
    )


async def delete_storage_credential(name: str) -> Dict[str, Any]:
    """Delete a storage credential.
    
    Args:
        name: The credential name
    
    Returns:
        Empty dict on success
    
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await make_api_request(
        "DELETE", 
        f"/api/2.1/unity-catalog/storage-credentials/{name}"
    ) 