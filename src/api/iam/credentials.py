"""
API for managing Databricks Credentials.

This module provides functions for managing credentials in Databricks.
It is part of the IAM API group that includes Tokens, Secrets,
Credentials, and Service Principals.
"""

import json
from typing import Dict, List, Optional, Any

from src.core.utils import make_api_request


async def list_credentials() -> Dict[str, Any]:
    """List credentials.
    
    Returns:
        Dict containing the list of credentials
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await make_api_request("GET", "/api/2.0/credentials")


async def create_credentials(
    name: str, 
    credential_type: str, 
    value: Dict[str, Any]
) -> Dict[str, Any]:
    """Create new credentials.
    
    Args:
        name: The name of the credentials
        credential_type: The type of credentials (e.g., "password", "oauth")
        value: The configuration value for the credential (depends on type)
        
    Returns:
        Dict containing the created credentials details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    data = {
        "name": name,
        "type": credential_type,
        "value": value
    }
    
    return await make_api_request("POST", "/api/2.0/credentials", data=data)


async def get_credentials(credentials_id: str) -> Dict[str, Any]:
    """Get credentials by ID.
    
    Args:
        credentials_id: The ID of the credentials
        
    Returns:
        Dict containing the credentials details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await make_api_request("GET", f"/api/2.0/credentials/{credentials_id}")


async def update_credentials(
    credentials_id: str, 
    updates: Dict[str, Any]
) -> Dict[str, Any]:
    """Update existing credentials.
    
    Args:
        credentials_id: The ID of the credentials to update
        updates: The fields to update
        
    Returns:
        Dict containing the updated credentials details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await make_api_request("PATCH", f"/api/2.0/credentials/{credentials_id}", data=updates)


async def delete_credentials(credentials_id: str) -> Dict[str, Any]:
    """Delete credentials.
    
    Args:
        credentials_id: The ID of the credentials to delete
        
    Returns:
        Empty dict on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await make_api_request("DELETE", f"/api/2.0/credentials/{credentials_id}") 