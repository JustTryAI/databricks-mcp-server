"""
API for managing Databricks Secrets.

This module provides functions for managing secrets in Databricks.
It is part of the IAM API group that includes Tokens, Secrets, 
Credentials, and Service Principals.
"""

import logging
import base64
import json
from typing import Dict, List, Optional, Any

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

async def create_scope(
    scope: str,
    initial_manage_principal: Optional[str] = None
) -> Dict[str, Any]:
    """Create a new secret scope.

    Args:
        scope: Name of the scope to create.
        initial_manage_principal: The principal that can manage the created scope.

    Returns:
        Dict[str, Any]: Response from the create scope API.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Creating secret scope: {scope}")
    
    data = {"scope": scope}
    if initial_manage_principal:
        data["initial_manage_principal"] = initial_manage_principal
    
    try:
        response = await make_api_request("POST", "/api/2.0/secrets/scopes/create", data)
        return {"message": f"Secret scope '{scope}' created successfully"}
    except Exception as e:
        logger.error(f"Error creating secret scope '{scope}': {str(e)}")
        raise DatabricksAPIError(f"Failed to create secret scope '{scope}': {str(e)}")

async def list_scopes() -> Dict[str, Any]:
    """List all secret scopes.

    Returns:
        Dict[str, Any]: Response containing the list of secret scopes.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info("Listing all secret scopes")
    try:
        response = await make_api_request("GET", "/api/2.0/secrets/scopes/list")
        return response
    except Exception as e:
        logger.error(f"Error listing secret scopes: {str(e)}")
        raise DatabricksAPIError(f"Failed to list secret scopes: {str(e)}")

async def delete_scope(scope: str) -> Dict[str, Any]:
    """Delete a secret scope.

    Args:
        scope: Name of the scope to delete.

    Returns:
        Dict[str, Any]: Response from the delete scope API.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Deleting secret scope: {scope}")
    
    data = {"scope": scope}
    
    try:
        response = await make_api_request("POST", "/api/2.0/secrets/scopes/delete", data)
        return {"message": f"Secret scope '{scope}' deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting secret scope '{scope}': {str(e)}")
        raise DatabricksAPIError(f"Failed to delete secret scope '{scope}': {str(e)}")

async def put_secret(
    scope: str,
    key: str,
    string_value: Optional[str] = None,
    bytes_value: Optional[bytes] = None
) -> Dict[str, Any]:
    """Store a secret in a scope.

    Args:
        scope: Name of the scope to store the secret in.
        key: Key of the secret.
        string_value: String value of the secret.
        bytes_value: Bytes value of the secret.

    Returns:
        Dict[str, Any]: Response from the put secret API.

    Raises:
        DatabricksAPIError: If the API request fails or neither value is provided.
    """
    logger.info(f"Putting secret '{key}' in scope '{scope}'")
    
    if not string_value and not bytes_value:
        raise DatabricksAPIError("Either string_value or bytes_value must be provided")
    
    data = {"scope": scope, "key": key}
    
    if string_value:
        data["string_value"] = string_value
    elif bytes_value:
        data["bytes_value"] = bytes_value
    
    try:
        response = await make_api_request("POST", "/api/2.0/secrets/put", data)
        return {"message": f"Secret '{key}' in scope '{scope}' stored successfully"}
    except Exception as e:
        logger.error(f"Error putting secret '{key}' in scope '{scope}': {str(e)}")
        raise DatabricksAPIError(f"Failed to put secret '{key}' in scope '{scope}': {str(e)}")

async def delete_secret(scope: str, key: str) -> Dict[str, Any]:
    """Delete a secret.

    Args:
        scope: Name of the scope containing the secret.
        key: Key of the secret to delete.

    Returns:
        Dict[str, Any]: Response from the delete secret API.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Deleting secret '{key}' from scope '{scope}'")
    
    data = {"scope": scope, "key": key}
    
    try:
        response = await make_api_request("POST", "/api/2.0/secrets/delete", data)
        return {"message": f"Secret '{key}' in scope '{scope}' deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting secret '{key}' from scope '{scope}': {str(e)}")
        raise DatabricksAPIError(f"Failed to delete secret '{key}' from scope '{scope}': {str(e)}")

async def list_secrets(scope: str) -> Dict[str, Any]:
    """List all secrets within a scope.

    Args:
        scope: Name of the scope to list secrets from.

    Returns:
        Dict[str, Any]: Response containing the list of secrets.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Listing secrets in scope: {scope}")
    
    try:
        response = await make_api_request("GET", f"/api/2.0/secrets/list?scope={scope}")
        return response
    except Exception as e:
        logger.error(f"Error listing secrets in scope '{scope}': {str(e)}")
        raise DatabricksAPIError(f"Failed to list secrets in scope '{scope}': {str(e)}")

async def get_secret(scope: str, key: str) -> Dict[str, Any]:
    """Get a secret value.

    Args:
        scope: Name of the scope containing the secret.
        key: Key of the secret to retrieve.

    Returns:
        Dict[str, Any]: Response containing the secret value.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Getting secret '{key}' from scope '{scope}'")
    
    try:
        response = await make_api_request("GET", f"/api/2.0/secrets/get?scope={scope}&key={key}")
        return response
    except Exception as e:
        logger.error(f"Error getting secret '{key}' from scope '{scope}': {str(e)}")
        raise DatabricksAPIError(f"Failed to get secret '{key}' from scope '{scope}': {str(e)}") 