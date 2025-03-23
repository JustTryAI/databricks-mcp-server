"""
API for managing Databricks Tokens.

This module provides functions for creating, listing, and revoking Databricks API tokens
and personal access tokens (PATs). It is part of the IAM API group that includes
Tokens, Secrets, Credentials, and Service Principals.
"""

import json
from typing import Dict, List, Optional, Any

from src.core.utils import make_api_request


async def create_token(
    comment: Optional[str] = None,
    lifetime_seconds: Optional[int] = None
) -> Dict[str, Any]:
    """Create a new Databricks API token.

    Args:
        comment: Optional comment about the token
        lifetime_seconds: Optional token lifetime in seconds. If not specified, the token remains
                          valid until it is revoked

    Returns:
        Dict containing the token value and other metadata

    Raises:
        DatabricksAPIError: If the API request fails
    """
    data = {}
    if comment is not None:
        data["comment"] = comment
    if lifetime_seconds is not None:
        data["lifetime_seconds"] = lifetime_seconds

    return await make_api_request("POST", "/api/2.0/token/create", data=data)


async def list_tokens() -> Dict[str, Any]:
    """List all Databricks API tokens.

    Returns:
        Dict containing a list of tokens

    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await make_api_request("GET", "/api/2.0/token/list")


async def revoke_token(token_id: str) -> None:
    """Revoke a Databricks API token.

    Args:
        token_id: ID of the token to revoke

    Returns:
        None

    Raises:
        DatabricksAPIError: If the API request fails
    """
    data = {"token_id": token_id}
    await make_api_request("POST", "/api/2.0/token/delete", data=data)


async def create_pat(
    comment: Optional[str] = None,
    lifetime_seconds: Optional[int] = None
) -> Dict[str, Any]:
    """Create a new personal access token (PAT).

    Args:
        comment: Optional comment about the token
        lifetime_seconds: Optional token lifetime in seconds. If not specified, the token remains
                          valid until it is revoked

    Returns:
        Dict containing the token value and other metadata

    Raises:
        DatabricksAPIError: If the API request fails
    """
    data = {}
    if comment is not None:
        data["comment"] = comment
    if lifetime_seconds is not None:
        data["lifetime_seconds"] = lifetime_seconds

    return await make_api_request("POST", "/api/2.0/token/create", data=data)


async def list_pats() -> Dict[str, Any]:
    """List all personal access tokens (PATs).

    Returns:
        Dict containing a list of tokens

    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await make_api_request("GET", "/api/2.0/token/list")


async def revoke_pat(token_id: str) -> None:
    """Revoke a personal access token (PAT).

    Args:
        token_id: ID of the token to revoke

    Returns:
        None

    Raises:
        DatabricksAPIError: If the API request fails
    """
    data = {"token_id": token_id}
    await make_api_request("POST", "/api/2.0/token/delete", data=data) 