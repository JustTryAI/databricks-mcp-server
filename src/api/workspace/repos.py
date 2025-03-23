"""
API for managing Databricks Git repositories.

This module provides functions for interacting with the Databricks Repos API.
It is part of the workspace API group that handles workspace-related operations.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def create_repo(
    url: str,
    provider: str,
    path: str,
    sparse_checkout: Optional[Dict[str, Any]] = None,
    git_branch: Optional[str] = None,
    tag: Optional[str] = None,
    host: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a new repository.
    
    Args:
        url: URL of the Git repository
        provider: Git provider (e.g., "github", "gitlab", "bitbucket", "azureDevOpsServices")
        path: Path where the repository will be stored in the workspace
        sparse_checkout: Optional sparse checkout configuration
        git_branch: Optional branch to checkout
        tag: Optional tag to checkout
        host: Optional host for on-premise Git providers
        
    Returns:
        Response containing the created repository details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating repository from {url} at path {path}")
    
    data = {
        "url": url,
        "provider": provider,
        "path": path,
    }
    
    if sparse_checkout is not None:
        data["sparse_checkout"] = sparse_checkout
    
    if git_branch is not None:
        data["branch"] = git_branch
    
    if tag is not None:
        data["tag"] = tag
    
    if host is not None:
        data["host"] = host
    
    try:
        return await make_api_request("POST", "/api/2.0/repos", data=data)
    except Exception as e:
        logger.error(f"Failed to create repository: {str(e)}")
        raise DatabricksAPIError(f"Failed to create repository: {str(e)}")


async def get_repo(repo_id: str) -> Dict[str, Any]:
    """
    Get repository details.
    
    Args:
        repo_id: ID of the repository
        
    Returns:
        Response containing the repository details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting repository: {repo_id}")
    
    try:
        return await make_api_request("GET", f"/api/2.0/repos/{repo_id}")
    except Exception as e:
        logger.error(f"Failed to get repository: {str(e)}")
        raise DatabricksAPIError(f"Failed to get repository: {str(e)}")


async def list_repos() -> Dict[str, Any]:
    """
    List all repositories.
    
    Returns:
        Response containing a list of repositories
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing repositories")
    
    try:
        return await make_api_request("GET", "/api/2.0/repos")
    except Exception as e:
        logger.error(f"Failed to list repositories: {str(e)}")
        raise DatabricksAPIError(f"Failed to list repositories: {str(e)}")


async def update_repo(
    repo_id: str,
    branch: Optional[str] = None,
    tag: Optional[str] = None,
    sparse_checkout: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Update a repository.
    
    Args:
        repo_id: ID of the repository to update
        branch: Optional branch to checkout
        tag: Optional tag to checkout
        sparse_checkout: Optional sparse checkout configuration
        
    Returns:
        Response containing the updated repository details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating repository: {repo_id}")
    
    data = {}
    
    if branch is not None:
        data["branch"] = branch
    
    if tag is not None:
        data["tag"] = tag
    
    if sparse_checkout is not None:
        data["sparse_checkout"] = sparse_checkout
    
    if not data:
        logger.warning("No updates provided for repository update operation")
        return {"message": "No updates provided"}
    
    try:
        return await make_api_request("PATCH", f"/api/2.0/repos/{repo_id}", data=data)
    except Exception as e:
        logger.error(f"Failed to update repository: {str(e)}")
        raise DatabricksAPIError(f"Failed to update repository: {str(e)}")


async def delete_repo(repo_id: str) -> Dict[str, Any]:
    """
    Delete a repository.
    
    Args:
        repo_id: ID of the repository to delete
        
    Returns:
        Empty response if successful
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting repository: {repo_id}")
    
    try:
        return await make_api_request("DELETE", f"/api/2.0/repos/{repo_id}")
    except Exception as e:
        logger.error(f"Failed to delete repository: {str(e)}")
        raise DatabricksAPIError(f"Failed to delete repository: {str(e)}")


async def get_repo_permissions(repo_id: str) -> Dict[str, Any]:
    """
    Get permissions for a repository.
    
    Args:
        repo_id: ID of the repository
        
    Returns:
        Response containing the permissions details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting permissions for repository: {repo_id}")
    
    try:
        return await make_api_request("GET", f"/api/2.0/repos/{repo_id}/permissions")
    except Exception as e:
        logger.error(f"Failed to get repository permissions: {str(e)}")
        raise DatabricksAPIError(f"Failed to get repository permissions: {str(e)}")


async def update_repo_permissions(
    repo_id: str,
    access_control_list: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Update permissions for a repository.
    
    Args:
        repo_id: ID of the repository
        access_control_list: List of access control items
        
    Returns:
        Response containing the updated permissions details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating permissions for repository: {repo_id}")
    
    data = {
        "access_control_list": access_control_list
    }
    
    try:
        return await make_api_request("PATCH", f"/api/2.0/repos/{repo_id}/permissions", data=data)
    except Exception as e:
        logger.error(f"Failed to update repository permissions: {str(e)}")
        raise DatabricksAPIError(f"Failed to update repository permissions: {str(e)}") 