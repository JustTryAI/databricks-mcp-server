"""
API for managing Databricks workspace files and notebooks.

This module provides a unified interface for managing both workspace files and notebooks.
It is part of the workspace API group that handles workspace-related operations.
"""

import base64
import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

async def list_workspace(path: str) -> Dict[str, Any]:
    """
    List files and directories in a workspace path.
    
    Args:
        path: Workspace path to list files from
        
    Returns:
        Response containing the list of files and directories
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Listing files in workspace path: {path}")
    return make_api_request("GET", "/api/2.0/workspace/list", params={"path": path})


async def import_workspace(
    path: str,
    format: str,
    content: str,
    language: Optional[str] = None,
    overwrite: Optional[bool] = False
) -> Dict[str, Any]:
    """
    Import a file or notebook to the workspace.
    
    Args:
        path: Workspace path to import to
        format: Format of the file (SOURCE, HTML, JUPYTER, DBC, AUTO)
        content: Base64-encoded content of the file
        language: Optional language of the notebook (SCALA, PYTHON, SQL, R)
        overwrite: Optional flag to overwrite existing files
        
    Returns:
        Response indicating success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Importing file to workspace path: {path}")
    
    # Ensure content is base64 encoded
    if not is_base64(content):
        content = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    
    data = {
        "path": path,
        "format": format,
        "content": content,
        "overwrite": overwrite
    }
    
    if language:
        data["language"] = language
    
    return make_api_request("POST", "/api/2.0/workspace/import", data=data)


async def export_workspace(path: str, format: str = "SOURCE") -> Dict[str, Any]:
    """
    Export a file or notebook from the workspace.
    
    Args:
        path: Workspace path to export from
        format: Format to export as (SOURCE, HTML, JUPYTER, DBC)
        
    Returns:
        Response containing the exported file content
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Exporting file from workspace path: {path}")
    response = make_api_request("GET", "/api/2.0/workspace/export", params={"path": path, "format": format})
    
    # Optionally decode base64 content
    if "content" in response and format in ["SOURCE", "JUPYTER"]:
        try:
            response["decoded_content"] = base64.b64decode(response["content"]).decode("utf-8")
        except Exception as e:
            logger.warning(f"Failed to decode content: {str(e)}")
    
    return response


async def delete_workspace(path: str, recursive: Optional[bool] = False) -> Dict[str, Any]:
    """
    Delete a file, notebook, or directory from the workspace.
    
    Args:
        path: Workspace path to delete
        recursive: Optional flag to delete recursively
        
    Returns:
        Response indicating success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting file or directory at workspace path: {path}")
    return make_api_request("POST", "/api/2.0/workspace/delete", data={"path": path, "recursive": recursive})


async def get_workspace_status(path: str) -> Dict[str, Any]:
    """
    Get the status of a file, notebook, or directory in the workspace.
    
    Args:
        path: Workspace path to get status for
        
    Returns:
        Response containing the file status
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting status for workspace path: {path}")
    return make_api_request("GET", "/api/2.0/workspace/get-status", params={"path": path})


async def create_workspace_directory(path: str) -> Dict[str, Any]:
    """
    Create directories in the workspace.
    
    Args:
        path: Workspace path to create directories at
        
    Returns:
        Response indicating success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating directories at workspace path: {path}")
    return make_api_request("POST", "/api/2.0/workspace/mkdirs", data={"path": path})


def is_base64(content: str) -> bool:
    """
    Check if a string is already base64 encoded.
    
    Args:
        content: The string to check
        
    Returns:
        True if the string is base64 encoded, False otherwise
    """
    try:
        return base64.b64encode(base64.b64decode(content)) == content.encode('utf-8')
    except Exception:
        return False

# Aliases for backward compatibility
list_files = list_workspace
import_files = import_workspace
export_files = export_workspace
delete_files = delete_workspace
mkdirs = create_workspace_directory 