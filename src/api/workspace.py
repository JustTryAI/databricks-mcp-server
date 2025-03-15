"""
API for managing Databricks workspace files.
"""

import logging
from typing import Any, Dict, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

async def list_files(path: str) -> Dict[str, Any]:
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


async def import_files(
    path: str,
    format: str,
    content: str,
    language: Optional[str] = None,
    overwrite: Optional[bool] = False
) -> Dict[str, Any]:
    """
    Import a file to the workspace.
    
    Args:
        path: Workspace path to import to
        format: Format of the file (SOURCE, HTML, JUPYTER, DBC, AUTO)
        content: Base64-encoded content of the file
        language: Optional language of the notebook
        overwrite: Optional flag to overwrite existing files
        
    Returns:
        Response indicating success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Importing file to workspace path: {path}")
    
    data = {
        "path": path,
        "format": format,
        "content": content,
        "overwrite": overwrite
    }
    
    if language:
        data["language"] = language
    
    return make_api_request("POST", "/api/2.0/workspace/import", data=data)


async def export_files(path: str, format: str) -> Dict[str, Any]:
    """
    Export a file from the workspace.
    
    Args:
        path: Workspace path to export from
        format: Format to export as (SOURCE, HTML, JUPYTER, DBC)
        
    Returns:
        Response containing the exported file content
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Exporting file from workspace path: {path}")
    return make_api_request("GET", "/api/2.0/workspace/export", params={"path": path, "format": format})


async def delete_files(path: str, recursive: Optional[bool] = False) -> Dict[str, Any]:
    """
    Delete a file or directory from the workspace.
    
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


async def get_file_status(path: str) -> Dict[str, Any]:
    """
    Get the status of a file or directory in the workspace.
    
    Args:
        path: Workspace path to get status for
        
    Returns:
        Response containing the file status
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting status for workspace path: {path}")
    return make_api_request("GET", "/api/2.0/workspace/get-status", params={"path": path})


async def mkdirs(path: str) -> Dict[str, Any]:
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