"""
API for managing Databricks notebooks.

DEPRECATED: This module is deprecated and will be removed in a future version.
Please use the workspace module instead, which now includes all notebook functionality.
"""

import warnings
from typing import Any, Dict, Optional

from src.api.workspace.workspace import (
    import_workspace,
    export_workspace,
    list_workspace,
    delete_workspace,
    create_workspace_directory,
    get_workspace_status,
    is_base64
)

# Display deprecation warning
warnings.warn(
    "The 'notebooks' module is deprecated and will be removed in a future version. "
    "Please use 'workspace' module instead.",
    DeprecationWarning,
    stacklevel=2
)

async def import_notebook(
    path: str,
    content: str,
    format: str = "SOURCE",
    language: Optional[str] = None,
    overwrite: bool = False,
) -> Dict[str, Any]:
    """
    Import a notebook into the workspace.
    
    DEPRECATED: Use workspace.import_workspace() instead.
    
    Args:
        path: The path where the notebook should be stored
        content: The content of the notebook (base64 encoded)
        format: The format of the notebook (SOURCE, HTML, JUPYTER, DBC)
        language: The language of the notebook (SCALA, PYTHON, SQL, R)
        overwrite: Whether to overwrite an existing notebook
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await import_workspace(path, format, content, language, overwrite)


async def export_notebook(
    path: str,
    format: str = "SOURCE",
) -> Dict[str, Any]:
    """
    Export a notebook from the workspace.
    
    DEPRECATED: Use workspace.export_workspace() instead.
    
    Args:
        path: The path of the notebook to export
        format: The format to export (SOURCE, HTML, JUPYTER, DBC)
        
    Returns:
        Response containing the notebook content
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await export_workspace(path, format)


async def list_notebooks(path: str) -> Dict[str, Any]:
    """
    List notebooks in a workspace directory.
    
    DEPRECATED: Use workspace.list_workspace() instead.
    
    Args:
        path: The path to list
        
    Returns:
        Response containing the directory listing
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await list_workspace(path)


async def delete_notebook(path: str, recursive: bool = False) -> Dict[str, Any]:
    """
    Delete a notebook or directory.
    
    DEPRECATED: Use workspace.delete_workspace() instead.
    
    Args:
        path: The path to delete
        recursive: Whether to recursively delete directories
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await delete_workspace(path, recursive)


async def create_directory(path: str) -> Dict[str, Any]:
    """
    Create a directory in the workspace.
    
    DEPRECATED: Use workspace.create_workspace_directory() instead.
    
    Args:
        path: The path to create
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await create_workspace_directory(path)


async def get_status(path: str) -> Dict[str, Any]:
    """
    Get status of a notebook or directory.
    
    DEPRECATED: Use workspace.get_workspace_status() instead.
    
    Args:
        path: The path to get status for
        
    Returns:
        Response containing the status
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await get_workspace_status(path)


async def mkdirs(path: str) -> Dict[str, Any]:
    """
    Create directories for the notebook.
    
    DEPRECATED: Use workspace.create_workspace_directory() instead.
    
    Args:
        path: The path to create
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await create_workspace_directory(path) 