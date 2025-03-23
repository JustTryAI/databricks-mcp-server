"""Tool implementations for Databricks Workspace API endpoints.

This module provides modularized tools for interacting with the Databricks Workspace API.
It now includes all functionality for managing both files and notebooks in the workspace.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api import workspace
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_workspace_tools(server):
    """Register Workspace tools with the MCP server."""
    
    @server.tool(
        name="list_workspace_items",
        description=generate_tool_description(
            workspace.list_workspace,
            "GET",
            "/api/2.0/workspace/list"
        ),
    )
    async def list_workspace_items(params: Dict[str, Any]):
        logger.info(f"Listing workspace items with params: {params}")
        try:
            # Validate required parameters
            if "path" not in params:
                return missing_param_error("path")
                
            path = params.get("path")
            
            result = await workspace.list_workspace(path)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "workspace", path)
        except Exception as e:
            logger.error(f"Error listing workspace items: {str(e)}")
            return handle_api_error(e)

    @server.tool(
        name="get_workspace_status",
        description=generate_tool_description(
            workspace.get_workspace_status,
            "GET",
            "/api/2.0/workspace/get-status"
        ),
    )
    async def get_workspace_status(params: Dict[str, Any]):
        logger.info(f"Getting workspace status with params: {params}")
        try:
            # Validate required parameters
            if "path" not in params:
                return missing_param_error("path")
                
            path = params.get("path")
            
            result = await workspace.get_workspace_status(path)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "workspace item", path)
        except Exception as e:
            logger.error(f"Error getting workspace status: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="create_workspace_directory",
        description=generate_tool_description(
            workspace.create_workspace_directory,
            "POST",
            "/api/2.0/workspace/mkdirs"
        ),
    )
    async def create_workspace_directory(params: Dict[str, Any]):
        logger.info(f"Creating workspace directory with params: {params}")
        try:
            # Validate required parameters
            if "path" not in params:
                return missing_param_error("path")
                
            path = params.get("path")
            
            result = await workspace.create_workspace_directory(path)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "directory", path)
        except Exception as e:
            logger.error(f"Error creating workspace directory: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_workspace_item",
        description=generate_tool_description(
            workspace.delete_workspace,
            "POST",
            "/api/2.0/workspace/delete"
        ),
    )
    async def delete_workspace_item(params: Dict[str, Any]):
        logger.info(f"Deleting workspace item with params: {params}")
        try:
            # Validate required parameters
            if "path" not in params:
                return missing_param_error("path")
                
            path = params.get("path")
            recursive = params.get("recursive", False)
            
            result = await workspace.delete_workspace(path, recursive)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "workspace item", path)
        except Exception as e:
            logger.error(f"Error deleting workspace item: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="export_workspace_item",
        description=generate_tool_description(
            workspace.export_workspace,
            "GET",
            "/api/2.0/workspace/export"
        ),
    )
    async def export_workspace_item(params: Dict[str, Any]):
        logger.info(f"Exporting workspace item with params: {params}")
        try:
            # Validate required parameters
            if "path" not in params:
                return missing_param_error("path")
                
            path = params.get("path")
            format_type = params.get("format", "SOURCE")
            
            result = await workspace.export_workspace(path, format_type)
            
            # For large exports, truncate the content for readability
            if result and "content" in result and len(result["content"]) > 1000:
                preview = result["content"][:1000]
                result["content"] = f"{preview}... [content truncated, total length: {len(result['content'])} characters]"
                
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "workspace item", path)
        except Exception as e:
            logger.error(f"Error exporting workspace item: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="import_workspace_item",
        description=generate_tool_description(
            workspace.import_workspace,
            "POST",
            "/api/2.0/workspace/import"
        ),
    )
    async def import_workspace_item(params: Dict[str, Any]):
        logger.info(f"Importing workspace item with params: {params}")
        try:
            # Validate required parameters
            if "path" not in params:
                return missing_param_error("path")
            if "content" not in params:
                return missing_param_error("content")
            if "format" not in params:
                return missing_param_error("format")
                
            # Start with required parameters
            api_params = {
                "path": params.get("path"),
                "content": params.get("content"),
                "format": params.get("format"),
                "overwrite": params.get("overwrite", False)
            }

            # Add optional parameters only if they exist
            optional_params = ["language"]
            
            for param in optional_params:
                if param in params and params[param] is not None:
                    # For workspace.import_workspace, no name conversion needed
                    api_params[param] = params[param]
            
            result = await workspace.import_workspace(**api_params)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "workspace item", params.get("path"))
        except Exception as e:
            logger.error(f"Error importing workspace item: {str(e)}")
            return handle_api_error(e)
