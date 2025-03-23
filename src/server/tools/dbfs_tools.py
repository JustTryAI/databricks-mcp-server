"""Tool implementations for Databricks File System (DBFS) API endpoints.

This module provides modularized tools for interacting with the Databricks File System API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.storage import dbfs
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_dbfs_tools(server):
    """Register DBFS tools with the MCP server."""
    
    @server.tool(
        name="list_files",
        description=generate_tool_description(
            dbfs.list_files,
            "GET",
            "/api/2.0/dbfs/list"
        ),
    )
    async def list_files(params: Dict[str, Any]):
        logger.info(f"Listing files with params: {params}")
        try:
            result = await dbfs.list_files(params.get("dbfs_path"))
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "files")
        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")
            return handle_api_error(e)

    @server.tool(
        name="import_file",
        description=generate_tool_description(
            dbfs.import_file,
            "POST",
            "/api/2.0/dbfs/put"
        ),
    )
    async def import_file(params: Dict[str, Any]):
        logger.info(f"Importing file with params: {params}")
        try:
            # Validate required parameters
            if "source_path" not in params:
                return missing_param_error("source_path")
            if "target_path" not in params:
                return missing_param_error("target_path")
                
            source_path = params.get("source_path")
            target_path = params.get("target_path")
            overwrite = params.get("overwrite", False)
            
            result = await dbfs.import_file(source_path, target_path, overwrite)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "file")
        except Exception as e:
            logger.error(f"Error importing file: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="create_directory",
        description=generate_tool_description(
            dbfs.create_directory,
            "POST",
            "/api/2.0/dbfs/mkdirs"
        ),
    )
    async def create_directory(params: Dict[str, Any]):
        logger.info(f"Creating directory with params: {params}")
        try:
            # Validate required parameters
            if "path" not in params:
                return missing_param_error("path")
                
            path = params.get("path")
            
            result = await dbfs.create_directory(path)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "directory")
        except Exception as e:
            logger.error(f"Error creating directory: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_dbfs_file",
        description=generate_tool_description(
            dbfs.delete_file,
            "POST",
            "/api/2.0/dbfs/delete"
        ),
    )
    async def delete_dbfs_file(params: Dict[str, Any]):
        logger.info(f"Deleting DBFS file/directory with params: {params}")
        try:
            # Validate required parameters
            if "path" not in params:
                return missing_param_error("path")
                
            path = params.get("path")
            recursive = params.get("recursive", False)
            
            # First check if the file exists by trying to get its status
            try:
                await dbfs.get_status(path)
            except DatabricksAPIError as e:
                # If file doesn't exist, return a specific error message
                if e.status_code == 404 or "RESOURCE_DOES_NOT_EXIST" in str(e):
                    logger.warning(f"File or directory {path} does not exist or was already deleted")
                    return handle_api_error(e, "file", path)
                # Re-raise any other API errors
                raise
            
            # If we get here, the file exists, so we can try to delete it
            result = await dbfs.delete_file(path, recursive)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "file", params.get("path"))
        except Exception as e:
            logger.error(f"Error deleting DBFS file: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="read_dbfs_file",
        description=generate_tool_description(
            dbfs.read_file,
            "GET",
            "/api/2.0/dbfs/read"
        ),
    )
    async def read_dbfs_file(params: Dict[str, Any]):
        logger.info(f"Reading DBFS file with params: {params}")
        try:
            # Validate required parameters
            if "path" not in params:
                return missing_param_error("path")
                
            path = params.get("path")
            offset = params.get("offset", 0)
            length = params.get("length", 1024 * 1024)  # Default to 1MB
            
            result = await dbfs.read_file(path, offset, length)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "file", params.get("path"))
        except Exception as e:
            logger.error(f"Error reading DBFS file: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="move_dbfs_file",
        description=generate_tool_description(
            dbfs.move_file,
            "POST",
            "/api/2.0/dbfs/move"
        ),
    )
    async def move_dbfs_file(params: Dict[str, Any]):
        logger.info(f"Moving DBFS file with params: {params}")
        try:
            # Validate required parameters
            if "source_path" not in params:
                return missing_param_error("source_path")
            if "target_path" not in params:
                return missing_param_error("target_path")
                
            source_path = params.get("source_path")
            target_path = params.get("target_path")
            
            result = await dbfs.move_file(source_path, target_path)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "file", params.get("source_path"))
        except Exception as e:
            logger.error(f"Error moving DBFS file: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_dbfs_file_status",
        description=generate_tool_description(
            dbfs.get_status,
            "GET",
            "/api/2.0/dbfs/get-status"
        ),
    )
    async def get_dbfs_file_status(params: Dict[str, Any]):
        logger.info(f"Getting DBFS file status with params: {params}")
        try:
            # Validate required parameters
            if "path" not in params:
                return missing_param_error("path")
                
            path = params.get("path")
            
            result = await dbfs.get_status(path)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "file", params.get("path"))
        except Exception as e:
            logger.error(f"Error getting DBFS file status: {str(e)}")
            return handle_api_error(e) 