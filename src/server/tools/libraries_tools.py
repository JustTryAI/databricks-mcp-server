"""Tool implementations for Databricks Libraries API endpoints.

This module provides modularized tools for interacting with the Databricks Libraries API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.compute import libraries
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_libraries_tools(server):
    """Register Libraries tools with the MCP server."""
    
    @server.tool(
        name="install_libraries",
        description=generate_tool_description(
            libraries.install_libraries,
            "POST",
            "/api/2.0/libraries/install"
        ),
    )
    async def install_libraries(params: Dict[str, Any]):
        logger.info(f"Installing libraries with params: {params}")
        try:
            # Validate required parameters
            if "cluster_id" not in params:
                return missing_param_error("cluster_id")
            if "libraries" not in params:
                return missing_param_error("libraries")
                
            cluster_id = params.get("cluster_id")
            libraries_list = params.get("libraries")
            
            result = await libraries.install_libraries(cluster_id, libraries_list)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "libraries installation")
        except Exception as e:
            logger.error(f"Error installing libraries: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="uninstall_libraries",
        description=generate_tool_description(
            libraries.uninstall_libraries,
            "POST",
            "/api/2.0/libraries/uninstall"
        ),
    )
    async def uninstall_libraries(params: Dict[str, Any]):
        logger.info(f"Uninstalling libraries with params: {params}")
        try:
            # Validate required parameters
            if "cluster_id" not in params:
                return missing_param_error("cluster_id")
            if "libraries" not in params:
                return missing_param_error("libraries")
                
            cluster_id = params.get("cluster_id")
            libraries_list = params.get("libraries")
            
            result = await libraries.uninstall_libraries(cluster_id, libraries_list)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "libraries uninstallation")
        except Exception as e:
            logger.error(f"Error uninstalling libraries: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_libraries",
        description=generate_tool_description(
            libraries.list_libraries,
            "GET",
            "/api/2.0/libraries/all-cluster-statuses"
        ),
    )
    async def list_libraries(params: Dict[str, Any]):
        logger.info("Listing libraries for all clusters")
        try:
            result = await libraries.list_libraries()
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "libraries")
        except Exception as e:
            logger.error(f"Error listing libraries: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_library_statuses",
        description=generate_tool_description(
            libraries.list_library_statuses,
            "GET",
            "/api/2.0/libraries/cluster-status"
        ),
    )
    async def list_library_statuses(params: Dict[str, Any]):
        logger.info(f"Listing library statuses with params: {params}")
        try:
            # Validate required parameters
            if "cluster_id" not in params:
                return missing_param_error("cluster_id")
                
            cluster_id = params.get("cluster_id")
            
            result = await libraries.list_library_statuses(cluster_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "library statuses")
        except Exception as e:
            logger.error(f"Error listing library statuses: {str(e)}")
            return handle_api_error(e) 