"""Tool implementations for Databricks Connections API endpoints.

This module provides modularized tools for interacting with the Databricks Connections API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.catalog import connections
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_connections_tools(server):
    """Register Connections tools with the MCP server."""
    
    @server.tool(
        name="create_connection",
        description=generate_tool_description(
            connections.create_connection,
            "POST",
            "/api/2.1/unity-catalog/connections"
        ),
    )
    async def create_connection(params: Dict[str, Any]):
        logger.info(f"Creating connection with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
            if "connection_type" not in params:
                return missing_param_error("connection_type")
                
            name = params.get("name")
            connection_type = params.get("connection_type")
            comment = params.get("comment", "")
            options = params.get("options", {})
            properties = params.get("properties", {})
            
            result = await connections.create_connection(
                name, connection_type, comment, options, properties
            )
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "connection")
        except Exception as e:
            logger.error(f"Error creating connection: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_connections",
        description=generate_tool_description(
            connections.list_connections,
            "GET",
            "/api/2.1/unity-catalog/connections"
        ),
    )
    async def list_connections(params: Dict[str, Any]):
        logger.info("Listing connections")
        try:
            max_results = params.get("max_results", 100)
            page_token = params.get("page_token")
            
            result = await connections.list_connections(max_results, page_token)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "connections")
        except Exception as e:
            logger.error(f"Error listing connections: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_connection",
        description=generate_tool_description(
            connections.get_connection,
            "GET",
            "/api/2.1/unity-catalog/connections/{name}"
        ),
    )
    async def get_connection(params: Dict[str, Any]):
        logger.info(f"Getting connection with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
                
            name = params.get("name")
            
            result = await connections.get_connection(name)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "connection", params.get("name"))
        except Exception as e:
            logger.error(f"Error getting connection: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_connection",
        description=generate_tool_description(
            connections.update_connection,
            "PATCH",
            "/api/2.1/unity-catalog/connections/{name}"
        ),
    )
    async def update_connection(params: Dict[str, Any]):
        logger.info(f"Updating connection with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
                
            name = params.get("name")
            updates = {k: v for k, v in params.items() if k != "name"}
            
            result = await connections.update_connection(name, updates)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "connection", params.get("name"))
        except Exception as e:
            logger.error(f"Error updating connection: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_connection",
        description=generate_tool_description(
            connections.delete_connection,
            "DELETE",
            "/api/2.1/unity-catalog/connections/{name}"
        ),
    )
    async def delete_connection(params: Dict[str, Any]):
        logger.info(f"Deleting connection with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
                
            name = params.get("name")
            
            result = await connections.delete_connection(name)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "connection", params.get("name"))
        except Exception as e:
            logger.error(f"Error deleting connection: {str(e)}")
            return handle_api_error(e) 