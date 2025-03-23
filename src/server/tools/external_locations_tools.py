"""Tool implementations for Databricks External Locations API endpoints.

This module provides modularized tools for interacting with the Databricks External Locations API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.storage import external_locations
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_external_locations_tools(server):
    """Register External Locations tools with the MCP server."""
    
    @server.tool(
        name="create_external_location",
        description=generate_tool_description(
            external_locations.create_external_location,
            "POST",
            "/api/2.1/unity-catalog/external-locations"
        ),
    )
    async def create_external_location(params: Dict[str, Any]):
        logger.info(f"Creating external location with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
            if "url" not in params:
                return missing_param_error("url")
            if "credential_name" not in params:
                return missing_param_error("credential_name")
                
            name = params.get("name")
            url = params.get("url")
            credential_name = params.get("credential_name")
            comment = params.get("comment", "")
            skip_validation = params.get("skip_validation", False)
            read_only = params.get("read_only", False)
            
            result = await external_locations.create_external_location(
                name, url, credential_name, comment, skip_validation, read_only
            )
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "external location")
        except Exception as e:
            logger.error(f"Error creating external location: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_external_locations",
        description=generate_tool_description(
            external_locations.list_external_locations_with_params,
            "GET",
            "/api/2.1/unity-catalog/external-locations"
        ),
    )
    async def list_external_locations(params: Dict[str, Any]):
        logger.info("Listing external locations")
        try:
            result = await external_locations.list_external_locations_with_params(params)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "external locations")
        except Exception as e:
            logger.error(f"Error listing external locations: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_external_location",
        description=generate_tool_description(
            external_locations.get_external_location,
            "GET",
            "/api/2.1/unity-catalog/external-locations/{name}"
        ),
    )
    async def get_external_location(params: Dict[str, Any]):
        logger.info(f"Getting external location with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
                
            name = params.get("name")
            
            result = await external_locations.get_external_location(name)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "external location", params.get("name"))
        except Exception as e:
            logger.error(f"Error getting external location: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_external_location",
        description=generate_tool_description(
            external_locations.update_external_location,
            "PATCH",
            "/api/2.1/unity-catalog/external-locations/{name}"
        ),
    )
    async def update_external_location(params: Dict[str, Any]):
        logger.info(f"Updating external location with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
                
            name = params.get("name")
            updates = {k: v for k, v in params.items() if k != "name"}
            
            result = await external_locations.update_external_location(name, updates)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "external location", params.get("name"))
        except Exception as e:
            logger.error(f"Error updating external location: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_external_location",
        description=generate_tool_description(
            external_locations.delete_external_location,
            "DELETE",
            "/api/2.1/unity-catalog/external-locations/{name}"
        ),
    )
    async def delete_external_location(params: Dict[str, Any]):
        logger.info(f"Deleting external location with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
                
            name = params.get("name")
            
            result = await external_locations.delete_external_location(name)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "external location", params.get("name"))
        except Exception as e:
            logger.error(f"Error deleting external location: {str(e)}")
            return handle_api_error(e) 