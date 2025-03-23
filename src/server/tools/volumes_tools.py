"""Tool implementations for Databricks Volumes API endpoints.

This module provides modularized tools for interacting with the Databricks Volumes API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.storage import volumes
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_volumes_tools(server):
    """Register Volumes tools with the MCP server."""
    
    @server.tool(
        name="create_volume",
        description=generate_tool_description(
            volumes.create_volume,
            "POST",
            "/api/2.1/unity-catalog/volumes"
        ),
    )
    async def create_volume(params: Dict[str, Any]):
        logger.info(f"Creating volume with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
            if "catalog_name" not in params:
                return missing_param_error("catalog_name")
            if "schema_name" not in params:
                return missing_param_error("schema_name")
            if "volume_type" not in params:
                return missing_param_error("volume_type")
                
            name = params.get("name")
            catalog_name = params.get("catalog_name")
            schema_name = params.get("schema_name")
            volume_type = params.get("volume_type")
            comment = params.get("comment", "")
            storage_location = params.get("storage_location")
            
            result = await volumes.create_volume(
                name, catalog_name, schema_name, volume_type, comment, storage_location
            )
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "volume")
        except Exception as e:
            logger.error(f"Error creating volume: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_volumes",
        description=generate_tool_description(
            volumes.list_volumes,
            "GET",
            "/api/2.1/unity-catalog/volumes"
        ),
    )
    async def list_volumes(params: Dict[str, Any]):
        logger.info(f"Listing volumes with params: {params}")
        try:
            # Validate required parameters
            if "catalog_name" not in params:
                return missing_param_error("catalog_name")
            if "schema_name" not in params:
                return missing_param_error("schema_name")
                
            catalog_name = params.get("catalog_name")
            schema_name = params.get("schema_name")
            max_results = params.get("max_results", 100)
            page_token = params.get("page_token")
            
            result = await volumes.list_volumes(catalog_name, schema_name, max_results, page_token)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "volumes")
        except Exception as e:
            logger.error(f"Error listing volumes: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_volume",
        description=generate_tool_description(
            volumes.get_volume,
            "GET",
            "/api/2.1/unity-catalog/volumes/{full_name}"
        ),
    )
    async def get_volume(params: Dict[str, Any]):
        logger.info(f"Getting volume with params: {params}")
        try:
            # Check for required parameters - either full_name or individual components
            if "full_name" not in params and ("name" not in params or "catalog_name" not in params or "schema_name" not in params):
                return missing_param_error("full_name or (name, catalog_name, and schema_name)")
            
            # Construct full_name if individual components are provided
            if "full_name" in params:
                full_name = params.get("full_name")
            else:
                catalog_name = params.get("catalog_name")
                schema_name = params.get("schema_name")
                name = params.get("name")
                full_name = f"{catalog_name}.{schema_name}.{name}"
            
            result = await volumes.get_volume(full_name)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "volume", params.get("full_name", ""))
        except Exception as e:
            logger.error(f"Error getting volume: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_volume",
        description=generate_tool_description(
            volumes.update_volume,
            "PATCH",
            "/api/2.1/unity-catalog/volumes/{full_name}"
        ),
    )
    async def update_volume(params: Dict[str, Any]):
        logger.info(f"Updating volume with params: {params}")
        try:
            # Check for required parameters - either full_name or individual components
            if "full_name" not in params and ("name" not in params or "catalog_name" not in params or "schema_name" not in params):
                return missing_param_error("full_name or (name, catalog_name, and schema_name)")
            
            # Construct full_name if individual components are provided
            if "full_name" in params:
                full_name = params.get("full_name")
            else:
                catalog_name = params.get("catalog_name")
                schema_name = params.get("schema_name")
                name = params.get("name")
                full_name = f"{catalog_name}.{schema_name}.{name}"
            
            # Build the updates dictionary, excluding the name components
            updates = {k: v for k, v in params.items() 
                      if k not in ["full_name", "name", "catalog_name", "schema_name"]}
            
            result = await volumes.update_volume(full_name, updates)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "volume", params.get("full_name", ""))
        except Exception as e:
            logger.error(f"Error updating volume: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_volume",
        description=generate_tool_description(
            volumes.delete_volume,
            "DELETE",
            "/api/2.1/unity-catalog/volumes/{full_name}"
        ),
    )
    async def delete_volume(params: Dict[str, Any]):
        logger.info(f"Deleting volume with params: {params}")
        try:
            # Check for required parameters - either full_name or individual components
            if "full_name" not in params and ("name" not in params or "catalog_name" not in params or "schema_name" not in params):
                return missing_param_error("full_name or (name, catalog_name, and schema_name)")
            
            # Construct full_name if individual components are provided
            if "full_name" in params:
                full_name = params.get("full_name")
            else:
                catalog_name = params.get("catalog_name")
                schema_name = params.get("schema_name")
                name = params.get("name")
                full_name = f"{catalog_name}.{schema_name}.{name}"
            
            result = await volumes.delete_volume(full_name)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "volume", params.get("full_name", ""))
        except Exception as e:
            logger.error(f"Error deleting volume: {str(e)}")
            return handle_api_error(e) 