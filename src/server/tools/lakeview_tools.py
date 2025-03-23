"""Tool implementations for Databricks Lakeview API endpoints.

This module provides modularized tools for interacting with the Databricks Lakeview API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.misc import lakeview
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_lakeview_tools(server):
    """Register Lakeview tools with the MCP server."""
    
    @server.tool(
        name="create_lakeview",
        description=generate_tool_description(
            lakeview.create_lakeview,
            "POST",
            "/api/2.0/lakeview"
        ),
    )
    async def create_lakeview(params: Dict[str, Any]):
        logger.info(f"Creating lakeview with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
            if "catalog_name" not in params:
                return missing_param_error("catalog_name")
            if "schema_name" not in params:
                return missing_param_error("schema_name")
                
            name = params.get("name")
            catalog_name = params.get("catalog_name")
            schema_name = params.get("schema_name")
            source_id = params.get("source_id")
            definition = params.get("definition", {})
            comment = params.get("comment", "")
            
            result = await lakeview.create_lakeview(
                name, catalog_name, schema_name, source_id, definition, comment
            )
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "lakeview")
        except Exception as e:
            logger.error(f"Error creating lakeview: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_lakeviews",
        description=generate_tool_description(
            lakeview.list_lakeviews,
            "GET",
            "/api/2.0/lakeview"
        ),
    )
    async def list_lakeviews(params: Dict[str, Any]):
        logger.info(f"Listing lakeviews with params: {params}")
        try:
            catalog_name = params.get("catalog_name")
            schema_name = params.get("schema_name")
            max_results = params.get("max_results", 100)
            page_token = params.get("page_token")
            
            result = await lakeview.list_lakeviews(catalog_name, schema_name, max_results, page_token)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "lakeviews")
        except Exception as e:
            logger.error(f"Error listing lakeviews: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_lakeview",
        description=generate_tool_description(
            lakeview.get_lakeview,
            "GET",
            "/api/2.0/lakeview/{full_name}"
        ),
    )
    async def get_lakeview(params: Dict[str, Any]):
        logger.info(f"Getting lakeview with params: {params}")
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
            
            result = await lakeview.get_lakeview(full_name)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "lakeview", params.get("full_name", ""))
        except Exception as e:
            logger.error(f"Error getting lakeview: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_lakeview",
        description=generate_tool_description(
            lakeview.update_lakeview,
            "PATCH",
            "/api/2.0/lakeview/{full_name}"
        ),
    )
    async def update_lakeview(params: Dict[str, Any]):
        logger.info(f"Updating lakeview with params: {params}")
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
            
            result = await lakeview.update_lakeview(full_name, updates)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "lakeview", params.get("full_name", ""))
        except Exception as e:
            logger.error(f"Error updating lakeview: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_lakeview",
        description=generate_tool_description(
            lakeview.delete_lakeview,
            "DELETE",
            "/api/2.0/lakeview/{full_name}"
        ),
    )
    async def delete_lakeview(params: Dict[str, Any]):
        logger.info(f"Deleting lakeview with params: {params}")
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
            
            result = await lakeview.delete_lakeview(full_name)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "lakeview", params.get("full_name", ""))
        except Exception as e:
            logger.error(f"Error deleting lakeview: {str(e)}")
            return handle_api_error(e) 