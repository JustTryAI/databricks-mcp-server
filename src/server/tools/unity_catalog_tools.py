"""Tool implementations for Databricks Unity Catalog API endpoints.

This module provides modularized tools for interacting with the Databricks Unity Catalog API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.catalog import unity_catalog
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_unity_catalog_tools(server):
    """Register Unity Catalog tools with the MCP server."""
    
    # Catalog operations
    @server.tool(
        name="list_catalogs",
        description=generate_tool_description(
            unity_catalog.list_catalogs,
            "GET",
            "/api/2.1/unity-catalog/catalogs"
        ),
    )
    async def list_catalogs(params: Dict[str, Any]):
        logger.info("Listing catalogs")
        try:
            result = await unity_catalog.list_catalogs()
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "catalogs")
        except Exception as e:
            logger.error(f"Error listing catalogs: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="create_catalog",
        description=generate_tool_description(
            unity_catalog.create_catalog,
            "POST",
            "/api/2.1/unity-catalog/catalogs"
        ),
    )
    async def create_catalog(params: Dict[str, Any]):
        logger.info(f"Creating catalog with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
                
            name = params.get("name")
            comment = params.get("comment", "")
            properties = params.get("properties", {})
            provider = params.get("provider", "databricks")
            storage_root = params.get("storage_root")
            
            result = await unity_catalog.create_catalog(name, comment, properties, provider, storage_root)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "catalog")
        except Exception as e:
            logger.error(f"Error creating catalog: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_catalog",
        description=generate_tool_description(
            unity_catalog.get_catalog,
            "GET",
            "/api/2.1/unity-catalog/catalogs/{name}"
        ),
    )
    async def get_catalog(params: Dict[str, Any]):
        logger.info(f"Getting catalog with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
                
            name = params.get("name")
            
            result = await unity_catalog.get_catalog(name)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "catalog", params.get("name"))
        except Exception as e:
            logger.error(f"Error getting catalog: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_catalog",
        description=generate_tool_description(
            unity_catalog.update_catalog,
            "PATCH",
            "/api/2.1/unity-catalog/catalogs/{name}"
        ),
    )
    async def update_catalog(params: Dict[str, Any]):
        logger.info(f"Updating catalog with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
                
            name = params.get("name")
            new_name = params.get("new_name")
            comment = params.get("comment")
            properties = params.get("properties")
            
            result = await unity_catalog.update_catalog(name, new_name, comment, properties)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "catalog", params.get("name"))
        except Exception as e:
            logger.error(f"Error updating catalog: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_catalog",
        description=generate_tool_description(
            unity_catalog.delete_catalog,
            "DELETE",
            "/api/2.1/unity-catalog/catalogs/{name}"
        ),
    )
    async def delete_catalog(params: Dict[str, Any]):
        logger.info(f"Deleting catalog with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
                
            name = params.get("name")
            
            result = await unity_catalog.delete_catalog(name)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "catalog", params.get("name"))
        except Exception as e:
            logger.error(f"Error deleting catalog: {str(e)}")
            return handle_api_error(e)
    
    # Schema operations
    @server.tool(
        name="list_schemas",
        description=generate_tool_description(
            unity_catalog.list_schemas,
            "GET",
            "/api/2.1/unity-catalog/schemas"
        ),
    )
    async def list_schemas(params: Dict[str, Any]):
        logger.info(f"Listing schemas with params: {params}")
        try:
            # Validate required parameters
            if "catalog_name" not in params:
                return missing_param_error("catalog_name")
                
            catalog_name = params.get("catalog_name")
            
            result = await unity_catalog.list_schemas(catalog_name)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "schemas")
        except Exception as e:
            logger.error(f"Error listing schemas: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="create_schema",
        description=generate_tool_description(
            unity_catalog.create_schema,
            "POST",
            "/api/2.1/unity-catalog/schemas"
        ),
    )
    async def create_schema(params: Dict[str, Any]):
        logger.info(f"Creating schema with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
            if "catalog_name" not in params:
                return missing_param_error("catalog_name")
                
            name = params.get("name")
            catalog_name = params.get("catalog_name")
            comment = params.get("comment", "")
            properties = params.get("properties", {})
            
            result = await unity_catalog.create_schema(name, catalog_name, comment, properties)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "schema")
        except Exception as e:
            logger.error(f"Error creating schema: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_schema",
        description=generate_tool_description(
            unity_catalog.get_schema,
            "GET",
            "/api/2.1/unity-catalog/schemas/{full_name}"
        ),
    )
    async def get_schema(params: Dict[str, Any]):
        logger.info(f"Getting schema with params: {params}")
        try:
            # Validate required parameters
            if "full_name" not in params:
                return missing_param_error("full_name")
                
            full_name = params.get("full_name")
            
            result = await unity_catalog.get_schema(full_name)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "schema", params.get("full_name"))
        except Exception as e:
            logger.error(f"Error getting schema: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_schema",
        description=generate_tool_description(
            unity_catalog.update_schema,
            "PATCH",
            "/api/2.1/unity-catalog/schemas/{full_name}"
        ),
    )
    async def update_schema(params: Dict[str, Any]):
        logger.info(f"Updating schema with params: {params}")
        try:
            # Validate required parameters
            if "full_name" not in params:
                return missing_param_error("full_name")
                
            full_name = params.get("full_name")
            new_name = params.get("new_name")
            comment = params.get("comment")
            properties = params.get("properties")
            
            result = await unity_catalog.update_schema(full_name, new_name, comment, properties)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "schema", params.get("full_name"))
        except Exception as e:
            logger.error(f"Error updating schema: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_schema",
        description=generate_tool_description(
            unity_catalog.delete_schema,
            "DELETE",
            "/api/2.1/unity-catalog/schemas/{full_name}"
        ),
    )
    async def delete_schema(params: Dict[str, Any]):
        logger.info(f"Deleting schema with params: {params}")
        try:
            # Validate required parameters
            if "full_name" not in params:
                return missing_param_error("full_name")
                
            full_name = params.get("full_name")
            
            result = await unity_catalog.delete_schema(full_name)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "schema", params.get("full_name"))
        except Exception as e:
            logger.error(f"Error deleting schema: {str(e)}")
            return handle_api_error(e)
    
    # Table operations
    @server.tool(
        name="list_tables",
        description=generate_tool_description(
            unity_catalog.list_tables,
            "GET",
            "/api/2.1/unity-catalog/tables"
        ),
    )
    async def list_tables(params: Dict[str, Any]):
        logger.info(f"Listing tables with params: {params}")
        try:
            # Validate required parameters
            if "catalog_name" not in params:
                return missing_param_error("catalog_name")
            if "schema_name" not in params:
                return missing_param_error("schema_name")
                
            catalog_name = params.get("catalog_name")
            schema_name = params.get("schema_name")
            
            result = await unity_catalog.list_tables(catalog_name, schema_name)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "tables")
        except Exception as e:
            logger.error(f"Error listing tables: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_table",
        description=generate_tool_description(
            unity_catalog.get_table,
            "GET",
            "/api/2.1/unity-catalog/tables/{full_name}"
        ),
    )
    async def get_table(params: Dict[str, Any]):
        logger.info(f"Getting table with params: {params}")
        try:
            # Validate required parameters
            if "full_name" not in params:
                return missing_param_error("full_name")
                
            full_name = params.get("full_name")
            
            result = await unity_catalog.get_table(full_name)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "table", params.get("full_name"))
        except Exception as e:
            logger.error(f"Error getting table: {str(e)}")
            return handle_api_error(e) 