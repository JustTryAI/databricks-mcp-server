"""Tool implementations for Databricks SQL API endpoints.

This module provides modularized tools for interacting with the Databricks SQL API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.sql import sql
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_sql_tools(server):
    """Register SQL tools with the MCP server."""
    
    @server.tool(
        name="execute_sql",
        description=generate_tool_description(
            sql.execute_sql,
            "POST",
            "/api/2.0/sql/statements/execute"
        ),
    )
    async def execute_sql(params: Dict[str, Any]):
        logger.info(f"Executing SQL query with params: {params}")
        try:
            # Validate required parameters
            if "statement" not in params:
                return missing_param_error("statement")
            if "warehouse_id" not in params:
                return missing_param_error("warehouse_id")
                
            statement = params.get("statement")
            warehouse_id = params.get("warehouse_id")
            catalog = params.get("catalog")
            schema = params.get("schema")
            
            result = await sql.execute_sql(statement, warehouse_id, catalog, schema)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "SQL query")
        except Exception as e:
            logger.error(f"Error executing SQL query: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_warehouses",
        description=generate_tool_description(
            sql.list_warehouses,
            "GET",
            "/api/2.0/sql/warehouses"
        ),
    )
    async def list_warehouses(params: Dict[str, Any]):
        logger.info("Listing SQL warehouses")
        try:
            result = await sql.list_warehouses()
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "warehouses")
        except Exception as e:
            logger.error(f"Error listing warehouses: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_warehouse",
        description=generate_tool_description(
            sql.get_warehouse,
            "GET",
            "/api/2.0/sql/warehouses/{id}"
        ),
    )
    async def get_warehouse(params: Dict[str, Any]):
        logger.info(f"Getting warehouse with params: {params}")
        try:
            # Validate required parameters
            if "id" not in params:
                return missing_param_error("id")
                
            warehouse_id = params.get("id")
            
            result = await sql.get_warehouse(warehouse_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "warehouse", warehouse_id)
        except Exception as e:
            logger.error(f"Error getting warehouse: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="create_warehouse",
        description=generate_tool_description(
            sql.create_warehouse,
            "POST",
            "/api/2.0/sql/warehouses"
        ),
    )
    async def create_warehouse(params: Dict[str, Any]):
        logger.info(f"Creating warehouse with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
            if "cluster_size" not in params:
                return missing_param_error("cluster_size")
                
            name = params.get("name")
            cluster_size = params.get("cluster_size")
            
            # Optional parameters with defaults
            auto_stop_mins = params.get("auto_stop_mins", 120)
            max_num_clusters = params.get("max_num_clusters", 1)
            
            # Other optional parameters
            enable_photon = params.get("enable_photon")
            spot_instance_policy = params.get("spot_instance_policy")
            tags = params.get("tags")
            warehouse_type = params.get("warehouse_type")
            
            result = await sql.create_warehouse(
                name=name,
                cluster_size=cluster_size,
                auto_stop_mins=auto_stop_mins,
                max_num_clusters=max_num_clusters,
                enable_photon=enable_photon,
                spot_instance_policy=spot_instance_policy,
                tags=tags,
                warehouse_type=warehouse_type
            )
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "warehouse creation")
        except Exception as e:
            logger.error(f"Error creating warehouse: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_warehouse",
        description=generate_tool_description(
            sql.update_warehouse,
            "POST",
            "/api/2.0/sql/warehouses/{id}/edit"
        ),
    )
    async def update_warehouse(params: Dict[str, Any]):
        logger.info(f"Updating warehouse with params: {params}")
        try:
            # Validate required parameters
            if "id" not in params:
                return missing_param_error("id")
                
            warehouse_id = params.get("id")
            
            # Remove id from params to create update_config
            update_params = params.copy()
            update_params.pop("id")
            
            result = await sql.update_warehouse(warehouse_id, **update_params)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "warehouse update", warehouse_id)
        except Exception as e:
            logger.error(f"Error updating warehouse: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_warehouse",
        description=generate_tool_description(
            sql.delete_warehouse,
            "DELETE",
            "/api/2.0/sql/warehouses/{id}"
        ),
    )
    async def delete_warehouse(params: Dict[str, Any]):
        logger.info(f"Deleting warehouse with params: {params}")
        try:
            # Validate required parameters
            if "id" not in params:
                return missing_param_error("id")
                
            warehouse_id = params.get("id")
            
            result = await sql.delete_warehouse(warehouse_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "warehouse deletion", warehouse_id)
        except Exception as e:
            logger.error(f"Error deleting warehouse: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="start_warehouse",
        description=generate_tool_description(
            sql.start_warehouse,
            "POST",
            "/api/2.0/sql/warehouses/{id}/start"
        ),
    )
    async def start_warehouse(params: Dict[str, Any]):
        logger.info(f"Starting warehouse with params: {params}")
        try:
            # Validate required parameters
            if "id" not in params:
                return missing_param_error("id")
                
            warehouse_id = params.get("id")
            
            result = await sql.start_warehouse(warehouse_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "warehouse start", warehouse_id)
        except Exception as e:
            logger.error(f"Error starting warehouse: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="stop_warehouse",
        description=generate_tool_description(
            sql.stop_warehouse,
            "POST",
            "/api/2.0/sql/warehouses/{id}/stop"
        ),
    )
    async def stop_warehouse(params: Dict[str, Any]):
        logger.info(f"Stopping warehouse with params: {params}")
        try:
            # Validate required parameters
            if "id" not in params:
                return missing_param_error("id")
                
            warehouse_id = params.get("id")
            
            result = await sql.stop_warehouse(warehouse_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "warehouse stop", warehouse_id)
        except Exception as e:
            logger.error(f"Error stopping warehouse: {str(e)}")
            return handle_api_error(e) 