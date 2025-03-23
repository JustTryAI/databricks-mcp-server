"""Tool implementations for Databricks SQL Dashboard API endpoints.

This module provides modularized tools for interacting with the Databricks SQL Dashboard API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.sql import sql_dashboards
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_sql_dashboards_tools(server):
    """Register SQL Dashboard tools with the MCP server."""
    
    @server.tool(
        name="create_dashboard",
        description=generate_tool_description(
            sql_dashboards.create_dashboard,
            "POST",
            "/api/2.0/sql/dashboards"
        ),
    )
    async def create_dashboard(params: Dict[str, Any]):
        logger.info(f"Creating SQL dashboard with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
                
            name = params.get("name")
            dashboard_filters_enabled = params.get("dashboard_filters_enabled", False)
            is_favorite = params.get("is_favorite", False)
            run_as_role = params.get("run_as_role")
            parent = params.get("parent")
            tags = params.get("tags", [])
            
            result = await sql_dashboards.create_dashboard(
                name, dashboard_filters_enabled, is_favorite, run_as_role, parent, tags
            )
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "SQL dashboard")
        except Exception as e:
            logger.error(f"Error creating SQL dashboard: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_dashboards",
        description=generate_tool_description(
            sql_dashboards.list_dashboards,
            "GET",
            "/api/2.0/sql/dashboards"
        ),
    )
    async def list_dashboards(params: Dict[str, Any]):
        logger.info("Listing SQL dashboards")
        try:
            page_size = params.get("page_size", 100)
            page = params.get("page", 1)
            order = params.get("order")
            q = params.get("q")
            
            result = await sql_dashboards.list_dashboards(page_size, page, order, q)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "SQL dashboards")
        except Exception as e:
            logger.error(f"Error listing SQL dashboards: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_dashboard",
        description=generate_tool_description(
            sql_dashboards.get_dashboard,
            "GET",
            "/api/2.0/sql/dashboards/{dashboard_id}"
        ),
    )
    async def get_dashboard(params: Dict[str, Any]):
        logger.info(f"Getting SQL dashboard with params: {params}")
        try:
            # Validate required parameters
            if "dashboard_id" not in params:
                return missing_param_error("dashboard_id")
                
            dashboard_id = params.get("dashboard_id")
            
            result = await sql_dashboards.get_dashboard(dashboard_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "SQL dashboard", params.get("dashboard_id"))
        except Exception as e:
            logger.error(f"Error getting SQL dashboard: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_dashboard",
        description=generate_tool_description(
            sql_dashboards.update_dashboard,
            "PUT",
            "/api/2.0/sql/dashboards/{dashboard_id}"
        ),
    )
    async def update_dashboard(params: Dict[str, Any]):
        logger.info(f"Updating SQL dashboard with params: {params}")
        try:
            # Validate required parameters
            if "dashboard_id" not in params:
                return missing_param_error("dashboard_id")
                
            dashboard_id = params.get("dashboard_id")
            updates = {k: v for k, v in params.items() if k != "dashboard_id"}
            
            result = await sql_dashboards.update_dashboard(dashboard_id, updates)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "SQL dashboard", params.get("dashboard_id"))
        except Exception as e:
            logger.error(f"Error updating SQL dashboard: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_dashboard",
        description=generate_tool_description(
            sql_dashboards.delete_dashboard,
            "DELETE",
            "/api/2.0/sql/dashboards/{dashboard_id}"
        ),
    )
    async def delete_dashboard(params: Dict[str, Any]):
        logger.info(f"Deleting SQL dashboard with params: {params}")
        try:
            # Validate required parameters
            if "dashboard_id" not in params:
                return missing_param_error("dashboard_id")
                
            dashboard_id = params.get("dashboard_id")
            
            result = await sql_dashboards.delete_dashboard(dashboard_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "SQL dashboard", params.get("dashboard_id"))
        except Exception as e:
            logger.error(f"Error deleting SQL dashboard: {str(e)}")
            return handle_api_error(e) 