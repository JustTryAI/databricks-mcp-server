"""Tool implementations for Databricks SQL Alert API endpoints.

This module provides modularized tools for interacting with the Databricks SQL Alert API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.sql import sql_alerts
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_sql_alerts_tools(server):
    """Register SQL Alert tools with the MCP server."""
    
    @server.tool(
        name="create_alert",
        description=generate_tool_description(
            sql_alerts.create_alert,
            "POST",
            "/api/2.0/sql/alerts"
        ),
    )
    async def create_alert(params: Dict[str, Any]):
        logger.info(f"Creating SQL alert with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
            if "query_id" not in params:
                return missing_param_error("query_id")
            if "options" not in params:
                return missing_param_error("options")
                
            name = params.get("name")
            query_id = params.get("query_id")
            options = params.get("options")
            parent = params.get("parent")
            rearm = params.get("rearm")
            
            result = await sql_alerts.create_alert(
                name, query_id, options, parent, rearm
            )
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "SQL alert")
        except Exception as e:
            logger.error(f"Error creating SQL alert: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_alerts",
        description=generate_tool_description(
            sql_alerts.list_alerts,
            "GET",
            "/api/2.0/sql/alerts"
        ),
    )
    async def list_alerts(params: Dict[str, Any]):
        logger.info("Listing SQL alerts")
        try:
            page_size = params.get("page_size", 100)
            page = params.get("page", 1)
            
            result = await sql_alerts.list_alerts(page_size, page)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "SQL alerts")
        except Exception as e:
            logger.error(f"Error listing SQL alerts: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_alert",
        description=generate_tool_description(
            sql_alerts.get_alert,
            "GET",
            "/api/2.0/sql/alerts/{alert_id}"
        ),
    )
    async def get_alert(params: Dict[str, Any]):
        logger.info(f"Getting SQL alert with params: {params}")
        try:
            # Validate required parameters
            if "alert_id" not in params:
                return missing_param_error("alert_id")
                
            alert_id = params.get("alert_id")
            
            result = await sql_alerts.get_alert(alert_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "SQL alert", params.get("alert_id"))
        except Exception as e:
            logger.error(f"Error getting SQL alert: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_alert",
        description=generate_tool_description(
            sql_alerts.update_alert,
            "PUT",
            "/api/2.0/sql/alerts/{alert_id}"
        ),
    )
    async def update_alert(params: Dict[str, Any]):
        logger.info(f"Updating SQL alert with params: {params}")
        try:
            # Validate required parameters
            if "alert_id" not in params:
                return missing_param_error("alert_id")
                
            alert_id = params.get("alert_id")
            updates = {k: v for k, v in params.items() if k != "alert_id"}
            
            result = await sql_alerts.update_alert(alert_id, updates)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "SQL alert", params.get("alert_id"))
        except Exception as e:
            logger.error(f"Error updating SQL alert: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_alert",
        description=generate_tool_description(
            sql_alerts.delete_alert,
            "DELETE",
            "/api/2.0/sql/alerts/{alert_id}"
        ),
    )
    async def delete_alert(params: Dict[str, Any]):
        logger.info(f"Deleting SQL alert with params: {params}")
        try:
            # Validate required parameters
            if "alert_id" not in params:
                return missing_param_error("alert_id")
                
            alert_id = params.get("alert_id")
            
            result = await sql_alerts.delete_alert(alert_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "SQL alert", params.get("alert_id"))
        except Exception as e:
            logger.error(f"Error deleting SQL alert: {str(e)}")
            return handle_api_error(e) 