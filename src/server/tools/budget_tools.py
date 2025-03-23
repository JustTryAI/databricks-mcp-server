"""Tool implementations for Databricks Budget API endpoints.

This module provides modularized tools for interacting with the Databricks Budget API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.misc import budgets
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_budget_tools(server):
    """Register Budget tools with the MCP server."""
    
    @server.tool(
        name="create_budget",
        description=generate_tool_description(
            budgets.create_budget,
            "POST",
            "/api/2.0/account/budgets"
        ),
    )
    async def create_budget(params: Dict[str, Any]):
        logger.info(f"Creating budget with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
            if "amount" not in params:
                return missing_param_error("amount")
            if "period" not in params:
                return missing_param_error("period")
                
            name = params.get("name")
            amount = params.get("amount")
            period = params.get("period")
            start_date = params.get("start_date")
            end_date = params.get("end_date")
            filters = params.get("filters", {})
            alerts = params.get("alerts", [])
            
            result = await budgets.create_budget(
                name, amount, period, start_date, end_date, filters, alerts
            )
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "budget")
        except Exception as e:
            logger.error(f"Error creating budget: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_budgets",
        description=generate_tool_description(
            budgets.list_budgets,
            "GET",
            "/api/2.0/account/budgets"
        ),
    )
    async def list_budgets(params: Dict[str, Any]):
        logger.info("Listing budgets")
        try:
            page_size = params.get("page_size", 100)
            page_token = params.get("page_token")
            
            result = await budgets.list_budgets(page_size, page_token)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "budgets")
        except Exception as e:
            logger.error(f"Error listing budgets: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_budget",
        description=generate_tool_description(
            budgets.get_budget,
            "GET",
            "/api/2.0/account/budgets/{budget_id}"
        ),
    )
    async def get_budget(params: Dict[str, Any]):
        logger.info(f"Getting budget with params: {params}")
        try:
            # Validate required parameters
            if "budget_id" not in params:
                return missing_param_error("budget_id")
                
            budget_id = params.get("budget_id")
            
            result = await budgets.get_budget(budget_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "budget", params.get("budget_id"))
        except Exception as e:
            logger.error(f"Error getting budget: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_budget",
        description=generate_tool_description(
            budgets.update_budget,
            "PATCH",
            "/api/2.0/account/budgets/{budget_id}"
        ),
    )
    async def update_budget(params: Dict[str, Any]):
        logger.info(f"Updating budget with params: {params}")
        try:
            # Validate required parameters
            if "budget_id" not in params:
                return missing_param_error("budget_id")
                
            budget_id = params.get("budget_id")
            updates = {k: v for k, v in params.items() if k != "budget_id"}
            
            result = await budgets.update_budget(budget_id, updates)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "budget", params.get("budget_id"))
        except Exception as e:
            logger.error(f"Error updating budget: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_budget",
        description=generate_tool_description(
            budgets.delete_budget,
            "DELETE",
            "/api/2.0/account/budgets/{budget_id}"
        ),
    )
    async def delete_budget(params: Dict[str, Any]):
        logger.info(f"Deleting budget with params: {params}")
        try:
            # Validate required parameters
            if "budget_id" not in params:
                return missing_param_error("budget_id")
                
            budget_id = params.get("budget_id")
            
            result = await budgets.delete_budget(budget_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "budget", params.get("budget_id"))
        except Exception as e:
            logger.error(f"Error deleting budget: {str(e)}")
            return handle_api_error(e) 