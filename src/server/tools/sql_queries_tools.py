"""Tool implementations for Databricks SQL Queries API endpoints.

This module provides modularized tools for interacting with the Databricks SQL Queries API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.sql import sql_queries
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_sql_queries_tools(server):
    """Register SQL Queries tools with the MCP server."""
    
    @server.tool(
        name="create_query",
        description=generate_tool_description(
            sql_queries.create_query,
            "POST",
            "/api/2.0/preview/sql/queries"
        ),
    )
    async def create_query(params: Dict[str, Any]):
        logger.info(f"Creating SQL query with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
            if "query" not in params:
                return missing_param_error("query")
                
            name = params.get("name")
            query = params.get("query")
            description = params.get("description", "")
            warehouse_id = params.get("warehouse_id")
            
            result = await sql_queries.create_query(name, query, description, warehouse_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "query")
        except Exception as e:
            logger.error(f"Error creating SQL query: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_queries",
        description=generate_tool_description(
            sql_queries.list_queries,
            "GET",
            "/api/2.0/preview/sql/queries"
        ),
    )
    async def list_queries(params: Dict[str, Any]):
        logger.info("Listing SQL queries")
        try:
            page_size = params.get("page_size", 100)
            page = params.get("page", 1)
            
            result = await sql_queries.list_queries(page_size, page)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "queries")
        except Exception as e:
            logger.error(f"Error listing SQL queries: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_query",
        description=generate_tool_description(
            sql_queries.get_query,
            "GET",
            "/api/2.0/preview/sql/queries/{query_id}"
        ),
    )
    async def get_query(params: Dict[str, Any]):
        logger.info(f"Getting SQL query with params: {params}")
        try:
            # Validate required parameters
            if "query_id" not in params:
                return missing_param_error("query_id")
                
            query_id = params.get("query_id")
            
            result = await sql_queries.get_query(query_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "query", params.get("query_id"))
        except Exception as e:
            logger.error(f"Error getting SQL query: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_query",
        description=generate_tool_description(
            sql_queries.update_query,
            "POST",
            "/api/2.0/preview/sql/queries/{query_id}"
        ),
    )
    async def update_query(params: Dict[str, Any]):
        logger.info(f"Updating SQL query with params: {params}")
        try:
            # Validate required parameters
            if "query_id" not in params:
                return missing_param_error("query_id")
                
            query_id = params.get("query_id")
            name = params.get("name")
            query = params.get("query")
            description = params.get("description")
            warehouse_id = params.get("warehouse_id")
            
            result = await sql_queries.update_query(query_id, name, query, description, warehouse_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "query", params.get("query_id"))
        except Exception as e:
            logger.error(f"Error updating SQL query: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_query",
        description=generate_tool_description(
            sql_queries.delete_query,
            "DELETE",
            "/api/2.0/preview/sql/queries/{query_id}"
        ),
    )
    async def delete_query(params: Dict[str, Any]):
        logger.info(f"Deleting SQL query with params: {params}")
        try:
            # Validate required parameters
            if "query_id" not in params:
                return missing_param_error("query_id")
                
            query_id = params.get("query_id")
            
            result = await sql_queries.delete_query(query_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "query", params.get("query_id"))
        except Exception as e:
            logger.error(f"Error deleting SQL query: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="run_query",
        description=generate_tool_description(
            sql_queries.run_query,
            "POST",
            "/api/2.0/preview/sql/queries/{query_id}/run"
        ),
    )
    async def run_query(params: Dict[str, Any]):
        logger.info(f"Running SQL query with params: {params}")
        try:
            # Validate required parameters
            if "query_id" not in params:
                return missing_param_error("query_id")
                
            query_id = params.get("query_id")
            parameters = params.get("parameters", {})
            
            result = await sql_queries.run_query(query_id, parameters)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "query run", params.get("query_id"))
        except Exception as e:
            logger.error(f"Error running SQL query: {str(e)}")
            return handle_api_error(e) 