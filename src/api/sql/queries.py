"""
API for managing Databricks SQL Queries.

This module provides functions for managing SQL queries in Databricks.
It is part of the SQL API group that includes Warehouses, Queries,
Dashboards, and Alerts.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

# Create an alias for backward compatibility
import sys
sql_queries = sys.modules[__name__]


async def create_query(
    name: str,
    description: Optional[str] = None,
    query: Optional[str] = None,
    parent: Optional[str] = None,
    run_as_role: Optional[str] = None
) -> Dict[str, Any]:
    """Create a new SQL query.
    
    Args:
        name: Name of the query
        description: Optional description of the query
        query: Optional SQL text of the query
        parent: Optional parent folder ID
        run_as_role: Optional role to run the query as
        
    Returns:
        Response containing the created query information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new SQL query: {name}")
    
    data = {
        "name": name
    }
    
    if description is not None:
        data["description"] = description
        
    if query is not None:
        data["query"] = query
        
    if parent is not None:
        data["parent"] = parent
        
    if run_as_role is not None:
        data["run_as_role"] = run_as_role
    
    try:
        return await make_api_request("POST", "/api/2.0/sql/queries", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to create SQL query: {str(e)}")
        raise


async def list_queries(
    page_size: Optional[int] = None,
    page: Optional[int] = None,
    order: Optional[str] = None,
    q: Optional[str] = None
) -> Dict[str, Any]:
    """List SQL queries.
    
    Args:
        page_size: Optional number of queries to return per page
        page: Optional page number to return
        order: Optional ordering field (e.g., "name", "created_at")
        q: Optional search query
        
    Returns:
        Response containing a list of queries
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing SQL queries")
    
    params = {}
    
    if page_size is not None:
        params["page_size"] = page_size
        
    if page is not None:
        params["page"] = page
        
    if order is not None:
        params["order"] = order
        
    if q is not None:
        params["q"] = q
    
    try:
        return await make_api_request("GET", "/api/2.0/sql/queries", params=params)
    except DatabricksAPIError as e:
        logger.error(f"Failed to list SQL queries: {str(e)}")
        raise


async def get_query(query_id: str) -> Dict[str, Any]:
    """Get a specific SQL query.
    
    Args:
        query_id: ID of the query
        
    Returns:
        Response containing the query information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting SQL query: {query_id}")
    
    try:
        return await make_api_request("GET", f"/api/2.0/sql/queries/{query_id}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to get SQL query: {str(e)}")
        raise


async def update_query(
    query_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    query: Optional[str] = None,
    run_as_role: Optional[str] = None
) -> Dict[str, Any]:
    """Update a SQL query.
    
    Args:
        query_id: ID of the query to update
        name: Optional new name for the query
        description: Optional new description for the query
        query: Optional new SQL text for the query
        run_as_role: Optional new role to run the query as
        
    Returns:
        Response containing the updated query information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating SQL query: {query_id}")
    
    data = {}
    
    if name is not None:
        data["name"] = name
        
    if description is not None:
        data["description"] = description
        
    if query is not None:
        data["query"] = query
        
    if run_as_role is not None:
        data["run_as_role"] = run_as_role
    
    try:
        return await make_api_request("POST", f"/api/2.0/sql/queries/{query_id}", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to update SQL query: {str(e)}")
        raise


async def delete_query(query_id: str) -> Dict[str, Any]:
    """Delete a SQL query.
    
    Args:
        query_id: ID of the query to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting SQL query: {query_id}")
    
    try:
        return await make_api_request("DELETE", f"/api/2.0/sql/queries/{query_id}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to delete SQL query: {str(e)}")
        raise


async def run_query(
    query_id: str,
    parameters: Optional[Dict[str, Any]] = None,
    warehouse_id: Optional[str] = None
) -> Dict[str, Any]:
    """Run a SQL query.
    
    Args:
        query_id: ID of the query to run
        parameters: Optional parameters for the query
        warehouse_id: Optional warehouse ID to run the query on
        
    Returns:
        Response containing the query result information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Running SQL query: {query_id}")
    
    data = {}
    
    if parameters is not None:
        data["parameters"] = parameters
        
    if warehouse_id is not None:
        data["warehouse_id"] = warehouse_id
    
    try:
        return await make_api_request("POST", f"/api/2.0/sql/queries/{query_id}/run", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to run SQL query: {str(e)}")
        raise 