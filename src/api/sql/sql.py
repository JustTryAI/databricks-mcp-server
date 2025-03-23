"""
API for managing Databricks SQL operations.

This module provides direct access to Databricks SQL operations, primarily
as a convenience wrapper around the warehouses.py and other SQL modules.
"""

import logging
from typing import Any, Dict, List, Optional

from src.api.sql.warehouses import (
    list_warehouses,
    get_warehouse,
    create_warehouse,
    update_warehouse,
    delete_warehouse,
    start_warehouse,
    stop_warehouse
)
from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

# Re-export warehouses functions directly
__all__ = [
    "execute_sql",
    "list_warehouses",
    "get_warehouse",
    "create_warehouse",
    "update_warehouse",
    "delete_warehouse",
    "start_warehouse",
    "stop_warehouse"
]

async def execute_sql(
    statement: str,
    warehouse_id: str,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    parameters: Optional[Dict[str, Any]] = None,
    row_limit: Optional[int] = None,
    byte_limit: Optional[int] = None,
    wait_timeout: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute a SQL statement on a SQL warehouse.

    Args:
        statement: The SQL statement to execute.
        warehouse_id: The ID of the SQL warehouse to use.
        catalog: The catalog to use.
        schema: The schema to use.
        parameters: Named parameters for the SQL statement.
        row_limit: Maximum number of rows to return.
        byte_limit: Maximum number of bytes to return.
        wait_timeout: Maximum time to wait for query to complete (ms).

    Returns:
        Dict[str, Any]: Response containing the query results.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Executing SQL statement on warehouse {warehouse_id}")
    
    data = {
        "statement": statement,
        "warehouse_id": warehouse_id
    }
    
    if catalog:
        data["catalog"] = catalog
    if schema:
        data["schema"] = schema
    if parameters:
        data["parameters"] = parameters
    if row_limit is not None:
        data["row_limit"] = row_limit
    if byte_limit is not None:
        data["byte_limit"] = byte_limit
    if wait_timeout is not None:
        data["wait_timeout"] = wait_timeout
    
    try:
        response = await make_api_request(
            "POST", 
            "/api/2.0/sql/statements", 
            data
        )
        return response
    except Exception as e:
        logger.error(f"Error executing SQL statement: {str(e)}")
        raise DatabricksAPIError(f"Failed to execute SQL statement: {str(e)}") 