"""
API for managing Databricks SQL Alerts.

This module provides functions for managing SQL alerts in Databricks.
It is part of the SQL API group that includes Warehouses, Queries,
Dashboards, and Visualizations.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

# Create an alias for backward compatibility
import sys
sql_alerts = sys.modules[__name__]

async def create_alert(
    name: str,
    options: Dict[str, Any],
    query_id: str,
    parent: Optional[str] = None,
    rearm: Optional[int] = None
) -> Dict[str, Any]:
    """Create a new SQL alert.

    Args:
        name: Name of the alert.
        options: Alert options including thresholds.
        query_id: ID of the query to monitor.
        parent: ID of the parent folder (if any).
        rearm: Time in seconds before the alert can be triggered again.

    Returns:
        Dict[str, Any]: Response containing the created alert.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Creating SQL alert: {name}")
    
    data = {
        "name": name,
        "options": options,
        "query_id": query_id
    }
    
    if parent:
        data["parent"] = parent
    if rearm is not None:
        data["rearm"] = rearm
    
    try:
        response = await make_api_request("POST", "/api/2.0/sql/alerts", data)
        return response
    except Exception as e:
        logger.error(f"Error creating SQL alert '{name}': {str(e)}")
        raise DatabricksAPIError(f"Failed to create SQL alert '{name}': {str(e)}")

async def list_alerts() -> Dict[str, Any]:
    """List all SQL alerts.

    Returns:
        Dict[str, Any]: Response containing the list of alerts.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info("Listing all SQL alerts")
    
    try:
        response = await make_api_request("GET", "/api/2.0/sql/alerts")
        return response
    except Exception as e:
        logger.error(f"Error listing SQL alerts: {str(e)}")
        raise DatabricksAPIError(f"Failed to list SQL alerts: {str(e)}")

async def get_alert(alert_id: str) -> Dict[str, Any]:
    """Get a SQL alert by ID.

    Args:
        alert_id: ID of the alert to retrieve.

    Returns:
        Dict[str, Any]: Response containing the alert details.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Getting SQL alert with ID: {alert_id}")
    
    try:
        response = await make_api_request("GET", f"/api/2.0/sql/alerts/{alert_id}")
        return response
    except Exception as e:
        logger.error(f"Error getting SQL alert with ID '{alert_id}': {str(e)}")
        raise DatabricksAPIError(f"Failed to get SQL alert with ID '{alert_id}': {str(e)}")

async def update_alert(
    alert_id: str,
    name: Optional[str] = None,
    options: Optional[Dict[str, Any]] = None,
    query_id: Optional[str] = None,
    parent: Optional[str] = None,
    rearm: Optional[int] = None
) -> Dict[str, Any]:
    """Update a SQL alert.

    Args:
        alert_id: ID of the alert to update.
        name: New name for the alert.
        options: New alert options.
        query_id: New query ID to monitor.
        parent: New parent folder ID.
        rearm: New rearm time in seconds.

    Returns:
        Dict[str, Any]: Response containing the updated alert.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Updating SQL alert with ID: {alert_id}")
    
    data = {}
    if name:
        data["name"] = name
    if options:
        data["options"] = options
    if query_id:
        data["query_id"] = query_id
    if parent:
        data["parent"] = parent
    if rearm is not None:
        data["rearm"] = rearm
    
    if not data:
        raise DatabricksAPIError("At least one field to update must be provided")
    
    try:
        response = await make_api_request("PUT", f"/api/2.0/sql/alerts/{alert_id}", data)
        return response
    except Exception as e:
        logger.error(f"Error updating SQL alert with ID '{alert_id}': {str(e)}")
        raise DatabricksAPIError(f"Failed to update SQL alert with ID '{alert_id}': {str(e)}")

async def delete_alert(alert_id: str) -> Dict[str, Any]:
    """Delete a SQL alert.

    Args:
        alert_id: ID of the alert to delete.

    Returns:
        Dict[str, Any]: Response from the delete operation.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Deleting SQL alert with ID: {alert_id}")
    
    try:
        response = await make_api_request("DELETE", f"/api/2.0/sql/alerts/{alert_id}")
        return {"message": f"SQL alert with ID '{alert_id}' deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting SQL alert with ID '{alert_id}': {str(e)}")
        raise DatabricksAPIError(f"Failed to delete SQL alert with ID '{alert_id}': {str(e)}") 