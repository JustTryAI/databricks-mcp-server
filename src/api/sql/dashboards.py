"""
API for managing Databricks SQL Dashboards.

This module provides functions for managing SQL dashboards in Databricks.
It is part of the SQL API group that includes Warehouses, Queries,
Alerts, and Visualizations.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

# Create an alias for backward compatibility
import sys
sql_dashboards = sys.modules[__name__]


async def create_dashboard(
    name: str,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Create a new dashboard.
    
    Args:
        name: Name of the dashboard
        description: Optional description of the dashboard
        tags: Optional list of tags for the dashboard
        
    Returns:
        Response containing the created dashboard information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new dashboard: {name}")
    
    data = {
        "name": name
    }
    
    if description is not None:
        data["description"] = description
        
    if tags is not None:
        data["tags"] = tags
    
    try:
        return await make_api_request("POST", "/api/2.0/sql/dashboards", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to create dashboard: {str(e)}")
        raise


async def list_dashboards(
    page_size: Optional[int] = None,
    page: Optional[int] = None,
    order: Optional[str] = None,
    q: Optional[str] = None
) -> Dict[str, Any]:
    """List dashboards.
    
    Args:
        page_size: Optional number of dashboards to return per page
        page: Optional page number to return
        order: Optional ordering field (e.g., "name", "created_at")
        q: Optional search query
        
    Returns:
        Response containing a list of dashboards
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing dashboards")
    
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
        return await make_api_request("GET", "/api/2.0/sql/dashboards", params=params)
    except DatabricksAPIError as e:
        logger.error(f"Failed to list dashboards: {str(e)}")
        raise


async def get_dashboard(dashboard_id: str) -> Dict[str, Any]:
    """Get a specific dashboard.
    
    Args:
        dashboard_id: ID of the dashboard
        
    Returns:
        Response containing the dashboard information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting dashboard: {dashboard_id}")
    
    try:
        return await make_api_request("GET", f"/api/2.0/sql/dashboards/{dashboard_id}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to get dashboard: {str(e)}")
        raise


async def update_dashboard(
    dashboard_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Update a dashboard.
    
    Args:
        dashboard_id: ID of the dashboard to update
        name: Optional new name for the dashboard
        description: Optional new description for the dashboard
        tags: Optional new list of tags for the dashboard
        
    Returns:
        Response containing the updated dashboard information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating dashboard: {dashboard_id}")
    
    data = {}
    
    if name is not None:
        data["name"] = name
        
    if description is not None:
        data["description"] = description
        
    if tags is not None:
        data["tags"] = tags
    
    try:
        return await make_api_request("POST", f"/api/2.0/sql/dashboards/{dashboard_id}", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to update dashboard: {str(e)}")
        raise


async def delete_dashboard(dashboard_id: str) -> Dict[str, Any]:
    """Delete a dashboard.
    
    Args:
        dashboard_id: ID of the dashboard to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting dashboard: {dashboard_id}")
    
    try:
        return await make_api_request("DELETE", f"/api/2.0/sql/dashboards/{dashboard_id}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to delete dashboard: {str(e)}")
        raise 