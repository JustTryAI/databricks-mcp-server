"""
API for managing Databricks SQL Queries.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def create_query(
    name: str,
    description: Optional[str] = None,
    query: Optional[str] = None,
    parent: Optional[str] = None,
    run_as_role: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create a new SQL query.
    
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
    
    data = {"name": name}
    
    if description:
        data["description"] = description
    if query:
        data["query"] = query
    if parent:
        data["parent"] = parent
    if run_as_role:
        data["run_as_role"] = run_as_role
    
    return make_api_request("POST", "/api/2.0/preview/sql/queries", data=data)


async def list_queries(
    page_size: Optional[int] = None,
    page: Optional[int] = None,
    order: Optional[str] = None,
    q: Optional[str] = None
) -> Dict[str, Any]:
    """
    List SQL queries.
    
    Args:
        page_size: Optional number of queries to return per page
        page: Optional page number
        order: Optional ordering (e.g., "name" or "created_at")
        q: Optional search query string
        
    Returns:
        Response containing a list of queries
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing SQL queries")
    
    params = {}
    if page_size:
        params["page_size"] = page_size
    if page:
        params["page"] = page
    if order:
        params["order"] = order
    if q:
        params["q"] = q
    
    return make_api_request("GET", "/api/2.0/preview/sql/queries", params=params)


async def get_query(query_id: str) -> Dict[str, Any]:
    """
    Get a specific SQL query by ID.
    
    Args:
        query_id: ID of the query
        
    Returns:
        Response containing query information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting SQL query: {query_id}")
    return make_api_request("GET", f"/api/2.0/preview/sql/queries/{query_id}")


async def update_query(
    query_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    query: Optional[str] = None,
    run_as_role: Optional[str] = None
) -> Dict[str, Any]:
    """
    Update an existing SQL query.
    
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
    if name:
        data["name"] = name
    if description:
        data["description"] = description
    if query:
        data["query"] = query
    if run_as_role:
        data["run_as_role"] = run_as_role
    
    return make_api_request("POST", f"/api/2.0/preview/sql/queries/{query_id}", data=data)


async def delete_query(query_id: str) -> Dict[str, Any]:
    """
    Delete a SQL query.
    
    Args:
        query_id: ID of the query to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting SQL query: {query_id}")
    return make_api_request("DELETE", f"/api/2.0/preview/sql/queries/{query_id}")


async def run_query(
    query_id: str,
    parameters: Optional[Dict[str, Any]] = None,
    warehouse_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Run a SQL query and return results.
    
    Args:
        query_id: ID of the query to run
        parameters: Optional parameters for the query
        warehouse_id: Optional warehouse ID to run the query on
        
    Returns:
        Response containing query results
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Running SQL query: {query_id}")
    
    data = {}
    if parameters:
        data["parameters"] = parameters
    if warehouse_id:
        data["warehouse_id"] = warehouse_id
    
    return make_api_request("POST", f"/api/2.0/preview/sql/queries/{query_id}/run", data=data)


# Visualizations
async def create_visualization(
    query_id: str,
    visualization_type: str,
    name: str,
    options: Dict[str, Any],
    description: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create a new visualization for a SQL query.
    
    Args:
        query_id: ID of the query to create visualization for
        visualization_type: Type of visualization (e.g., "chart", "table", "cohort")
        name: Name of the visualization
        options: Visualization options
        description: Optional description
        
    Returns:
        Response containing the created visualization
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating visualization for query {query_id}: {name}")
    
    data = {
        "query_id": query_id,
        "type": visualization_type,
        "name": name,
        "options": options
    }
    
    if description:
        data["description"] = description
    
    return make_api_request("POST", "/api/2.0/preview/sql/visualizations", data=data)


async def update_visualization(
    visualization_id: str,
    visualization_type: Optional[str] = None,
    name: Optional[str] = None,
    options: Optional[Dict[str, Any]] = None,
    description: Optional[str] = None
) -> Dict[str, Any]:
    """
    Update an existing visualization.
    
    Args:
        visualization_id: ID of the visualization to update
        visualization_type: Optional new type of visualization
        name: Optional new name for the visualization
        options: Optional new visualization options
        description: Optional new description
        
    Returns:
        Response containing the updated visualization
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating visualization: {visualization_id}")
    
    data = {}
    if visualization_type:
        data["type"] = visualization_type
    if name:
        data["name"] = name
    if options:
        data["options"] = options
    if description:
        data["description"] = description
    
    return make_api_request("POST", f"/api/2.0/preview/sql/visualizations/{visualization_id}", data=data)


async def delete_visualization(visualization_id: str) -> Dict[str, Any]:
    """
    Delete a visualization.
    
    Args:
        visualization_id: ID of the visualization to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting visualization: {visualization_id}")
    return make_api_request("DELETE", f"/api/2.0/preview/sql/visualizations/{visualization_id}")


# Dashboards
async def create_dashboard(
    name: str,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Create a new SQL dashboard.
    
    Args:
        name: Name of the dashboard
        description: Optional description
        tags: Optional list of tags
        
    Returns:
        Response containing the created dashboard
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating dashboard: {name}")
    
    data = {"name": name}
    
    if description:
        data["description"] = description
    if tags:
        data["tags"] = tags
    
    return make_api_request("POST", "/api/2.0/preview/sql/dashboards", data=data)


async def list_dashboards(
    page_size: Optional[int] = None,
    page: Optional[int] = None,
    order: Optional[str] = None,
    q: Optional[str] = None
) -> Dict[str, Any]:
    """
    List SQL dashboards.
    
    Args:
        page_size: Optional number of dashboards to return per page
        page: Optional page number
        order: Optional ordering (e.g., "name" or "created_at")
        q: Optional search query string
        
    Returns:
        Response containing a list of dashboards
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing dashboards")
    
    params = {}
    if page_size:
        params["page_size"] = page_size
    if page:
        params["page"] = page
    if order:
        params["order"] = order
    if q:
        params["q"] = q
    
    return make_api_request("GET", "/api/2.0/preview/sql/dashboards", params=params)


async def get_dashboard(dashboard_id: str) -> Dict[str, Any]:
    """
    Get a specific dashboard by ID.
    
    Args:
        dashboard_id: ID of the dashboard
        
    Returns:
        Response containing dashboard information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting dashboard: {dashboard_id}")
    return make_api_request("GET", f"/api/2.0/preview/sql/dashboards/{dashboard_id}")


async def update_dashboard(
    dashboard_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Update an existing dashboard.
    
    Args:
        dashboard_id: ID of the dashboard to update
        name: Optional new name for the dashboard
        description: Optional new description
        tags: Optional new list of tags
        
    Returns:
        Response containing the updated dashboard
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating dashboard: {dashboard_id}")
    
    data = {}
    if name:
        data["name"] = name
    if description:
        data["description"] = description
    if tags:
        data["tags"] = tags
    
    return make_api_request("POST", f"/api/2.0/preview/sql/dashboards/{dashboard_id}", data=data)


async def delete_dashboard(dashboard_id: str) -> Dict[str, Any]:
    """
    Delete a dashboard.
    
    Args:
        dashboard_id: ID of the dashboard to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting dashboard: {dashboard_id}")
    return make_api_request("DELETE", f"/api/2.0/preview/sql/dashboards/{dashboard_id}")


# Alerts
async def create_alert(
    query_id: str,
    name: str,
    options: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Create a new alert for a SQL query.
    
    Args:
        query_id: ID of the query to create an alert for
        name: Name of the alert
        options: Alert options (containing threshold values, etc.)
        
    Returns:
        Response containing the created alert
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating alert for query {query_id}: {name}")
    
    data = {
        "query_id": query_id,
        "name": name,
        "options": options
    }
    
    return make_api_request("POST", "/api/2.0/preview/sql/alerts", data=data)


async def list_alerts(
    page_size: Optional[int] = None,
    page: Optional[int] = None
) -> Dict[str, Any]:
    """
    List SQL alerts.
    
    Args:
        page_size: Optional number of alerts to return per page
        page: Optional page number
        
    Returns:
        Response containing a list of alerts
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing alerts")
    
    params = {}
    if page_size:
        params["page_size"] = page_size
    if page:
        params["page"] = page
    
    return make_api_request("GET", "/api/2.0/preview/sql/alerts", params=params)


async def get_alert(alert_id: str) -> Dict[str, Any]:
    """
    Get a specific alert by ID.
    
    Args:
        alert_id: ID of the alert
        
    Returns:
        Response containing alert information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting alert: {alert_id}")
    return make_api_request("GET", f"/api/2.0/preview/sql/alerts/{alert_id}")


async def update_alert(
    alert_id: str,
    name: Optional[str] = None,
    options: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Update an existing alert.
    
    Args:
        alert_id: ID of the alert to update
        name: Optional new name for the alert
        options: Optional new alert options
        
    Returns:
        Response containing the updated alert
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating alert: {alert_id}")
    
    data = {}
    if name:
        data["name"] = name
    if options:
        data["options"] = options
    
    return make_api_request("POST", f"/api/2.0/preview/sql/alerts/{alert_id}", data=data)


async def delete_alert(alert_id: str) -> Dict[str, Any]:
    """
    Delete an alert.
    
    Args:
        alert_id: ID of the alert to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting alert: {alert_id}")
    return make_api_request("DELETE", f"/api/2.0/preview/sql/alerts/{alert_id}") 