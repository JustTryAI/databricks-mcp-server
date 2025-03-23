"""
API for managing Databricks SQL Visualizations.

This module provides functions for managing SQL visualizations in Databricks.
It is part of the SQL API group that includes Warehouses, Queries,
Dashboards, and Alerts.
"""

import logging
from typing import Any, Dict, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def create_visualization(
    query_id: str,
    visualization_type: str,
    name: str,
    options: Dict[str, Any],
    description: Optional[str] = None
) -> Dict[str, Any]:
    """Create a new visualization.
    
    Args:
        query_id: ID of the query to create a visualization for
        visualization_type: Type of visualization (e.g., "chart", "table", "map")
        name: Name of the visualization
        options: Visualization options and configuration
        description: Optional description of the visualization
        
    Returns:
        Response containing the created visualization information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new visualization: {name} for query: {query_id}")
    
    data = {
        "type": visualization_type,
        "name": name,
        "options": options
    }
    
    if description is not None:
        data["description"] = description
    
    try:
        return await make_api_request("POST", f"/api/2.0/sql/queries/{query_id}/visualizations", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to create visualization: {str(e)}")
        raise


async def update_visualization(
    visualization_id: str,
    visualization_type: Optional[str] = None,
    name: Optional[str] = None,
    options: Optional[Dict[str, Any]] = None,
    description: Optional[str] = None
) -> Dict[str, Any]:
    """Update a visualization.
    
    Args:
        visualization_id: ID of the visualization to update
        visualization_type: Optional new type of visualization
        name: Optional new name for the visualization
        options: Optional new visualization options and configuration
        description: Optional new description for the visualization
        
    Returns:
        Response containing the updated visualization information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating visualization: {visualization_id}")
    
    data = {}
    
    if visualization_type is not None:
        data["type"] = visualization_type
        
    if name is not None:
        data["name"] = name
        
    if options is not None:
        data["options"] = options
        
    if description is not None:
        data["description"] = description
    
    try:
        return await make_api_request("POST", f"/api/2.0/sql/visualizations/{visualization_id}", data=data)
    except DatabricksAPIError as e:
        logger.error(f"Failed to update visualization: {str(e)}")
        raise


async def delete_visualization(visualization_id: str) -> Dict[str, Any]:
    """Delete a visualization.
    
    Args:
        visualization_id: ID of the visualization to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting visualization: {visualization_id}")
    
    try:
        return await make_api_request("DELETE", f"/api/2.0/sql/visualizations/{visualization_id}")
    except DatabricksAPIError as e:
        logger.error(f"Failed to delete visualization: {str(e)}")
        raise 