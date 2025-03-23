"""API client for Databricks Budget endpoints.

This module provides functions to interact with the Databricks Budget API.
"""

import json
import logging
from typing import Dict, List, Optional, Any

from src.core.utils import make_api_request, DatabricksAPIError

# Configure logging
logger = logging.getLogger(__name__)

async def create_budget(name: str, 
                       amount: float, 
                       period: str, 
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       filters: Optional[Dict[str, Any]] = None,
                       alerts: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Create a new budget.
    
    Args:
        name: The name of the budget
        amount: The budget amount
        period: The budget period (MONTHLY, QUARTERLY, etc.)
        start_date: Optional start date in ISO format
        end_date: Optional end date in ISO format
        filters: Optional filters to scope the budget
        alerts: Optional alerts to be triggered
        
    Returns:
        The created budget details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating budget: {name}")
    data = {
        "name": name,
        "amount": amount,
        "period": period
    }
    
    if start_date:
        data["start_date"] = start_date
    if end_date:
        data["end_date"] = end_date
    if filters:
        data["filters"] = filters
    if alerts:
        data["alerts"] = alerts
        
    try:
        return await make_api_request("POST", "/api/2.0/budgets", data=data)
    except Exception as e:
        logger.error(f"Failed to create budget: {str(e)}")
        raise DatabricksAPIError(f"Failed to create budget: {str(e)}")


async def list_budgets(page_size: Optional[int] = None, 
                      page_token: Optional[str] = None) -> Dict[str, Any]:
    """List budgets.
    
    Args:
        page_size: Maximum number of budgets to return
        page_token: Token for pagination
        
    Returns:
        A list of budgets
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing budgets")
    params = {}
    if page_size is not None:
        params["page_size"] = page_size
    if page_token is not None:
        params["page_token"] = page_token
        
    try:
        return await make_api_request("GET", "/api/2.0/budgets", params=params)
    except Exception as e:
        logger.error(f"Failed to list budgets: {str(e)}")
        raise DatabricksAPIError(f"Failed to list budgets: {str(e)}")


async def get_budget(budget_id: str) -> Dict[str, Any]:
    """Get a specific budget.
    
    Args:
        budget_id: The ID of the budget to retrieve
        
    Returns:
        The budget details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting budget: {budget_id}")
    try:
        return await make_api_request("GET", f"/api/2.0/budgets/{budget_id}")
    except Exception as e:
        logger.error(f"Failed to get budget: {str(e)}")
        raise DatabricksAPIError(f"Failed to get budget: {str(e)}")


async def update_budget(budget_id: str, 
                       updates: Dict[str, Any]) -> Dict[str, Any]:
    """Update an existing budget.
    
    Args:
        budget_id: The ID of the budget to update
        updates: The fields to update
        
    Returns:
        The updated budget details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating budget: {budget_id}")
    try:
        return await make_api_request("PATCH", f"/api/2.0/budgets/{budget_id}", data=updates)
    except Exception as e:
        logger.error(f"Failed to update budget: {str(e)}")
        raise DatabricksAPIError(f"Failed to update budget: {str(e)}")


async def delete_budget(budget_id: str) -> Dict[str, Any]:
    """Delete a budget.
    
    Args:
        budget_id: The ID of the budget to delete
        
    Returns:
        An empty object if successful
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting budget: {budget_id}")
    try:
        return await make_api_request("DELETE", f"/api/2.0/budgets/{budget_id}")
    except Exception as e:
        logger.error(f"Failed to delete budget: {str(e)}")
        raise DatabricksAPIError(f"Failed to delete budget: {str(e)}") 