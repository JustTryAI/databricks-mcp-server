"""
Databricks Budget Management API

This module provides functions for managing budgets in Databricks.
"""

import logging
from typing import Dict, Any, Optional

from src.core.utils import make_api_request

logger = logging.getLogger(__name__)

async def create_budget(
    name: str,
    budget_configuration: Dict[str, Any],
    description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a new budget.

    Args:
        name: The name of the budget.
        budget_configuration: The configuration for the budget.
        description: Optional description for the budget.

    Returns:
        Dict containing the created budget information.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Creating budget: {name}")
    
    data = {
        "name": name,
        "budget_configuration": budget_configuration,
    }
    
    if description:
        data["description"] = description
    
    return await make_api_request("POST", "/api/2.0/budgets", data=data)

async def list_budgets() -> Dict[str, Any]:
    """
    List all budgets.

    Returns:
        Dict containing the list of budgets.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info("Listing budgets")
    
    return await make_api_request("GET", "/api/2.0/budgets")

async def get_budget(budget_id: str) -> Dict[str, Any]:
    """
    Get details of a specific budget.

    Args:
        budget_id: The ID of the budget.

    Returns:
        Dict containing the budget details.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Getting budget details: {budget_id}")
    
    return await make_api_request("GET", f"/api/2.0/budgets/{budget_id}")

async def update_budget(
    budget_id: str,
    name: Optional[str] = None,
    budget_configuration: Optional[Dict[str, Any]] = None,
    description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Update an existing budget.

    Args:
        budget_id: The ID of the budget to update.
        name: Optional new name for the budget.
        budget_configuration: Optional new configuration for the budget.
        description: Optional new description for the budget.

    Returns:
        Dict containing the updated budget information.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Updating budget: {budget_id}")
    
    data = {}
    
    if name:
        data["name"] = name
    
    if budget_configuration:
        data["budget_configuration"] = budget_configuration
    
    if description:
        data["description"] = description
    
    return await make_api_request("PATCH", f"/api/2.0/budgets/{budget_id}", data=data)

async def delete_budget(budget_id: str) -> Dict[str, Any]:
    """
    Delete a budget.

    Args:
        budget_id: The ID of the budget to delete.

    Returns:
        Dict containing the response from the API.

    Raises:
        DatabricksAPIError: If the API request fails.
    """
    logger.info(f"Deleting budget: {budget_id}")
    
    return await make_api_request("DELETE", f"/api/2.0/budgets/{budget_id}") 