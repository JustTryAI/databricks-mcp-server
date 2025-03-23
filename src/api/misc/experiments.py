"""
API for managing MLflow experiments in Databricks.

This module provides functions for interacting with the Databricks MLflow Experiments API.
It is part of the miscellaneous API group that includes various utility APIs.
"""

import logging
from typing import Any, Dict, List, Optional, Union

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def create_experiment(
    name: str,
    artifact_location: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a new MLflow experiment.
    
    Args:
        name: Name of the experiment to create
        artifact_location: Optional location to store experiment artifacts
        
    Returns:
        Response containing the created experiment details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating experiment: {name}")
    
    data = {
        "name": name,
    }
    
    if artifact_location is not None:
        data["artifact_location"] = artifact_location
    
    try:
        return await make_api_request("POST", "/api/2.0/mlflow/experiments/create", data=data)
    except Exception as e:
        logger.error(f"Failed to create experiment: {str(e)}")
        raise DatabricksAPIError(f"Failed to create experiment: {str(e)}")


async def get_experiment(
    experiment_id: Optional[str] = None,
    experiment_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Get an MLflow experiment.
    
    Args:
        experiment_id: ID of the experiment to get
        experiment_name: Name of the experiment to get
        
    Note:
        Either experiment_id or experiment_name must be provided, but not both.
        
    Returns:
        Response containing the experiment details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    if experiment_id is not None and experiment_name is not None:
        raise ValueError("Provide either experiment_id or experiment_name, not both")
    
    if experiment_id is None and experiment_name is None:
        raise ValueError("Either experiment_id or experiment_name must be provided")
    
    if experiment_id is not None:
        logger.info(f"Getting experiment by ID: {experiment_id}")
        endpoint = "/api/2.0/mlflow/experiments/get"
        params = {"experiment_id": experiment_id}
    else:
        logger.info(f"Getting experiment by name: {experiment_name}")
        endpoint = "/api/2.0/mlflow/experiments/get-by-name"
        params = {"experiment_name": experiment_name}
    
    try:
        return await make_api_request("GET", endpoint, params=params)
    except Exception as e:
        logger.error(f"Failed to get experiment: {str(e)}")
        raise DatabricksAPIError(f"Failed to get experiment: {str(e)}")


async def list_experiments(
    view_type: Optional[str] = None,
    max_results: Optional[int] = None,
    page_token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    List MLflow experiments.
    
    Args:
        view_type: Optional filter for experiment types, one of: "ACTIVE_ONLY", "DELETED_ONLY", "ALL"
        max_results: Optional maximum number of experiments to return
        page_token: Optional token for pagination
        
    Returns:
        Response containing the list of experiments
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing experiments")
    
    params = {}
    
    if view_type is not None:
        params["view_type"] = view_type
    
    if max_results is not None:
        params["max_results"] = max_results
    
    if page_token is not None:
        params["page_token"] = page_token
    
    try:
        return await make_api_request("GET", "/api/2.0/mlflow/experiments/list", params=params)
    except Exception as e:
        logger.error(f"Failed to list experiments: {str(e)}")
        raise DatabricksAPIError(f"Failed to list experiments: {str(e)}")


async def delete_experiment(experiment_id: str) -> Dict[str, Any]:
    """
    Delete an MLflow experiment.
    
    Args:
        experiment_id: ID of the experiment to delete
        
    Returns:
        Empty response if successful
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting experiment: {experiment_id}")
    
    data = {
        "experiment_id": experiment_id,
    }
    
    try:
        return await make_api_request("POST", "/api/2.0/mlflow/experiments/delete", data=data)
    except Exception as e:
        logger.error(f"Failed to delete experiment: {str(e)}")
        raise DatabricksAPIError(f"Failed to delete experiment: {str(e)}")


async def restore_experiment(experiment_id: str) -> Dict[str, Any]:
    """
    Restore a deleted MLflow experiment.
    
    Args:
        experiment_id: ID of the experiment to restore
        
    Returns:
        Empty response if successful
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Restoring experiment: {experiment_id}")
    
    data = {
        "experiment_id": experiment_id,
    }
    
    try:
        return await make_api_request("POST", "/api/2.0/mlflow/experiments/restore", data=data)
    except Exception as e:
        logger.error(f"Failed to restore experiment: {str(e)}")
        raise DatabricksAPIError(f"Failed to restore experiment: {str(e)}")


async def update_experiment(
    experiment_id: str,
    new_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Update an MLflow experiment.
    
    Args:
        experiment_id: ID of the experiment to update
        new_name: Optional new name for the experiment
        
    Returns:
        Empty response if successful
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating experiment: {experiment_id}")
    
    data = {
        "experiment_id": experiment_id,
    }
    
    if new_name is not None:
        data["new_name"] = new_name
    
    try:
        return await make_api_request("POST", "/api/2.0/mlflow/experiments/update", data=data)
    except Exception as e:
        logger.error(f"Failed to update experiment: {str(e)}")
        raise DatabricksAPIError(f"Failed to update experiment: {str(e)}")


async def get_experiment_permission_levels(experiment_id: str) -> Dict[str, Any]:
    """
    Get permission levels for an MLflow experiment.
    
    Args:
        experiment_id: ID of the experiment
        
    Returns:
        Response containing the permission levels
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting permission levels for experiment: {experiment_id}")
    
    try:
        return await make_api_request("GET", f"/api/2.0/permissions/experiments/{experiment_id}/permissionLevels")
    except Exception as e:
        logger.error(f"Failed to get experiment permission levels: {str(e)}")
        raise DatabricksAPIError(f"Failed to get experiment permission levels: {str(e)}")


async def get_experiment_permissions(experiment_id: str) -> Dict[str, Any]:
    """
    Get permissions for an MLflow experiment.
    
    Args:
        experiment_id: ID of the experiment
        
    Returns:
        Response containing the permissions details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting permissions for experiment: {experiment_id}")
    
    try:
        return await make_api_request("GET", f"/api/2.0/permissions/experiments/{experiment_id}")
    except Exception as e:
        logger.error(f"Failed to get experiment permissions: {str(e)}")
        raise DatabricksAPIError(f"Failed to get experiment permissions: {str(e)}")


async def update_experiment_permissions(
    experiment_id: str,
    access_control_list: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Update permissions for an MLflow experiment.
    
    Args:
        experiment_id: ID of the experiment
        access_control_list: List of access control items
        
    Returns:
        Response containing the updated permissions details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating permissions for experiment: {experiment_id}")
    
    data = {
        "access_control_list": access_control_list
    }
    
    try:
        return await make_api_request("PATCH", f"/api/2.0/permissions/experiments/{experiment_id}", data=data)
    except Exception as e:
        logger.error(f"Failed to update experiment permissions: {str(e)}")
        raise DatabricksAPIError(f"Failed to update experiment permissions: {str(e)}") 