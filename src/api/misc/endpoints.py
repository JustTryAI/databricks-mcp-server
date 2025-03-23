"""
API for managing Databricks serving endpoints.

This module provides functions for interacting with the Databricks Serving Endpoints API.
It is part of the miscellaneous API group that includes various utility APIs.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def create_serving_endpoint(
    name: str,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Create a new serving endpoint.
    
    Args:
        name: Name of the serving endpoint
        config: Configuration for the serving endpoint
        
    Returns:
        Response containing the created endpoint details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating serving endpoint: {name}")
    
    data = {
        "name": name,
        "config": config,
    }
    
    try:
        return await make_api_request("POST", "/api/2.0/serving-endpoints", data=data)
    except Exception as e:
        logger.error(f"Failed to create serving endpoint: {str(e)}")
        raise DatabricksAPIError(f"Failed to create serving endpoint: {str(e)}")


async def get_serving_endpoint(name: str) -> Dict[str, Any]:
    """
    Get details of a specific serving endpoint.
    
    Args:
        name: Name of the serving endpoint to get
        
    Returns:
        Response containing the endpoint details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting serving endpoint: {name}")
    
    try:
        return await make_api_request("GET", f"/api/2.0/serving-endpoints/{name}")
    except Exception as e:
        logger.error(f"Failed to get serving endpoint: {str(e)}")
        raise DatabricksAPIError(f"Failed to get serving endpoint: {str(e)}")


async def list_serving_endpoints() -> Dict[str, Any]:
    """
    List all serving endpoints.
    
    Returns:
        Response containing the list of endpoints
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing serving endpoints")
    
    try:
        return await make_api_request("GET", "/api/2.0/serving-endpoints")
    except Exception as e:
        logger.error(f"Failed to list serving endpoints: {str(e)}")
        raise DatabricksAPIError(f"Failed to list serving endpoints: {str(e)}")


async def update_serving_endpoint(
    name: str,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Update an existing serving endpoint.
    
    Args:
        name: Name of the serving endpoint to update
        config: New configuration for the serving endpoint
        
    Returns:
        Response containing the updated endpoint details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating serving endpoint: {name}")
    
    data = {
        "name": name,
        "config": config,
    }
    
    try:
        return await make_api_request("PUT", f"/api/2.0/serving-endpoints/{name}/config", data=data)
    except Exception as e:
        logger.error(f"Failed to update serving endpoint: {str(e)}")
        raise DatabricksAPIError(f"Failed to update serving endpoint: {str(e)}")


async def delete_serving_endpoint(name: str) -> Dict[str, Any]:
    """
    Delete a serving endpoint.
    
    Args:
        name: Name of the serving endpoint to delete
        
    Returns:
        Empty response if successful
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting serving endpoint: {name}")
    
    try:
        return await make_api_request("DELETE", f"/api/2.0/serving-endpoints/{name}")
    except Exception as e:
        logger.error(f"Failed to delete serving endpoint: {str(e)}")
        raise DatabricksAPIError(f"Failed to delete serving endpoint: {str(e)}")


async def get_serving_endpoint_logs(
    name: str,
    start_timestamp: Optional[int] = None,
    end_timestamp: Optional[int] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Get logs for a serving endpoint.
    
    Args:
        name: Name of the serving endpoint
        start_timestamp: Optional start time in milliseconds since epoch
        end_timestamp: Optional end time in milliseconds since epoch
        limit: Optional maximum number of log lines to return
        
    Returns:
        Response containing the endpoint logs
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting logs for serving endpoint: {name}")
    
    params = {}
    
    if start_timestamp is not None:
        params["start_timestamp"] = start_timestamp
    
    if end_timestamp is not None:
        params["end_timestamp"] = end_timestamp
    
    if limit is not None:
        params["limit"] = limit
    
    try:
        return await make_api_request("GET", f"/api/2.0/serving-endpoints/{name}/logs", params=params)
    except Exception as e:
        logger.error(f"Failed to get serving endpoint logs: {str(e)}")
        raise DatabricksAPIError(f"Failed to get serving endpoint logs: {str(e)}")


async def query_serving_endpoint(
    name: str,
    dataframe_records: Optional[List[Dict[str, Any]]] = None,
    dataframe_split: Optional[Dict[str, Any]] = None,
    inputs: Optional[List[Any]] = None,
    tensor_inputs: Optional[Dict[str, Any]] = None,
    input_type: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Query a serving endpoint.
    
    Args:
        name: Name of the serving endpoint to query
        dataframe_records: Optional list of dictionaries for dataframe input
        dataframe_split: Optional split-format dataframe input
        inputs: Optional list of arbitrary inputs
        tensor_inputs: Optional dictionary of tensor inputs
        input_type: Optional input type specifier
        
    Returns:
        Response containing the query results
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Querying serving endpoint: {name}")
    
    data = {}
    
    if dataframe_records is not None:
        data["dataframe_records"] = dataframe_records
    
    if dataframe_split is not None:
        data["dataframe_split"] = dataframe_split
    
    if inputs is not None:
        data["inputs"] = inputs
    
    if tensor_inputs is not None:
        data["tensor_inputs"] = tensor_inputs
    
    if input_type is not None:
        data["input_type"] = input_type
    
    try:
        return await make_api_request("POST", f"/api/2.0/serving-endpoints/{name}/invocations", data=data)
    except Exception as e:
        logger.error(f"Failed to query serving endpoint: {str(e)}")
        raise DatabricksAPIError(f"Failed to query serving endpoint: {str(e)}")


async def get_serving_endpoint_permission_levels(name: str) -> Dict[str, Any]:
    """
    Get permission levels for a serving endpoint.
    
    Args:
        name: Name of the serving endpoint
        
    Returns:
        Response containing the permission levels
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting permission levels for serving endpoint: {name}")
    
    try:
        return await make_api_request("GET", f"/api/2.0/permissions/serving-endpoints/{name}/permissionLevels")
    except Exception as e:
        logger.error(f"Failed to get serving endpoint permission levels: {str(e)}")
        raise DatabricksAPIError(f"Failed to get serving endpoint permission levels: {str(e)}")


async def get_serving_endpoint_permissions(name: str) -> Dict[str, Any]:
    """
    Get permissions for a serving endpoint.
    
    Args:
        name: Name of the serving endpoint
        
    Returns:
        Response containing the permissions details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting permissions for serving endpoint: {name}")
    
    try:
        return await make_api_request("GET", f"/api/2.0/permissions/serving-endpoints/{name}")
    except Exception as e:
        logger.error(f"Failed to get serving endpoint permissions: {str(e)}")
        raise DatabricksAPIError(f"Failed to get serving endpoint permissions: {str(e)}")


async def update_serving_endpoint_permissions(
    name: str,
    access_control_list: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Update permissions for a serving endpoint.
    
    Args:
        name: Name of the serving endpoint
        access_control_list: List of access control items
        
    Returns:
        Response containing the updated permissions details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating permissions for serving endpoint: {name}")
    
    data = {
        "access_control_list": access_control_list
    }
    
    try:
        return await make_api_request("PATCH", f"/api/2.0/permissions/serving-endpoints/{name}", data=data)
    except Exception as e:
        logger.error(f"Failed to update serving endpoint permissions: {str(e)}")
        raise DatabricksAPIError(f"Failed to update serving endpoint permissions: {str(e)}") 