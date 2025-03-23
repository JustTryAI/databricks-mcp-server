"""
API for managing Databricks Delta Live Tables (DLT) pipelines.

This module provides functions for interacting with the Databricks Pipelines API.
It is part of the workflow API group that includes jobs and pipelines.
"""

import logging
from typing import Any, Dict, List, Optional, Union

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def create_pipeline(
    name: str,
    storage_location: str,
    target: Optional[str] = None,
    configuration: Optional[Dict[str, str]] = None,
    clusters: Optional[List[Dict[str, Any]]] = None,
    libraries: Optional[List[Dict[str, Any]]] = None,
    continuous: Optional[bool] = None,
    development: Optional[bool] = None,
    photon: Optional[bool] = None,
    edition: Optional[str] = None,
    channel: Optional[str] = None,
    catalog: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Create a new Delta Live Tables (DLT) pipeline.
    
    Args:
        name: The name of the pipeline
        storage_location: DBFS root directory for storing pipeline state
        target: Optional target schema for tables
        configuration: Optional configuration parameters 
        clusters: Optional cluster configurations
        libraries: Optional library specifications
        continuous: Optional flag for continuous execution
        development: Optional development mode flag
        photon: Optional photon mode flag
        edition: Optional pipeline edition
        channel: Optional release channel
        catalog: Optional catalog name for Unity Catalog
        **kwargs: Additional parameters to pass to the API
        
    Returns:
        Response containing the pipeline ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating pipeline: {name}")
    
    data = {
        "name": name,
        "storage": storage_location,
    }
    
    # Add optional parameters
    optional_params = {
        "target": target,
        "configuration": configuration,
        "clusters": clusters,
        "libraries": libraries,
        "continuous": continuous,
        "development": development,
        "photon": photon,
        "edition": edition,
        "channel": channel,
        "catalog": catalog,
    }
    
    for key, value in optional_params.items():
        if value is not None:
            data[key] = value
    
    # Add any additional parameters
    for key, value in kwargs.items():
        data[key] = value
    
    return make_api_request("POST", "/api/2.0/pipelines", data=data)


async def get_pipeline(pipeline_id: str) -> Dict[str, Any]:
    """
    Get information about a specific pipeline.
    
    Args:
        pipeline_id: The ID of the pipeline
        
    Returns:
        Response containing the pipeline information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting pipeline info for {pipeline_id}")
    return make_api_request("GET", f"/api/2.0/pipelines/{pipeline_id}")


async def list_pipelines(
    max_results: Optional[int] = None,
    page_token: Optional[str] = None,
    filter_string: Optional[str] = None,
) -> Dict[str, Any]:
    """
    List all pipelines in the workspace.
    
    Args:
        max_results: Maximum number of pipelines to return
        page_token: Token for pagination
        filter_string: Optional filter expression
        
    Returns:
        Response containing the list of pipelines
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing pipelines")
    
    params = {}
    if max_results is not None:
        params["max_results"] = max_results
    if page_token is not None:
        params["page_token"] = page_token
    if filter_string is not None:
        params["filter"] = filter_string
    
    return make_api_request("GET", "/api/2.0/pipelines", params=params)


async def update_pipeline(
    pipeline_id: str,
    name: Optional[str] = None,
    storage_location: Optional[str] = None,
    target: Optional[str] = None,
    configuration: Optional[Dict[str, str]] = None,
    clusters: Optional[List[Dict[str, Any]]] = None,
    libraries: Optional[List[Dict[str, Any]]] = None,
    continuous: Optional[bool] = None,
    development: Optional[bool] = None,
    photon: Optional[bool] = None,
    edition: Optional[str] = None,
    channel: Optional[str] = None,
    catalog: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Update an existing pipeline.
    
    Args:
        pipeline_id: The ID of the pipeline to update
        name: Optional new name for the pipeline
        storage_location: Optional new storage location
        target: Optional new target schema
        configuration: Optional new configuration parameters
        clusters: Optional new cluster configurations
        libraries: Optional new library specifications
        continuous: Optional new continuous execution flag
        development: Optional new development mode flag
        photon: Optional new photon mode flag
        edition: Optional new pipeline edition
        channel: Optional new release channel
        catalog: Optional new catalog name for Unity Catalog
        **kwargs: Additional parameters to update
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating pipeline {pipeline_id}")
    
    data = {}
    
    # Add optional parameters
    optional_params = {
        "name": name,
        "storage": storage_location,
        "target": target,
        "configuration": configuration,
        "clusters": clusters,
        "libraries": libraries,
        "continuous": continuous,
        "development": development,
        "photon": photon,
        "edition": edition,
        "channel": channel,
        "catalog": catalog,
    }
    
    for key, value in optional_params.items():
        if value is not None:
            data[key] = value
    
    # Add any additional parameters
    for key, value in kwargs.items():
        data[key] = value
    
    return make_api_request("PUT", f"/api/2.0/pipelines/{pipeline_id}", data=data)


async def delete_pipeline(pipeline_id: str) -> Dict[str, Any]:
    """
    Delete a pipeline.
    
    Args:
        pipeline_id: The ID of the pipeline to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting pipeline {pipeline_id}")
    return make_api_request("DELETE", f"/api/2.0/pipelines/{pipeline_id}")


async def start_update(
    pipeline_id: str,
    full_refresh: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Start an update of a pipeline.
    
    Args:
        pipeline_id: The ID of the pipeline to update
        full_refresh: Whether to perform a full refresh
        
    Returns:
        Response containing the update ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Starting update for pipeline {pipeline_id}")
    
    data = {}
    if full_refresh is not None:
        data["full_refresh"] = full_refresh
    
    return make_api_request("POST", f"/api/2.0/pipelines/{pipeline_id}/updates", data=data)


# Alias for start_update for compatibility
async def start_pipeline_update(
    pipeline_id: str,
    full_refresh: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Start an update of a pipeline.
    
    Args:
        pipeline_id: The ID of the pipeline to update
        full_refresh: Whether to perform a full refresh
        
    Returns:
        Response containing the update ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await start_update(pipeline_id, full_refresh)


async def get_update(
    pipeline_id: str,
    update_id: str,
) -> Dict[str, Any]:
    """
    Get information about a specific update.
    
    Args:
        pipeline_id: The ID of the pipeline
        update_id: The ID of the update
        
    Returns:
        Response containing the update information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting update info for pipeline {pipeline_id}, update {update_id}")
    return make_api_request("GET", f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}")


# Alias for get_update for compatibility
async def get_pipeline_update(
    pipeline_id: str,
    update_id: str,
) -> Dict[str, Any]:
    """
    Get information about a specific pipeline update.
    
    Args:
        pipeline_id: The ID of the pipeline
        update_id: The ID of the update
        
    Returns:
        Response containing the update information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    return await get_update(pipeline_id, update_id)


async def list_pipeline_updates(
    pipeline_id: str,
    max_results: Optional[int] = None,
    page_token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    List updates for a pipeline.
    
    Args:
        pipeline_id: The ID of the pipeline
        max_results: Maximum number of updates to return
        page_token: Token for pagination
        
    Returns:
        Response containing the list of updates
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Listing updates for pipeline {pipeline_id}")
    
    params = {}
    if max_results is not None:
        params["max_results"] = max_results
    if page_token is not None:
        params["page_token"] = page_token
    
    return make_api_request(
        "GET", f"/api/2.0/pipelines/{pipeline_id}/updates", params=params
    )


async def list_pipeline_events(
    pipeline_id: str,
    max_results: Optional[int] = None,
    order_by: Optional[List[str]] = None,
    filter_string: Optional[str] = None,
    page_token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    List events for a pipeline.
    
    Args:
        pipeline_id: The ID of the pipeline
        max_results: Maximum number of events to return
        order_by: Sort order for events
        filter_string: Filter expression
        page_token: Token for pagination
        
    Returns:
        Response containing the list of events
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Listing events for pipeline {pipeline_id}")
    
    params = {}
    if max_results is not None:
        params["max_results"] = max_results
    if order_by is not None:
        params["order_by"] = order_by
    if filter_string is not None:
        params["filter"] = filter_string
    if page_token is not None:
        params["page_token"] = page_token
    
    return make_api_request(
        "GET", f"/api/2.0/pipelines/{pipeline_id}/events", params=params
    ) 