"""
API for managing Databricks Delta Live Tables pipelines.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def create_pipeline(pipeline_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new Delta Live Tables pipeline.
    
    Args:
        pipeline_config: Pipeline configuration
        
    Returns:
        Response containing the pipeline ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Creating new pipeline")
    return await make_api_request("POST", "/api/2.0/pipelines", data=pipeline_config)


async def list_pipelines(max_results: Optional[int] = None, page_token: Optional[str] = None) -> Dict[str, Any]:
    """
    List all Delta Live Tables pipelines.
    
    Args:
        max_results: Optional maximum number of results to return
        page_token: Optional token for pagination
        
    Returns:
        Response containing a list of pipelines
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing all pipelines")
    
    params = {}
    if max_results:
        params["max_results"] = max_results
    if page_token:
        params["page_token"] = page_token
        
    return await make_api_request("GET", "/api/2.0/pipelines", params=params)


async def get_pipeline(pipeline_id: str) -> Dict[str, Any]:
    """
    Get details about a specific pipeline.
    
    Args:
        pipeline_id: ID of the pipeline
        
    Returns:
        Response containing pipeline details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting pipeline details: {pipeline_id}")
    return await make_api_request("GET", f"/api/2.0/pipelines/{pipeline_id}")


async def update_pipeline(pipeline_id: str, pipeline_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update an existing Delta Live Tables pipeline.
    
    Args:
        pipeline_id: ID of the pipeline to update
        pipeline_config: New pipeline configuration
        
    Returns:
        Response containing success information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating pipeline: {pipeline_id}")
    return await make_api_request("PUT", f"/api/2.0/pipelines/{pipeline_id}", data=pipeline_config)


async def delete_pipeline(pipeline_id: str) -> Dict[str, Any]:
    """
    Delete a Delta Live Tables pipeline.
    
    Args:
        pipeline_id: ID of the pipeline to delete
        
    Returns:
        Response containing success information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting pipeline: {pipeline_id}")
    return await make_api_request("DELETE", f"/api/2.0/pipelines/{pipeline_id}")


async def start_pipeline_update(pipeline_id: str, full_refresh: Optional[bool] = None) -> Dict[str, Any]:
    """
    Start an update of a Delta Live Tables pipeline.
    
    Args:
        pipeline_id: ID of the pipeline to update
        full_refresh: Optional flag to perform a full refresh
        
    Returns:
        Response containing the update ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Starting update for pipeline: {pipeline_id}")
    
    data = {}
    if full_refresh is not None:
        data["full_refresh"] = full_refresh
        
    return await make_api_request("POST", f"/api/2.0/pipelines/{pipeline_id}/updates", data=data)


async def get_pipeline_update(pipeline_id: str, update_id: str) -> Dict[str, Any]:
    """
    Get details of a specific pipeline update.
    
    Args:
        pipeline_id: ID of the pipeline
        update_id: ID of the update
        
    Returns:
        Response containing update details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting update details for pipeline: {pipeline_id}, update: {update_id}")
    return await make_api_request("GET", f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}")


async def list_pipeline_updates(
    pipeline_id: str,
    max_results: Optional[int] = None,
    page_token: Optional[str] = None
) -> Dict[str, Any]:
    """
    List updates for a specific pipeline.
    
    Args:
        pipeline_id: ID of the pipeline
        max_results: Optional maximum number of results to return
        page_token: Optional token for pagination
        
    Returns:
        Response containing a list of updates
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Listing updates for pipeline: {pipeline_id}")
    
    params = {}
    if max_results:
        params["max_results"] = max_results
    if page_token:
        params["page_token"] = page_token
        
    return await make_api_request("GET", f"/api/2.0/pipelines/{pipeline_id}/updates", params=params) 