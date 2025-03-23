"""Tool implementations for Databricks Pipelines API endpoints.

This module provides modularized tools for interacting with the Databricks Pipelines API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.workflow import pipelines
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_pipelines_tools(server):
    """Register Pipelines tools with the MCP server."""
    
    @server.tool(
        name="create_pipeline",
        description=generate_tool_description(
            pipelines.create_pipeline,
            "POST",
            "/api/2.0/pipelines"
        ),
    )
    async def create_pipeline(params: Dict[str, Any]):
        logger.info(f"Creating pipeline with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
            if "storage" not in params:
                return missing_param_error("storage")
                
            pipeline_config = params
            
            result = await pipelines.create_pipeline(pipeline_config)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "pipeline")
        except Exception as e:
            logger.error(f"Error creating pipeline: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_pipelines",
        description=generate_tool_description(
            pipelines.list_pipelines,
            "GET",
            "/api/2.0/pipelines"
        ),
    )
    async def list_pipelines(params: Dict[str, Any]):
        logger.info("Listing pipelines")
        try:
            max_results = params.get("max_results")
            page_token = params.get("page_token")
            filter_string = params.get("filter")
            
            result = await pipelines.list_pipelines(max_results, page_token, filter_string)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "pipelines")
        except Exception as e:
            logger.error(f"Error listing pipelines: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_pipeline",
        description=generate_tool_description(
            pipelines.get_pipeline,
            "GET",
            "/api/2.0/pipelines/{pipeline_id}"
        ),
    )
    async def get_pipeline(params: Dict[str, Any]):
        logger.info(f"Getting pipeline with params: {params}")
        try:
            # Validate required parameters
            if "pipeline_id" not in params:
                return missing_param_error("pipeline_id")
                
            pipeline_id = params.get("pipeline_id")
            
            result = await pipelines.get_pipeline(pipeline_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "pipeline", params.get("pipeline_id"))
        except Exception as e:
            logger.error(f"Error getting pipeline: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_pipeline",
        description=generate_tool_description(
            pipelines.update_pipeline,
            "PATCH",
            "/api/2.0/pipelines/{pipeline_id}"
        ),
    )
    async def update_pipeline(params: Dict[str, Any]):
        logger.info(f"Updating pipeline with params: {params}")
        try:
            # Validate required parameters
            if "pipeline_id" not in params:
                return missing_param_error("pipeline_id")
                
            pipeline_id = params.get("pipeline_id")
            updates = {k: v for k, v in params.items() if k != "pipeline_id"}
            
            result = await pipelines.update_pipeline(pipeline_id, updates)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "pipeline", params.get("pipeline_id"))
        except Exception as e:
            logger.error(f"Error updating pipeline: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_pipeline",
        description=generate_tool_description(
            pipelines.delete_pipeline,
            "DELETE",
            "/api/2.0/pipelines/{pipeline_id}"
        ),
    )
    async def delete_pipeline(params: Dict[str, Any]):
        logger.info(f"Deleting pipeline with params: {params}")
        try:
            # Validate required parameters
            if "pipeline_id" not in params:
                return missing_param_error("pipeline_id")
                
            pipeline_id = params.get("pipeline_id")
            
            result = await pipelines.delete_pipeline(pipeline_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "pipeline", params.get("pipeline_id"))
        except Exception as e:
            logger.error(f"Error deleting pipeline: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="start_pipeline_update",
        description=generate_tool_description(
            pipelines.start_pipeline_update,
            "POST",
            "/api/2.0/pipelines/{pipeline_id}/updates"
        ),
    )
    async def start_pipeline_update(params: Dict[str, Any]):
        logger.info(f"Starting pipeline update with params: {params}")
        try:
            # Validate required parameters
            if "pipeline_id" not in params:
                return missing_param_error("pipeline_id")
                
            # Start with required parameters
            api_params = {
                "pipeline_id": params.get("pipeline_id")
            }
            
            # Add optional parameters only if they exist
            if "full_refresh" in params and params["full_refresh"] is not None:
                api_params["full_refresh"] = params["full_refresh"]
            
            result = await pipelines.start_pipeline_update(**api_params)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "pipeline update", params.get("pipeline_id"))
        except Exception as e:
            logger.error(f"Error starting pipeline update: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_pipeline_update",
        description=generate_tool_description(
            pipelines.get_pipeline_update,
            "GET",
            "/api/2.0/pipelines/{pipeline_id}/updates/{update_id}"
        ),
    )
    async def get_pipeline_update(params: Dict[str, Any]):
        logger.info(f"Getting pipeline update with params: {params}")
        try:
            # Validate required parameters
            if "pipeline_id" not in params:
                return missing_param_error("pipeline_id")
            if "update_id" not in params:
                return missing_param_error("update_id")
                
            pipeline_id = params.get("pipeline_id")
            update_id = params.get("update_id")
            
            result = await pipelines.get_pipeline_update(pipeline_id, update_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "pipeline update", f"{params.get('pipeline_id')}/{params.get('update_id')}")
        except Exception as e:
            logger.error(f"Error getting pipeline update: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_pipeline_updates",
        description=generate_tool_description(
            pipelines.list_pipeline_updates,
            "GET",
            "/api/2.0/pipelines/{pipeline_id}/updates"
        ),
    )
    async def list_pipeline_updates(params: Dict[str, Any]):
        logger.info(f"Listing pipeline updates with params: {params}")
        try:
            # Validate required parameters
            if "pipeline_id" not in params:
                return missing_param_error("pipeline_id")
                
            pipeline_id = params.get("pipeline_id")
            max_results = params.get("max_results")
            page_token = params.get("page_token")
            
            result = await pipelines.list_pipeline_updates(pipeline_id, max_results, page_token)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "pipeline updates", params.get("pipeline_id"))
        except Exception as e:
            logger.error(f"Error listing pipeline updates: {str(e)}")
            return handle_api_error(e) 