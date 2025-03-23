"""Tool implementations for Databricks Visualization API endpoints.

This module provides modularized tools for interacting with the Databricks Visualization API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.sql import visualizations
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_visualizations_tools(server):
    """Register Visualizations tools with the MCP server."""
    
    @server.tool(
        name="create_visualization",
        description=generate_tool_description(
            visualizations.create_visualization,
            "POST",
            "/api/2.0/preview/sql/visualizations"
        ),
    )
    async def create_visualization(params: Dict[str, Any]):
        logger.info(f"Creating visualization with params: {params}")
        try:
            # Validate required parameters
            if "query_id" not in params:
                return missing_param_error("query_id")
            if "type" not in params:
                return missing_param_error("type")
            if "name" not in params:
                return missing_param_error("name")
            if "options" not in params:
                return missing_param_error("options")
                
            query_id = params.get("query_id")
            visualization_type = params.get("type")
            name = params.get("name")
            options = params.get("options")
            
            # Extract additional kwargs
            kwargs = {k: v for k, v in params.items() 
                     if k not in ["query_id", "type", "name", "options"]}
            
            result = await visualizations.create_visualization(
                query_id=query_id,
                type=visualization_type,
                name=name,
                options=options,
                **kwargs
            )
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "visualization")
        except Exception as e:
            logger.error(f"Error creating visualization: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_visualization",
        description=generate_tool_description(
            visualizations.update_visualization,
            "POST",
            "/api/2.0/preview/sql/visualizations/{visualization_id}"
        ),
    )
    async def update_visualization(params: Dict[str, Any]):
        logger.info(f"Updating visualization with params: {params}")
        try:
            # Validate required parameters
            if "id" not in params:
                return missing_param_error("id")
                
            visualization_id = params.get("id")
            updates = {k: v for k, v in params.items() if k != "id"}
            
            result = await visualizations.update_visualization(
                visualization_id=visualization_id,
                updates=updates
            )
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "visualization", params.get("id"))
        except Exception as e:
            logger.error(f"Error updating visualization: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_visualization",
        description=generate_tool_description(
            visualizations.delete_visualization,
            "DELETE",
            "/api/2.0/preview/sql/visualizations/{visualization_id}"
        ),
    )
    async def delete_visualization(params: Dict[str, Any]):
        logger.info(f"Deleting visualization with params: {params}")
        try:
            # Validate required parameters
            if "id" not in params:
                return missing_param_error("id")
                
            visualization_id = params.get("id")
            
            result = await visualizations.delete_visualization(
                visualization_id=visualization_id
            )
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "visualization", params.get("id"))
        except Exception as e:
            logger.error(f"Error deleting visualization: {str(e)}")
            return handle_api_error(e) 