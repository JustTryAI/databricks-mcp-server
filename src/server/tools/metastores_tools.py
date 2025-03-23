"""Tool implementations for Databricks Metastores API endpoints.

This module provides modularized tools for interacting with the Databricks Metastores API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.catalog import metastores
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_metastores_tools(server):
    """Register Metastores tools with the MCP server."""
    
    @server.tool(
        name="create_metastore",
        description=generate_tool_description(
            metastores.create_metastore,
            "POST",
            "/api/2.1/unity-catalog/metastores"
        ),
    )
    async def create_metastore(params: Dict[str, Any]):
        logger.info(f"Creating metastore with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
            if "storage_root" not in params:
                return missing_param_error("storage_root")
                
            name = params.get("name")
            storage_root = params.get("storage_root")
            region = params.get("region")
            
            result = await metastores.create_metastore(name, storage_root, region)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "metastore")
        except Exception as e:
            logger.error(f"Error creating metastore: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_metastores",
        description=generate_tool_description(
            metastores.list_metastores,
            "GET",
            "/api/2.1/unity-catalog/metastores"
        ),
    )
    async def list_metastores(params: Dict[str, Any]):
        logger.info("Listing metastores")
        try:
            result = await metastores.list_metastores()
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "metastores")
        except Exception as e:
            logger.error(f"Error listing metastores: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_metastore",
        description=generate_tool_description(
            metastores.get_metastore,
            "GET",
            "/api/2.1/unity-catalog/metastores/{id}"
        ),
    )
    async def get_metastore(params: Dict[str, Any]):
        logger.info(f"Getting metastore with params: {params}")
        try:
            # Validate required parameters
            if "id" not in params:
                return missing_param_error("id")
                
            metastore_id = params.get("id")
            
            result = await metastores.get_metastore(metastore_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "metastore", params.get("id"))
        except Exception as e:
            logger.error(f"Error getting metastore: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_metastore",
        description=generate_tool_description(
            metastores.update_metastore,
            "PATCH",
            "/api/2.1/unity-catalog/metastores/{id}"
        ),
    )
    async def update_metastore(params: Dict[str, Any]):
        logger.info(f"Updating metastore with params: {params}")
        try:
            # Validate required parameters
            if "id" not in params:
                return missing_param_error("id")
                
            metastore_id = params.get("id")
            updates = {k: v for k, v in params.items() if k != "id"}
            
            result = await metastores.update_metastore(metastore_id, updates)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "metastore", params.get("id"))
        except Exception as e:
            logger.error(f"Error updating metastore: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_metastore",
        description=generate_tool_description(
            metastores.delete_metastore,
            "DELETE",
            "/api/2.1/unity-catalog/metastores/{id}"
        ),
    )
    async def delete_metastore(params: Dict[str, Any]):
        logger.info(f"Deleting metastore with params: {params}")
        try:
            # Validate required parameters
            if "id" not in params:
                return missing_param_error("id")
                
            metastore_id = params.get("id")
            force = params.get("force", False)
            
            result = await metastores.delete_metastore(metastore_id, force)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "metastore", params.get("id"))
        except Exception as e:
            logger.error(f"Error deleting metastore: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="assign_metastore",
        description=generate_tool_description(
            metastores.assign_metastore,
            "PUT",
            "/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore"
        ),
    )
    async def assign_metastore(params: Dict[str, Any]):
        logger.info(f"Assigning metastore with params: {params}")
        try:
            # Validate required parameters
            if "workspace_id" not in params:
                return missing_param_error("workspace_id")
            if "metastore_id" not in params:
                return missing_param_error("metastore_id")
                
            workspace_id = params.get("workspace_id")
            metastore_id = params.get("metastore_id")
            default_catalog_name = params.get("default_catalog_name")
            
            result = await metastores.assign_metastore(workspace_id, metastore_id, default_catalog_name)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "metastore assignment")
        except Exception as e:
            logger.error(f"Error assigning metastore: {str(e)}")
            return handle_api_error(e)
            
    # The following tools are commented out because the corresponding functions
    # don't exist in the metastores.py module
    '''
    @server.tool(
        name="get_metastore_assignment",
        description=generate_tool_description(
            metastores.get_metastore_assignment,
            "GET",
            "/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore"
        ),
    )
    async def get_metastore_assignment(params: Dict[str, Any]):
        logger.info(f"Getting metastore assignment with params: {params}")
        try:
            # Validate required parameters
            if "workspace_id" not in params:
                return missing_param_error("workspace_id")
                
            workspace_id = params.get("workspace_id")
            
            result = await metastores.get_metastore_assignment(workspace_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "metastore assignment", params.get("workspace_id"))
        except Exception as e:
            logger.error(f"Error getting metastore assignment: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_metastore_assignment",
        description=generate_tool_description(
            metastores.update_metastore_assignment,
            "PATCH",
            "/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore"
        ),
    )
    async def update_metastore_assignment(params: Dict[str, Any]):
        logger.info(f"Updating metastore assignment with params: {params}")
        try:
            # Validate required parameters
            if "workspace_id" not in params:
                return missing_param_error("workspace_id")
                
            workspace_id = params.get("workspace_id")
            metastore_id = params.get("metastore_id")
            default_catalog_name = params.get("default_catalog_name")
            
            result = await metastores.update_metastore_assignment(
                workspace_id, metastore_id, default_catalog_name
            )
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "metastore assignment update", params.get("workspace_id"))
        except Exception as e:
            logger.error(f"Error updating metastore assignment: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_metastore_assignment",
        description=generate_tool_description(
            metastores.delete_metastore_assignment,
            "DELETE",
            "/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore"
        ),
    )
    async def delete_metastore_assignment(params: Dict[str, Any]):
        logger.info(f"Deleting metastore assignment with params: {params}")
        try:
            # Validate required parameters
            if "workspace_id" not in params:
                return missing_param_error("workspace_id")
                
            workspace_id = params.get("workspace_id")
            
            result = await metastores.delete_metastore_assignment(workspace_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "metastore assignment", params.get("workspace_id"))
        except Exception as e:
            logger.error(f"Error deleting metastore assignment: {str(e)}")
            return handle_api_error(e)
    '''
    
    # Add an unassign_metastore tool since it exists in the API
    @server.tool(
        name="unassign_metastore",
        description=generate_tool_description(
            metastores.unassign_metastore,
            "DELETE",
            "/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore"
        ),
    )
    async def unassign_metastore(params: Dict[str, Any]):
        logger.info(f"Unassigning metastore with params: {params}")
        try:
            # Validate required parameters
            if "workspace_id" not in params:
                return missing_param_error("workspace_id")
                
            workspace_id = params.get("workspace_id")
            
            result = await metastores.unassign_metastore(workspace_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "metastore unassignment", params.get("workspace_id"))
        except Exception as e:
            logger.error(f"Error unassigning metastore: {str(e)}")
            return handle_api_error(e) 