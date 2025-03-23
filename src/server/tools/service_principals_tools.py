"""Tool implementations for Databricks Service Principals API endpoints.

This module provides modularized tools for interacting with the Databricks Service Principals API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.iam import service_principals
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_service_principals_tools(server):
    """Register Service Principals tools with the MCP server."""
    
    @server.tool(
        name="create_service_principal",
        description=generate_tool_description(
            service_principals.create_service_principal,
            "POST",
            "/api/2.0/preview/scim/v2/ServicePrincipals"
        ),
    )
    async def create_service_principal(params: Dict[str, Any]):
        logger.info(f"Creating service principal with params: {params}")
        try:
            # Validate required parameters
            if "display_name" not in params:
                return missing_param_error("display_name")
                
            display_name = params.get("display_name")
            application_id = params.get("application_id")
            
            result = await service_principals.create_service_principal(display_name, application_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "service principal")
        except Exception as e:
            logger.error(f"Error creating service principal: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_service_principals",
        description=generate_tool_description(
            service_principals.list_service_principals,
            "GET",
            "/api/2.0/preview/scim/v2/ServicePrincipals"
        ),
    )
    async def list_service_principals(params: Dict[str, Any]):
        logger.info("Listing service principals")
        try:
            filter_params = params.get("filter")
            attributes = params.get("attributes")
            
            result = await service_principals.list_service_principals(filter_params, attributes)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "service principals")
        except Exception as e:
            logger.error(f"Error listing service principals: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_service_principal",
        description=generate_tool_description(
            service_principals.get_service_principal,
            "GET",
            "/api/2.0/preview/scim/v2/ServicePrincipals/{id}"
        ),
    )
    async def get_service_principal(params: Dict[str, Any]):
        logger.info(f"Getting service principal with params: {params}")
        try:
            # Validate required parameters
            if "id" not in params:
                return missing_param_error("id")
                
            sp_id = params.get("id")
            
            result = await service_principals.get_service_principal(sp_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "service principal", params.get("id"))
        except Exception as e:
            logger.error(f"Error getting service principal: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_service_principal",
        description=generate_tool_description(
            service_principals.update_service_principal,
            "PATCH",
            "/api/2.0/preview/scim/v2/ServicePrincipals/{id}"
        ),
    )
    async def update_service_principal(params: Dict[str, Any]):
        logger.info(f"Updating service principal with params: {params}")
        try:
            # Validate required parameters
            if "id" not in params:
                return missing_param_error("id")
                
            sp_id = params.get("id")
            operations = params.get("operations", [])
            
            result = await service_principals.update_service_principal(sp_id, operations)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "service principal", params.get("id"))
        except Exception as e:
            logger.error(f"Error updating service principal: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_service_principal",
        description=generate_tool_description(
            service_principals.delete_service_principal,
            "DELETE",
            "/api/2.0/preview/scim/v2/ServicePrincipals/{id}"
        ),
    )
    async def delete_service_principal(params: Dict[str, Any]):
        logger.info(f"Deleting service principal with params: {params}")
        try:
            # Validate required parameters
            if "id" not in params:
                return missing_param_error("id")
                
            sp_id = params.get("id")
            
            result = await service_principals.delete_service_principal(sp_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "service principal", params.get("id"))
        except Exception as e:
            logger.error(f"Error deleting service principal: {str(e)}")
            return handle_api_error(e) 