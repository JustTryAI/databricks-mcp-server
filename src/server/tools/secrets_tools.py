"""Tool implementations for Databricks Secrets API endpoints.

This module provides modularized tools for interacting with the Databricks Secrets API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.iam import secrets
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_secrets_tools(server):
    """Register Secrets tools with the MCP server."""
    
    @server.tool(
        name="list_secret_scopes",
        description=generate_tool_description(
            secrets.list_scopes,
            "GET",
            "/api/2.0/secrets/scopes/list"
        ),
    )
    async def list_secret_scopes(params: Dict[str, Any]):
        logger.info("Listing secret scopes")
        try:
            result = await secrets.list_scopes()
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "secret scopes")
        except Exception as e:
            logger.error(f"Error listing secret scopes: {str(e)}")
            return handle_api_error(e)

    @server.tool(
        name="create_secret_scope",
        description=generate_tool_description(
            secrets.create_scope,
            "POST",
            "/api/2.0/secrets/scopes/create"
        ),
    )
    async def create_secret_scope(params: Dict[str, Any]):
        logger.info(f"Creating secret scope with params: {params}")
        try:
            # Validate required parameters
            if "scope" not in params:
                return missing_param_error("scope")
                
            scope = params.get("scope")
            initial_manage_principal = params.get("initial_manage_principal")
            
            result = await secrets.create_scope(scope, initial_manage_principal)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "secret scope", params.get("scope"))
        except Exception as e:
            logger.error(f"Error creating secret scope: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_secret_scope",
        description=generate_tool_description(
            secrets.delete_scope,
            "POST",
            "/api/2.0/secrets/scopes/delete"
        ),
    )
    async def delete_secret_scope(params: Dict[str, Any]):
        logger.info(f"Deleting secret scope with params: {params}")
        try:
            # Validate required parameters
            if "scope" not in params:
                return missing_param_error("scope")
                
            scope = params.get("scope")
            
            result = await secrets.delete_scope(scope)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "secret scope", params.get("scope"))
        except Exception as e:
            logger.error(f"Error deleting secret scope: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_secrets",
        description=generate_tool_description(
            secrets.list_secrets,
            "GET",
            "/api/2.0/secrets/list"
        ),
    )
    async def list_secrets(params: Dict[str, Any]):
        logger.info(f"Listing secrets with params: {params}")
        try:
            # Validate required parameters
            if "scope" not in params:
                return missing_param_error("scope")
                
            scope = params.get("scope")
            
            result = await secrets.list_secrets(scope)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "secrets", params.get("scope"))
        except Exception as e:
            logger.error(f"Error listing secrets: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="put_secret",
        description=generate_tool_description(
            secrets.put_secret,
            "POST",
            "/api/2.0/secrets/put"
        ),
    )
    async def put_secret(params: Dict[str, Any]):
        logger.info(f"Putting secret with params: {params}")
        try:
            # Validate required parameters
            if "scope" not in params:
                return missing_param_error("scope")
            if "key" not in params:
                return missing_param_error("key")
            if "string_value" not in params:
                return missing_param_error("string_value")
                
            scope = params.get("scope")
            key = params.get("key")
            string_value = params.get("string_value")
            
            result = await secrets.put_secret(scope, key, string_value)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "secret", f"{params.get('scope')}/{params.get('key')}")
        except Exception as e:
            logger.error(f"Error putting secret: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_secret",
        description=generate_tool_description(
            secrets.delete_secret,
            "POST",
            "/api/2.0/secrets/delete"
        ),
    )
    async def delete_secret(params: Dict[str, Any]):
        logger.info(f"Deleting secret with params: {params}")
        try:
            # Validate required parameters
            if "scope" not in params:
                return missing_param_error("scope")
            if "key" not in params:
                return missing_param_error("key")
                
            scope = params.get("scope")
            key = params.get("key")
            
            result = await secrets.delete_secret(scope, key)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "secret", f"{params.get('scope')}/{params.get('key')}")
        except Exception as e:
            logger.error(f"Error deleting secret: {str(e)}")
            return handle_api_error(e) 