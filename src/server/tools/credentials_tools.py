"""Tool implementations for Databricks Credentials API endpoints.

This module provides modularized tools for interacting with the Databricks Credentials API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.iam import credentials
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_credentials_tools(server):
    """Register Credentials tools with the MCP server."""
    
    @server.tool(
        name="create_credential",
        description=generate_tool_description(
            credentials.create_credentials,
            "POST",
            "/api/2.0/accounts/credentials"
        ),
    )
    async def create_credential(params: Dict[str, Any]):
        logger.info(f"Creating credential with params: {params}")
        try:
            # Validate required parameters
            if "credential_name" not in params:
                return missing_param_error("credential_name")
            if "credential_type" not in params:
                return missing_param_error("credential_type")
                
            credential_name = params.get("credential_name")
            credential_type = params.get("credential_type")
            aws_credentials = params.get("aws_credentials")
            azure_credentials = params.get("azure_credentials")
            gcp_credentials = params.get("gcp_credentials")
            
            result = await credentials.create_credentials(
                credential_name, credential_type, aws_credentials, 
                azure_credentials, gcp_credentials
            )
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "credential")
        except Exception as e:
            logger.error(f"Error creating credential: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_credentials",
        description=generate_tool_description(
            credentials.list_credentials,
            "GET",
            "/api/2.0/accounts/credentials"
        ),
    )
    async def list_credentials(params: Dict[str, Any]):
        logger.info("Listing credentials")
        try:
            result = await credentials.list_credentials()
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "credentials")
        except Exception as e:
            logger.error(f"Error listing credentials: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_credential",
        description=generate_tool_description(
            credentials.get_credentials,
            "GET",
            "/api/2.0/accounts/credentials/{credential_id}"
        ),
    )
    async def get_credential(params: Dict[str, Any]):
        logger.info(f"Getting credential with params: {params}")
        try:
            # Validate required parameters
            if "credential_id" not in params:
                return missing_param_error("credential_id")
                
            credential_id = params.get("credential_id")
            
            result = await credentials.get_credentials(credential_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "credential", params.get("credential_id"))
        except Exception as e:
            logger.error(f"Error getting credential: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_credential",
        description=generate_tool_description(
            credentials.update_credentials,
            "PATCH",
            "/api/2.0/accounts/credentials/{credential_id}"
        ),
    )
    async def update_credential(params: Dict[str, Any]):
        logger.info(f"Updating credential with params: {params}")
        try:
            # Validate required parameters
            if "credential_id" not in params:
                return missing_param_error("credential_id")
                
            credential_id = params.get("credential_id")
            aws_credentials = params.get("aws_credentials")
            azure_credentials = params.get("azure_credentials")
            gcp_credentials = params.get("gcp_credentials")
            
            result = await credentials.update_credentials(
                credential_id, aws_credentials, azure_credentials, gcp_credentials
            )
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "credential", params.get("credential_id"))
        except Exception as e:
            logger.error(f"Error updating credential: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_credential",
        description=generate_tool_description(
            credentials.delete_credentials,
            "DELETE",
            "/api/2.0/accounts/credentials/{credential_id}"
        ),
    )
    async def delete_credential(params: Dict[str, Any]):
        logger.info(f"Deleting credential with params: {params}")
        try:
            # Validate required parameters
            if "credential_id" not in params:
                return missing_param_error("credential_id")
                
            credential_id = params.get("credential_id")
            
            result = await credentials.delete_credentials(credential_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "credential", params.get("credential_id"))
        except Exception as e:
            logger.error(f"Error deleting credential: {str(e)}")
            return handle_api_error(e) 