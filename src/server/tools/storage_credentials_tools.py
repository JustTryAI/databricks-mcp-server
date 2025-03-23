"""Tool implementations for Databricks Storage Credentials API endpoints.

This module provides modularized tools for interacting with the Databricks Storage Credentials API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.storage import storage_credentials
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_storage_credentials_tools(server):
    """Register Storage Credentials tools with the MCP server."""
    
    @server.tool(
        name="create_storage_credential",
        description=generate_tool_description(
            storage_credentials.create_storage_credential,
            "POST",
            "/api/2.1/unity-catalog/storage-credentials"
        ),
    )
    async def create_storage_credential(params: Dict[str, Any]):
        logger.info(f"Creating storage credential with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
            if "aws_iam_role" not in params and "azure_service_principal" not in params and "gcp_service_account_key" not in params:
                return missing_param_error("credential type (aws_iam_role, azure_service_principal, or gcp_service_account_key)")
                
            name = params.get("name")
            comment = params.get("comment", "")
            aws_iam_role = params.get("aws_iam_role")
            azure_service_principal = params.get("azure_service_principal")
            gcp_service_account_key = params.get("gcp_service_account_key")
            read_only = params.get("read_only", False)
            skip_validation = params.get("skip_validation", False)
            
            result = await storage_credentials.create_storage_credential(
                name, comment, aws_iam_role, azure_service_principal, 
                gcp_service_account_key, read_only, skip_validation
            )
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "storage credential")
        except Exception as e:
            logger.error(f"Error creating storage credential: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="list_storage_credentials",
        description=generate_tool_description(
            storage_credentials.list_storage_credentials,
            "GET",
            "/api/2.1/unity-catalog/storage-credentials"
        ),
    )
    async def list_storage_credentials(params: Dict[str, Any]):
        logger.info("Listing storage credentials")
        try:
            result = await storage_credentials.list_storage_credentials()
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "storage credentials")
        except Exception as e:
            logger.error(f"Error listing storage credentials: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_storage_credential",
        description=generate_tool_description(
            storage_credentials.get_storage_credential,
            "GET",
            "/api/2.1/unity-catalog/storage-credentials/{name}"
        ),
    )
    async def get_storage_credential(params: Dict[str, Any]):
        logger.info(f"Getting storage credential with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
                
            name = params.get("name")
            
            result = await storage_credentials.get_storage_credential(name)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "storage credential", params.get("name"))
        except Exception as e:
            logger.error(f"Error getting storage credential: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="update_storage_credential",
        description=generate_tool_description(
            storage_credentials.update_storage_credential,
            "PATCH",
            "/api/2.1/unity-catalog/storage-credentials/{name}"
        ),
    )
    async def update_storage_credential(params: Dict[str, Any]):
        logger.info(f"Updating storage credential with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
                
            name = params.get("name")
            updates = {k: v for k, v in params.items() if k != "name"}
            
            result = await storage_credentials.update_storage_credential(name, updates)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "storage credential", params.get("name"))
        except Exception as e:
            logger.error(f"Error updating storage credential: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="delete_storage_credential",
        description=generate_tool_description(
            storage_credentials.delete_storage_credential,
            "DELETE",
            "/api/2.1/unity-catalog/storage-credentials/{name}"
        ),
    )
    async def delete_storage_credential(params: Dict[str, Any]):
        logger.info(f"Deleting storage credential with params: {params}")
        try:
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
                
            name = params.get("name")
            force = params.get("force", False)
            
            result = await storage_credentials.delete_storage_credential(name, force)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "storage credential", params.get("name"))
        except Exception as e:
            logger.error(f"Error deleting storage credential: {str(e)}")
            return handle_api_error(e)
    
    # The following tool is commented out because the corresponding function
    # doesn't exist in the storage_credentials.py module
    '''
    @server.tool(
        name="validate_storage_credential",
        description=generate_tool_description(
            storage_credentials.validate_storage_credential,
            "POST",
            "/api/2.1/unity-catalog/storage-credentials/validate"
        ),
    )
    async def validate_storage_credential(params: Dict[str, Any]):
        logger.info(f"Validating storage credential with params: {params}")
        try:
            # Validate required parameters
            if "storage_credential_name" not in params and "external_location_name" not in params:
                return missing_param_error("either storage_credential_name or external_location_name")
                
            storage_credential_name = params.get("storage_credential_name")
            external_location_name = params.get("external_location_name")
            url = params.get("url")
            
            result = await storage_credentials.validate_storage_credential(
                storage_credential_name, external_location_name, url
            )
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "storage credential validation")
        except Exception as e:
            logger.error(f"Error validating storage credential: {str(e)}")
            return handle_api_error(e)
    ''' 