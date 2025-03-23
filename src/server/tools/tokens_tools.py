"""Tool implementations for Databricks Token API endpoints.

This module provides modularized tools for interacting with the Databricks Token API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.iam import tokens
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_tokens_tools(server):
    """Register Token tools with the MCP server."""
    
    @server.tool(
        name="list_tokens",
        description=generate_tool_description(
            tokens.list_tokens,
            "GET",
            "/api/2.0/token/list"
        ),
    )
    async def list_tokens(params: Dict[str, Any]):
        logger.info("Listing tokens")
        try:
            result = await tokens.list_tokens()
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "tokens")
        except Exception as e:
            logger.error(f"Error listing tokens: {str(e)}")
            return handle_api_error(e)

    @server.tool(
        name="create_token",
        description=generate_tool_description(
            tokens.create_token,
            "POST",
            "/api/2.0/token/create"
        ),
    )
    async def create_token(params: Dict[str, Any]):
        logger.info(f"Creating token with params: {params}")
        try:
            comment = params.get("comment", "")
            lifetime_seconds = params.get("lifetime_seconds")
            
            result = await tokens.create_token(comment, lifetime_seconds)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "token")
        except Exception as e:
            logger.error(f"Error creating token: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="revoke_token",
        description=generate_tool_description(
            tokens.revoke_token,
            "POST",
            "/api/2.0/token/delete"
        ),
    )
    async def revoke_token(params: Dict[str, Any]):
        logger.info(f"Revoking token with params: {params}")
        try:
            # Validate required parameters
            if "token_id" not in params:
                return missing_param_error("token_id")
                
            token_id = params.get("token_id")
            
            result = await tokens.revoke_token(token_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "token", params.get("token_id"))
        except Exception as e:
            logger.error(f"Error revoking token: {str(e)}")
            return handle_api_error(e) 