"""Tool implementations for Databricks Command Execution API endpoints.

This module provides modularized tools for interacting with the Databricks Command Execution API.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.api.misc import commands
from src.core.utils import DatabricksAPIError
from src.server.tools.common import (
    generate_tool_description,
    handle_api_error,
    missing_param_error,
    success_response
)

# Set up logger
logger = logging.getLogger(__name__)

def register_commands_tools(server):
    """Register Command Execution tools with the MCP server."""
    
    @server.tool(
        name="create_command_context",
        description=generate_tool_description(
            commands.create_context,
            "POST",
            "/api/1.2/contexts/create"
        ),
    )
    async def create_command_context(params: Dict[str, Any]):
        logger.info(f"Creating command context with params: {params}")
        try:
            # Validate required parameters
            if "language" not in params:
                return missing_param_error("language")
            if "cluster_id" not in params:
                return missing_param_error("cluster_id")
                
            language = params.get("language")
            cluster_id = params.get("cluster_id")
            
            result = await commands.create_context(language, cluster_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "command context")
        except Exception as e:
            logger.error(f"Error creating command context: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="execute_command",
        description=generate_tool_description(
            commands.execute_command,
            "POST",
            "/api/1.2/commands/execute"
        ),
    )
    async def execute_command(params: Dict[str, Any]):
        logger.info(f"Executing command with params: {params}")
        try:
            # Validate required parameters
            if "context_id" not in params:
                return missing_param_error("context_id")
            if "command" not in params:
                return missing_param_error("command")
                
            context_id = params.get("context_id")
            command = params.get("command")
            language = params.get("language")
            
            result = await commands.execute_command(context_id, command, language)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "command execution")
        except Exception as e:
            logger.error(f"Error executing command: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="get_command_status",
        description=generate_tool_description(
            commands.get_command_status,
            "GET",
            "/api/1.2/commands/status"
        ),
    )
    async def get_command_status(params: Dict[str, Any]):
        logger.info(f"Getting command status with params: {params}")
        try:
            # Validate required parameters
            if "command_id" not in params:
                return missing_param_error("command_id")
                
            command_id = params.get("command_id")
            
            result = await commands.get_command_status(command_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "command", params.get("command_id"))
        except Exception as e:
            logger.error(f"Error getting command status: {str(e)}")
            return handle_api_error(e)
    
    @server.tool(
        name="cancel_command",
        description=generate_tool_description(
            commands.cancel_command,
            "POST",
            "/api/1.2/commands/cancel"
        ),
    )
    async def cancel_command(params: Dict[str, Any]):
        logger.info(f"Cancelling command with params: {params}")
        try:
            # Validate required parameters
            if "command_id" not in params:
                return missing_param_error("command_id")
                
            command_id = params.get("command_id")
            
            result = await commands.cancel_command(command_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "command", params.get("command_id"))
        except Exception as e:
            logger.error(f"Error cancelling command: {str(e)}")
            return handle_api_error(e) 