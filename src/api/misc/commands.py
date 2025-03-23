"""
API for executing commands on Databricks clusters.
"""

import logging
from typing import Any, Dict, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def create_context(
    cluster_id: str,
    language: str = "python",
) -> Dict[str, Any]:
    """
    Create a command execution context on a Databricks cluster.
    
    Args:
        cluster_id: ID of the cluster to create a context on
        language: Programming language for the context (python, scala, sql, r)
        
    Returns:
        Response containing the context ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating {language} context on cluster {cluster_id}")
    
    data = {
        "clusterId": cluster_id,
        "language": language,
    }
    
    try:
        return await make_api_request("POST", "/api/1.2/contexts/create", data=data)
    except Exception as e:
        logger.error(f"Failed to create context: {str(e)}")
        raise DatabricksAPIError(f"Failed to create context: {str(e)}")


async def execute_command(
    context_id: str,
    command: str,
) -> Dict[str, Any]:
    """
    Execute a command in a context.
    
    Args:
        context_id: ID of the context to execute the command in
        command: The command to execute
        
    Returns:
        Response containing the command ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Executing command in context {context_id}")
    
    data = {
        "contextId": context_id,
        "command": command,
    }
    
    try:
        return await make_api_request("POST", "/api/1.2/commands/execute", data=data)
    except Exception as e:
        logger.error(f"Failed to execute command: {str(e)}")
        raise DatabricksAPIError(f"Failed to execute command: {str(e)}")


async def get_command_status(command_id: str) -> Dict[str, Any]:
    """
    Get the status of a command.
    
    Args:
        command_id: ID of the command to get status for
        
    Returns:
        Response containing the command status
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting status of command {command_id}")
    try:
        return await make_api_request("GET", f"/api/1.2/commands/status?commandId={command_id}")
    except Exception as e:
        logger.error(f"Failed to get command status: {str(e)}")
        raise DatabricksAPIError(f"Failed to get command status: {str(e)}")


async def cancel_command(command_id: str) -> Dict[str, Any]:
    """
    Cancel a command.
    
    Args:
        command_id: ID of the command to cancel
        
    Returns:
        Response indicating if the cancellation was successful
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Cancelling command {command_id}")
    try:
        return await make_api_request("POST", f"/api/1.2/commands/cancel?commandId={command_id}")
    except Exception as e:
        logger.error(f"Failed to cancel command: {str(e)}")
        raise DatabricksAPIError(f"Failed to cancel command: {str(e)}") 