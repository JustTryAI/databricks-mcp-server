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
    logger.info(f"Creating command context on cluster: {cluster_id} for language: {language}")
    
    data = {
        "clusterId": cluster_id,
        "language": language,
    }
    
    return make_api_request("POST", "/api/1.2/contexts/create", data=data)


async def execute_command(
    context_id: str,
    command: str,
) -> Dict[str, Any]:
    """
    Execute a command in a given context.
    
    Args:
        context_id: ID of the context to execute the command in
        command: Command to execute
        
    Returns:
        Response containing the command ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Executing command in context: {context_id}")
    
    data = {
        "contextId": context_id,
        "command": command,
    }
    
    return make_api_request("POST", "/api/1.2/commands/execute", data=data)


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
    logger.info(f"Getting status for command: {command_id}")
    return make_api_request("GET", f"/api/1.2/commands/status?commandId={command_id}")


async def cancel_command(command_id: str) -> Dict[str, Any]:
    """
    Cancel a running command.
    
    Args:
        command_id: ID of the command to cancel
        
    Returns:
        Response indicating success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Cancelling command: {command_id}")
    return make_api_request("POST", f"/api/1.2/commands/cancel", data={"commandId": command_id}) 