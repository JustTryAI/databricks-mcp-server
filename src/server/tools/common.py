"""Common utilities for Databricks MCP server tools.

This module provides shared functionality used across multiple tool modules.
"""
import json
import logging
from typing import Any, Dict, List, Optional

# Temporarily removing fastmcp dependency
# from fastmcp.content import TextContent
# from fastmcp.tool_call import CallToolResult

from src.core.utils import DatabricksAPIError

# Set up logger
logger = logging.getLogger(__name__)


def generate_tool_description(func, method, endpoint):
    """Generate a standardized tool description for Databricks API endpoints.
    
    Args:
        func: The API function being wrapped
        method: The HTTP method (GET, POST, etc.)
        endpoint: The API endpoint path
        
    Returns:
        str: A formatted description string
    """
    doc = func.__doc__ or ""
    description = f"{method} {endpoint}\n{doc}"
    return description


def handle_api_error(e: DatabricksAPIError, resource_type: str, resource_id: Optional[str] = None):
    """Handle common API error patterns and return appropriate tool call results.
    
    Args:
        e: The DatabricksAPIError that was raised
        resource_type: The type of resource (e.g., "cluster", "job")
        resource_id: Optional identifier for the resource
        
    Returns:
        Dict: Formatted error result for the tool call
    """
    resource_desc = f"{resource_type} {resource_id}" if resource_id else resource_type
    
    if e.status_code == 404 or "RESOURCE_DOES_NOT_EXIST" in str(e):
        logger.warning(f"{resource_desc.capitalize()} not found")
        return {
            "isError": True,
            "error": f"{resource_desc.capitalize()} not found"
        }
    else:
        logger.error(f"Failed to process {resource_desc}: {e}")
        return {
            "isError": True,
            "error": f"Failed to process {resource_desc}: {e}"
        }


def missing_param_error(param_name: str):
    """Create an error response for a missing required parameter.
    
    Args:
        param_name: The name of the missing parameter
        
    Returns:
        Dict: Formatted error result for the tool call
    """
    logger.error(f"Missing required parameter: {param_name}")
    return {
        "isError": True,
        "error": f"Missing required parameter: {param_name}"
    }


def success_response(result: Any):
    """Create a success response with the result.
    
    Args:
        result: The result data to return
        
    Returns:
        Dict: Formatted success result
    """
    return {
        "isError": False, 
        "result": result
    } 