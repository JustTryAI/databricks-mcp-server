"""
API for managing Databricks instance profiles.

This module provides functions for interacting with the Databricks Instance Profiles API.
It is part of the compute API group that includes clusters, cluster policies, and libraries.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def add_instance_profile(
    instance_profile_arn: str,
    skip_validation: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Register an instance profile for use with Databricks clusters.
    
    Args:
        instance_profile_arn: The ARN of the instance profile to register
        skip_validation: Optional flag to skip validation of the instance profile
        
    Returns:
        Empty response if successful
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Adding instance profile: {instance_profile_arn}")
    
    data = {
        "instance_profile_arn": instance_profile_arn,
    }
    
    if skip_validation is not None:
        data["skip_validation"] = skip_validation
    
    try:
        return await make_api_request("POST", "/api/2.0/instance-profiles/add", data=data)
    except Exception as e:
        logger.error(f"Failed to add instance profile: {str(e)}")
        raise DatabricksAPIError(f"Failed to add instance profile: {str(e)}")


async def list_instance_profiles() -> Dict[str, Any]:
    """
    List all available instance profiles that can be used with Databricks clusters.
    
    Returns:
        Response containing a list of instance profiles
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing instance profiles")
    
    try:
        return await make_api_request("GET", "/api/2.0/instance-profiles/list")
    except Exception as e:
        logger.error(f"Failed to list instance profiles: {str(e)}")
        raise DatabricksAPIError(f"Failed to list instance profiles: {str(e)}")


async def remove_instance_profile(instance_profile_arn: str) -> Dict[str, Any]:
    """
    Remove an instance profile from Databricks.
    
    Args:
        instance_profile_arn: The ARN of the instance profile to remove
        
    Returns:
        Empty response if successful
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Removing instance profile: {instance_profile_arn}")
    
    data = {
        "instance_profile_arn": instance_profile_arn,
    }
    
    try:
        return await make_api_request("POST", "/api/2.0/instance-profiles/remove", data=data)
    except Exception as e:
        logger.error(f"Failed to remove instance profile: {str(e)}")
        raise DatabricksAPIError(f"Failed to remove instance profile: {str(e)}")


async def get_instance_profile_permissions(instance_profile_arn: str) -> Dict[str, Any]:
    """
    Get permissions for an instance profile.
    
    Args:
        instance_profile_arn: The ARN of the instance profile
        
    Returns:
        Response containing the permissions details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting permissions for instance profile: {instance_profile_arn}")
    
    # Encode the ARN for use in the URL
    encoded_arn = instance_profile_arn.replace('/', '%2F')
    
    try:
        return await make_api_request("GET", f"/api/2.0/permissions/instance-profiles/{encoded_arn}")
    except Exception as e:
        logger.error(f"Failed to get instance profile permissions: {str(e)}")
        raise DatabricksAPIError(f"Failed to get instance profile permissions: {str(e)}")


async def update_instance_profile_permissions(
    instance_profile_arn: str,
    access_control_list: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Update permissions for an instance profile.
    
    Args:
        instance_profile_arn: The ARN of the instance profile
        access_control_list: List of access control items
        
    Returns:
        Response containing the updated permissions details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating permissions for instance profile: {instance_profile_arn}")
    
    # Encode the ARN for use in the URL
    encoded_arn = instance_profile_arn.replace('/', '%2F')
    
    data = {
        "access_control_list": access_control_list
    }
    
    try:
        return await make_api_request("PATCH", f"/api/2.0/permissions/instance-profiles/{encoded_arn}", data=data)
    except Exception as e:
        logger.error(f"Failed to update instance profile permissions: {str(e)}")
        raise DatabricksAPIError(f"Failed to update instance profile permissions: {str(e)}") 