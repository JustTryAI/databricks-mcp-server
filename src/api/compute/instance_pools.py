"""
API for managing Databricks instance pools.

This module provides functions for interacting with the Databricks Instance Pools API.
It is part of the compute API group that includes clusters, instance pools, and libraries.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

async def create_instance_pool(
    instance_pool_name: str,
    node_type_id: str,
    min_idle_instances: int = 0,
    max_capacity: Optional[int] = None,
    idle_instance_autotermination_minutes: Optional[int] = None,
    enable_elastic_disk: Optional[bool] = None,
    disk_spec: Optional[Dict[str, Any]] = None,
    preloaded_spark_versions: Optional[List[str]] = None,
    custom_tags: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Create a new Databricks instance pool.
    
    Args:
        instance_pool_name: The name of the instance pool
        node_type_id: The node type to use for the instances in the pool
        min_idle_instances: The minimum number of idle instances to maintain
        max_capacity: The maximum number of instances the pool can contain
        idle_instance_autotermination_minutes: Time before idle instances are terminated
        enable_elastic_disk: Whether to enable elastic disk on the instances
        disk_spec: Specifications for the disk to use on the instances
        preloaded_spark_versions: Spark versions to preload on the instances
        custom_tags: Custom tags to apply to the instances
        **kwargs: Additional parameters to pass to the API
        
    Returns:
        Response containing the instance pool ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating instance pool: {instance_pool_name}")
    
    data = {
        "instance_pool_name": instance_pool_name,
        "node_type_id": node_type_id,
        "min_idle_instances": min_idle_instances,
    }
    
    # Add optional parameters
    optional_params = {
        "max_capacity": max_capacity,
        "idle_instance_autotermination_minutes": idle_instance_autotermination_minutes,
        "enable_elastic_disk": enable_elastic_disk,
        "disk_spec": disk_spec,
        "preloaded_spark_versions": preloaded_spark_versions,
        "custom_tags": custom_tags,
    }
    
    for key, value in optional_params.items():
        if value is not None:
            data[key] = value
    
    # Add any additional parameters
    for key, value in kwargs.items():
        data[key] = value
    
    return make_api_request("POST", "/api/2.0/instance-pools/create", data=data)


async def get_instance_pool(instance_pool_id: str) -> Dict[str, Any]:
    """
    Get information about a specific instance pool.
    
    Args:
        instance_pool_id: The ID of the instance pool
        
    Returns:
        Response containing the instance pool information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting instance pool info for {instance_pool_id}")
    return make_api_request(
        "GET", 
        "/api/2.0/instance-pools/get", 
        params={"instance_pool_id": instance_pool_id}
    )


async def list_instance_pools() -> Dict[str, Any]:
    """
    List all instance pools in the workspace.
    
    Returns:
        Response containing the list of instance pools
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing instance pools")
    return make_api_request("GET", "/api/2.0/instance-pools/list")


async def edit_instance_pool(
    instance_pool_id: str,
    instance_pool_name: Optional[str] = None,
    min_idle_instances: Optional[int] = None,
    max_capacity: Optional[int] = None,
    idle_instance_autotermination_minutes: Optional[int] = None,
    custom_tags: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Edit an existing instance pool.
    
    Args:
        instance_pool_id: The ID of the instance pool to edit
        instance_pool_name: New name for the instance pool
        min_idle_instances: New minimum number of idle instances
        max_capacity: New maximum capacity for the pool
        idle_instance_autotermination_minutes: New autotermination time
        custom_tags: New custom tags for the instances
        **kwargs: Additional parameters to update
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Editing instance pool {instance_pool_id}")
    
    data = {"instance_pool_id": instance_pool_id}
    
    # Add optional parameters
    optional_params = {
        "instance_pool_name": instance_pool_name,
        "min_idle_instances": min_idle_instances,
        "max_capacity": max_capacity,
        "idle_instance_autotermination_minutes": idle_instance_autotermination_minutes,
        "custom_tags": custom_tags,
    }
    
    for key, value in optional_params.items():
        if value is not None:
            data[key] = value
    
    # Add any additional parameters
    for key, value in kwargs.items():
        data[key] = value
    
    return make_api_request("POST", "/api/2.0/instance-pools/edit", data=data)


async def delete_instance_pool(instance_pool_id: str) -> Dict[str, Any]:
    """
    Delete an instance pool.
    
    Args:
        instance_pool_id: The ID of the instance pool to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting instance pool {instance_pool_id}")
    return make_api_request(
        "POST", 
        "/api/2.0/instance-pools/delete", 
        data={"instance_pool_id": instance_pool_id}
    ) 