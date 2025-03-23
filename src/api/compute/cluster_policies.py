"""
API for managing Databricks cluster policies.

This module provides functions for interacting with the Databricks Cluster Policies API.
It is part of the compute API group that includes clusters, libraries, and instance pools.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def create_cluster_policy(
    name: str,
    definition: Dict[str, Any],
    description: Optional[str] = None,
    max_clusters_per_user: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Create a new cluster policy.
    
    Args:
        name: The name of the cluster policy
        definition: JSON definition of the cluster policy
        description: Optional description for the cluster policy
        max_clusters_per_user: Optional maximum clusters per user
        
    Returns:
        Response containing the created cluster policy details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating cluster policy: {name}")
    
    data = {
        "name": name,
        "definition": definition,
    }
    
    if description is not None:
        data["description"] = description
    
    if max_clusters_per_user is not None:
        data["max_clusters_per_user"] = max_clusters_per_user
    
    try:
        return await make_api_request("POST", "/api/2.0/policies/clusters/create", data=data)
    except Exception as e:
        logger.error(f"Failed to create cluster policy: {str(e)}")
        raise DatabricksAPIError(f"Failed to create cluster policy: {str(e)}")


async def edit_cluster_policy(
    policy_id: str,
    name: Optional[str] = None,
    definition: Optional[Dict[str, Any]] = None,
    description: Optional[str] = None,
    max_clusters_per_user: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Edit an existing cluster policy.
    
    Args:
        policy_id: ID of the cluster policy to edit
        name: Optional new name for the cluster policy
        definition: Optional JSON definition of the cluster policy
        description: Optional description for the cluster policy
        max_clusters_per_user: Optional maximum clusters per user
        
    Returns:
        Response containing the updated cluster policy details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Editing cluster policy: {policy_id}")
    
    data = {
        "policy_id": policy_id,
    }
    
    if name is not None:
        data["name"] = name
    
    if definition is not None:
        data["definition"] = definition
    
    if description is not None:
        data["description"] = description
    
    if max_clusters_per_user is not None:
        data["max_clusters_per_user"] = max_clusters_per_user
    
    try:
        return await make_api_request("POST", "/api/2.0/policies/clusters/edit", data=data)
    except Exception as e:
        logger.error(f"Failed to edit cluster policy: {str(e)}")
        raise DatabricksAPIError(f"Failed to edit cluster policy: {str(e)}")


async def delete_cluster_policy(policy_id: str) -> Dict[str, Any]:
    """
    Delete a cluster policy.
    
    Args:
        policy_id: ID of the cluster policy to delete
        
    Returns:
        Empty response if successful
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting cluster policy: {policy_id}")
    
    data = {
        "policy_id": policy_id,
    }
    
    try:
        return await make_api_request("POST", "/api/2.0/policies/clusters/delete", data=data)
    except Exception as e:
        logger.error(f"Failed to delete cluster policy: {str(e)}")
        raise DatabricksAPIError(f"Failed to delete cluster policy: {str(e)}")


async def get_cluster_policy(policy_id: str) -> Dict[str, Any]:
    """
    Get a specific cluster policy.
    
    Args:
        policy_id: ID of the cluster policy to get
        
    Returns:
        Response containing the cluster policy details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting cluster policy: {policy_id}")
    
    data = {
        "policy_id": policy_id,
    }
    
    try:
        return await make_api_request("GET", "/api/2.0/policies/clusters/get", data=data)
    except Exception as e:
        logger.error(f"Failed to get cluster policy: {str(e)}")
        raise DatabricksAPIError(f"Failed to get cluster policy: {str(e)}")


async def list_cluster_policies(
    sort_by: Optional[str] = None,
    sort_order: Optional[str] = None,
) -> Dict[str, Any]:
    """
    List all cluster policies.
    
    Args:
        sort_by: Optional field to sort by (e.g., "POLICY_CREATION_TIME")
        sort_order: Optional sort order (e.g., "ASC" or "DESC")
        
    Returns:
        Response containing a list of cluster policies
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing cluster policies")
    
    params = {}
    
    if sort_by is not None:
        params["sort_by"] = sort_by
    
    if sort_order is not None:
        params["sort_order"] = sort_order
    
    try:
        return await make_api_request("GET", "/api/2.0/policies/clusters/list", params=params)
    except Exception as e:
        logger.error(f"Failed to list cluster policies: {str(e)}")
        raise DatabricksAPIError(f"Failed to list cluster policies: {str(e)}")


async def get_cluster_policy_permissions(policy_id: str) -> Dict[str, Any]:
    """
    Get permissions for a cluster policy.
    
    Args:
        policy_id: ID of the cluster policy
        
    Returns:
        Response containing the permissions details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting permissions for cluster policy: {policy_id}")
    
    try:
        return await make_api_request("GET", f"/api/2.0/permissions/cluster-policies/{policy_id}")
    except Exception as e:
        logger.error(f"Failed to get cluster policy permissions: {str(e)}")
        raise DatabricksAPIError(f"Failed to get cluster policy permissions: {str(e)}")


async def update_cluster_policy_permissions(
    policy_id: str,
    access_control_list: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Update permissions for a cluster policy.
    
    Args:
        policy_id: ID of the cluster policy
        access_control_list: List of access control items
        
    Returns:
        Response containing the updated permissions details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating permissions for cluster policy: {policy_id}")
    
    data = {
        "access_control_list": access_control_list
    }
    
    try:
        return await make_api_request("PATCH", f"/api/2.0/permissions/cluster-policies/{policy_id}", data=data)
    except Exception as e:
        logger.error(f"Failed to update cluster policy permissions: {str(e)}")
        raise DatabricksAPIError(f"Failed to update cluster policy permissions: {str(e)}") 