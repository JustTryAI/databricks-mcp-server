"""
Databricks Compute API Package.

This package provides modules for interacting with Databricks compute-related APIs
such as Clusters, Cluster Policies, Instance Profiles, and Libraries.
"""

# Import from internal modules 
from src.api.compute.clusters import *
from src.api.compute.cluster_policies import *
from src.api.compute.instance_pools import *
from src.api.compute.instance_profiles import *
from src.api.compute.libraries import *

# For backward compatibility, we still need to import from parent directory
# while the refactoring is in progress
import sys

# Only import old modules if they exist (for backward compatibility)
try:
    from src.api import clusters as _clusters
    from src.api import libraries as _libraries
    
    # Add things from old modules that might not be in the new modules yet
    # for name in dir(_clusters):
    #     if not name.startswith('_') and name not in globals():
    #         globals()[name] = getattr(_clusters, name)
    
    # for name in dir(_libraries):
    #     if not name.startswith('_') and name not in globals():
    #         globals()[name] = getattr(_libraries, name)
except ImportError:
    pass

from src.api.compute.clusters import (
    create_cluster,
    delete_cluster,
    get_cluster,
    get_cluster_events,
    list_clusters,
    list_node_types,
    list_spark_versions,
    pin_cluster,
    restart_cluster,
    resize_cluster,
    start_cluster,
    terminate_cluster,
    unpin_cluster,
)

from src.api.compute.cluster_policies import (
    create_cluster_policy,
    edit_cluster_policy,
    delete_cluster_policy,
    get_cluster_policy,
    list_cluster_policies,
    get_cluster_policy_permissions,
    update_cluster_policy_permissions,
)

from src.api.compute.instance_profiles import (
    add_instance_profile,
    list_instance_profiles,
    remove_instance_profile,
    get_instance_profile_permissions,
    update_instance_profile_permissions,
)

from src.api.compute.libraries import (
    get_all_libraries,
    get_library_status,
    install_libraries,
    uninstall_libraries,
)

__all__ = [
    # Clusters API
    "create_cluster",
    "delete_cluster",
    "get_cluster",
    "get_cluster_events",
    "list_clusters",
    "list_node_types",
    "list_spark_versions",
    "pin_cluster",
    "restart_cluster",
    "resize_cluster",
    "start_cluster",
    "terminate_cluster",
    "unpin_cluster",
    
    # Cluster Policies API
    "create_cluster_policy",
    "edit_cluster_policy",
    "delete_cluster_policy",
    "get_cluster_policy",
    "list_cluster_policies",
    "get_cluster_policy_permissions",
    "update_cluster_policy_permissions",
    
    # Instance Profiles API
    "add_instance_profile",
    "list_instance_profiles",
    "remove_instance_profile",
    "get_instance_profile_permissions",
    "update_instance_profile_permissions",
    
    # Libraries API
    "get_all_libraries",
    "get_library_status",
    "install_libraries",
    "uninstall_libraries",
] 