"""Clusters tools module for Databricks MCP server.

This module provides tool implementations for Databricks Clusters API endpoints.
"""
import json

from mcp.server import FastMCP
from typing import Any, Dict, List, Optional, Union, cast 
from mcp.types import TextContent, CallToolResult 

from src.api.compute import clusters
from src.core.utils import DatabricksAPIError
from src.core.utils import API_ENDPOINTS
from src.core.logging import get_logger
from src.server.tools.common import generate_tool_description, handle_api_error, missing_param_error, success_response

logger = get_logger(__name__)


def register_cluster_tools(server):
    """Register all cluster-related tools with the server.
    
    Args:
        server: The Databricks MCP server instance
    """
    @server.tool(
        name="list_clusters",
        description=generate_tool_description(
            clusters.list_clusters,
            API_ENDPOINTS["list_clusters"]["method"],
            API_ENDPOINTS["list_clusters"]["endpoint"]
        ),
    )
    async def list_clusters(params: Dict[str, Any]) -> List[TextContent]:
        """List all Databricks clusters."""
        try:
            logger.info("Listing clusters")
            result = await clusters.list_clusters()
            return success_response(result)
        except DatabricksAPIError as e:
            logger.error(f"Failed to list clusters: {e}")
            return handle_api_error(e, "clusters")

    @server.tool(
        name="create_cluster",
        description=generate_tool_description(
            clusters.create_cluster,
            API_ENDPOINTS["create_cluster"]["method"],
            API_ENDPOINTS["create_cluster"]["endpoint"]
        ),
    )
    async def create_cluster(params: Dict[str, Any]) -> List[TextContent]:
        """Create a new Databricks cluster.
        Args:
            cluster_name: Name of the cluster
            spark_version: Spark version to use
            node_type_id: Type of nodes in the cluster
            num_workers: Number of worker nodes
            autoscale: Autoscaling configuration
            spark_conf: Spark configuration
            aws_attributes: AWS-specific attributes
            ssh_public_keys: SSH public keys for cluster access
            custom_tags: Custom tags for the cluster
            cluster_log_conf: Cluster log configuration
            init_scripts: Initialization scripts
            spark_env_vars: Spark environment variables
            enable_elastic_disk: Whether to enable elastic disk
            driver_node_type_id: Type of driver node
            runtime_engine: Runtime engine (photon or standard)
            **kwargs: Additional parameters for cluster creation
        """
        # Log the operation
        logger.info(f"Creating cluster with params: {params}")
        
        try:
             # Validate required parameters
            if "cluster_name" not in params:
                return missing_param_error("cluster_name")
            if "spark_version" not in params:
                return missing_param_error("spark_version")
            if "node_type_id" not in params:
                return missing_param_error("node_type_id")

            # Start with required parameters
            api_params = {
                "cluster_name": params.get("cluster_name"),
                "spark_version": params.get("spark_version"),
                "node_type_id": params.get("node_type_id"),
                "num_workers": params.get("num_workers",1),
            }
            
            # Add optional parameters only if they exist
            optional_params = [
                "num_workers", "autoscale", "spark_conf", "aws_attributes", 
                "ssh_public_keys", "custom_tags", "cluster_log_conf", 
                "init_scripts", "spark_env_vars", "enable_elastic_disk",
                "driver_node_type_id", "runtime_engine", "autotermination_minutes"
            ]
            
            for param in optional_params:
                if param in params and params[param] is not None:
                    api_params[param] = params[param]

            result = await clusters.create_cluster(**api_params)
            logger.info(f"Cluster created: {result}")
            return success_response(result)
        except DatabricksAPIError as e:
            # Log and return error
            return handle_api_error(e, "cluster creation")

    @server.tool(
        name="terminate_cluster",
        description=generate_tool_description(
            clusters.terminate_cluster,
            API_ENDPOINTS["terminate_cluster"]["method"],
            API_ENDPOINTS["terminate_cluster"]["endpoint"]
        ),
    )
    async def terminate_cluster(params: Dict[str, Any]) -> List[TextContent]:
        """Terminate a Databricks cluster.
        Parameters:
        - cluster_id (required): ID of the cluster to terminate
        """
        # Extract cluster_id from params
        cluster_id = params.get("cluster_id")
        if not cluster_id:
            return missing_param_error("cluster_id")
        
        logger.info(f"Terminating cluster with ID: {cluster_id}")
        
        try:
            # Terminate the cluster
            result = await clusters.terminate_cluster(cluster_id)
            logger.info(f"Cluster termination initiated: {result}")
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "cluster", cluster_id)

    @server.tool(
        name="get_cluster",
        description=generate_tool_description(
            clusters.get_cluster,
            API_ENDPOINTS["get_cluster"]["method"],
            API_ENDPOINTS["get_cluster"]["endpoint"]
        ),
    )
    async def get_cluster(params: Dict[str, Any]) -> List[TextContent]:
        """Get information about a specific cluster.
        Parameters:
        - cluster_id (required): ID of the cluster
        """
        # Extract cluster_id from params
        cluster_id = params.get("cluster_id")
        if not cluster_id:
            return missing_param_error("cluster_id")
        
        logger.info(f"Getting cluster with ID: {cluster_id}")
        
        try:
            # Get the cluster
            result = await clusters.get_cluster(cluster_id)
            logger.info(f"Retrieved cluster: {result}")
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "cluster", cluster_id)

    @server.tool(
        name="start_cluster",
        description=generate_tool_description(
            clusters.start_cluster,
            API_ENDPOINTS["start_cluster"]["method"],
            API_ENDPOINTS["start_cluster"]["endpoint"]
        ),
    )
    async def start_cluster(params: Dict[str, Any]) -> List[TextContent]:
        """Start a terminated Databricks cluster.
        Parameters:
        - cluster_id (required): ID of the cluster to start
        """
        # Extract cluster_id from params
        cluster_id = params.get("cluster_id")
        if not cluster_id:
            return missing_param_error("cluster_id")
        
        logger.info(f"Starting cluster with ID: {cluster_id}")
        
        try:
            # Start the cluster
            result = await clusters.start_cluster(cluster_id)
            logger.info(f"Cluster start initiated: {result}")
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "cluster", cluster_id)

    @server.tool(
        name="restart_cluster",
        description=generate_tool_description(
            clusters.restart_cluster,
            API_ENDPOINTS["restart_cluster"]["method"],
            API_ENDPOINTS["restart_cluster"]["endpoint"]
        ),
    )
    async def restart_cluster(params: Dict[str, Any]) -> List[TextContent]:
        """Restart a Databricks cluster.
        Parameters:
        - cluster_id (required): ID of the cluster to restart
        """
        # Extract cluster_id from params
        cluster_id = params.get("cluster_id")
        if not cluster_id:
            return missing_param_error("cluster_id")
        
        logger.info(f"Restarting cluster with ID: {cluster_id}")
        
        try:
            # Restart the cluster
            result = await clusters.restart_cluster(cluster_id)
            logger.info(f"Cluster restart initiated: {result}")
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "cluster", cluster_id)

    @server.tool(
        name="resize_cluster",
        description=generate_tool_description(
            clusters.resize_cluster,
            API_ENDPOINTS["resize_cluster"]["method"],
            API_ENDPOINTS["resize_cluster"]["endpoint"]
        ),
    )
    async def resize_cluster(params: Dict[str, Any]) -> List[TextContent]:
        """Resize a cluster by changing the number of workers.
        Parameters:
        - cluster_id (required): ID of the cluster to resize
        - num_workers (required): New number of workers
        """
        # Extract parameters
        cluster_id = params.get("cluster_id")
        num_workers = params.get("num_workers")
        
        # Validate required parameters
        if not cluster_id:
            return missing_param_error("cluster_id")
        
        if num_workers is None:  # Allow 0 workers
            return missing_param_error("num_workers")
        
        logger.info(f"Resizing cluster {cluster_id} to {num_workers} workers")
        
        try:
            # Resize the cluster
            result = await clusters.resize_cluster(cluster_id, num_workers)
            logger.info(f"Cluster resize initiated: {result}")
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "cluster", cluster_id)

    @server.tool(
        name="permanently_delete_cluster",
        description=generate_tool_description(
            clusters.delete_cluster,
            API_ENDPOINTS["permanent-delete"]["method"],
            API_ENDPOINTS["permanent-delete"]["endpoint"]
        ),
    )
    async def permanently_delete_cluster(params: Dict[str, Any]) -> List[TextContent]:
        """Permanently delete a Databricks cluster.
        Parameters:
        - cluster_id (required): ID of the cluster to permanently delete
        """
        # Extract cluster_id from params
        cluster_id = params.get("cluster_id")
        if not cluster_id:
            return missing_param_error("cluster_id")
        
        logger.info(f"Permanently deleting cluster with ID: {cluster_id}")
        
        try:
            # Permanently delete the cluster
            result = await clusters.delete_cluster(cluster_id)
            logger.info(f"Cluster permanently deleted: {result}")
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "cluster", cluster_id) 