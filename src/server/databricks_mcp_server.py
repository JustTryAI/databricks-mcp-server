"""
Databricks MCP Server

This module implements a standalone MCP server that provides tools for interacting
with Databricks APIs. It follows the Model Context Protocol standard, communicating
via stdio and directly connecting to Databricks when tools are invoked.
"""

import asyncio
import json
import logging
import sys
import os
from typing import Any, Dict, List, Optional, Union, cast

from mcp.server import FastMCP
from mcp.types import TextContent
from mcp.server.stdio import stdio_server

from src.api import (
    budgets,
    clusters,
    commands,
    dbfs,
    external_locations,
    jobs,
    lakeview,
    libraries,
    notebooks,
    pipelines,
    service_principals,
    sql,
    sql_queries,
    unity_catalog,
    warehouses,
    workspace,
)
from src.core.config import settings
from src.core.utils import generate_tool_description, API_ENDPOINTS

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    filename="databricks_mcp.log",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class DatabricksMCPServer(FastMCP):
    """An MCP server for Databricks APIs."""

    def __init__(self):
        """Initialize the Databricks MCP server."""
        super().__init__(name="databricks-mcp", 
                         version="1.0.0", 
                         instructions="Use this server to manage Databricks resources")
        logger.info("Initializing Databricks MCP server")
        logger.info(f"Databricks host: {settings.DATABRICKS_HOST}")
        
        # Register tools
        self._register_tools()
    
    def _register_tools(self):
        """Register all Databricks MCP tools."""
        
        # Cluster management tools
        @self.tool(
            name="list_clusters",
            description=generate_tool_description(
                clusters.list_clusters,
                API_ENDPOINTS["list_clusters"]["method"],
                API_ENDPOINTS["list_clusters"]["endpoint"]
            ),
        )
        async def list_clusters(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing clusters with params: {params}")
            try:
                result = await clusters.list_clusters()
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing clusters: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="create_cluster",
            description=generate_tool_description(
                clusters.create_cluster,
                API_ENDPOINTS["create_cluster"]["method"],
                API_ENDPOINTS["create_cluster"]["endpoint"]
            ),
        )
        async def create_cluster(params: Dict[str, Any]) -> List[TextContent]:
            """Create a new Databricks cluster."""
            try:
                # Validate cluster_config is present
                if "cluster_config" not in params:
                    raise ValueError("Required parameter 'cluster_config' is missing")
                
                # Validate basic cluster config requirements
                cluster_config = params["cluster_config"]
                if not isinstance(cluster_config, dict):
                    raise ValueError("cluster_config must be a dictionary")
                
                # Check for essential cluster configuration parameters
                essential_params = ["spark_version", "node_type_id"]
                for param in essential_params:
                    if param not in cluster_config:
                        raise ValueError(f"Cluster config is missing required parameter: '{param}'")
                
                # Either autoscale or num_workers should be specified
                if "num_workers" not in cluster_config and "autoscale" not in cluster_config:
                    raise ValueError("Either 'num_workers' or 'autoscale' must be specified in cluster_config")
                
                result = await clusters.create_cluster(**params)
                return [TextContent(text=json.dumps(result))]
            except Exception as e:
                return [TextContent(text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="terminate_cluster",
            description=generate_tool_description(
                clusters.terminate_cluster,
                API_ENDPOINTS["terminate_cluster"]["method"],
                API_ENDPOINTS["terminate_cluster"]["endpoint"]
            ),
        )
        async def terminate_cluster(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Terminating cluster with params: {params}")
            try:
                result = await clusters.terminate_cluster(params.get("cluster_id"))
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error terminating cluster: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_cluster",
            description=generate_tool_description(
                clusters.get_cluster,
                API_ENDPOINTS["get_cluster"]["method"],
                API_ENDPOINTS["get_cluster"]["endpoint"]
            ),
        )
        async def get_cluster(params: Dict[str, Any]) -> List[TextContent]:
            """Get information about a specific cluster."""
            try:
                # Validate required parameters
                if "cluster_id" not in params:
                    raise ValueError("Required parameter 'cluster_id' is missing")
                
                # Validate cluster_id format
                if not isinstance(params["cluster_id"], str) or not params["cluster_id"]:
                    raise ValueError("cluster_id must be a non-empty string")
                
                result = await clusters.get_cluster(**params)
                return [TextContent(text=json.dumps(result))]
            except Exception as e:
                return [TextContent(text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="start_cluster",
            description=generate_tool_description(
                clusters.start_cluster,
                API_ENDPOINTS["start_cluster"]["method"],
                API_ENDPOINTS["start_cluster"]["endpoint"]
            ),
        )
        async def start_cluster(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Starting cluster with params: {params}")
            try:
                result = await clusters.start_cluster(params.get("cluster_id"))
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error starting cluster: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="restart_cluster",
            description=generate_tool_description(
                clusters.restart_cluster,
                API_ENDPOINTS["restart_cluster"]["method"],
                API_ENDPOINTS["restart_cluster"]["endpoint"]
            ),
        )
        async def restart_cluster(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Restarting cluster with params: {params}")
            try:
                result = await clusters.restart_cluster(params.get("cluster_id"))
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error restarting cluster: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="resize_cluster",
            description=generate_tool_description(
                clusters.resize_cluster,
                API_ENDPOINTS["resize_cluster"]["method"],
                API_ENDPOINTS["resize_cluster"]["endpoint"]
            ),
        )
        async def resize_cluster(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Resizing cluster with params: {params}")
            try:
                cluster_id = params.get("cluster_id")
                num_workers = params.get("num_workers")
                result = await clusters.resize_cluster(cluster_id, num_workers)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error resizing cluster: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="permanently_delete_cluster",
            description=generate_tool_description(
                clusters.permanent_delete_cluster,
                API_ENDPOINTS["permanent_delete_cluster"]["method"],
                API_ENDPOINTS["permanent_delete_cluster"]["endpoint"]
            ),
        )
        async def permanently_delete_cluster(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Permanently deleting cluster with params: {params}")
            try:
                result = await clusters.permanent_delete_cluster(params.get("cluster_id"))
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error permanently deleting cluster: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Job management tools
        @self.tool(
            name="list_jobs",
            description=generate_tool_description(
                jobs.list_jobs,
                API_ENDPOINTS["list_jobs"]["method"],
                API_ENDPOINTS["list_jobs"]["endpoint"]
            ),
        )
        async def list_jobs(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing jobs with params: {params}")
            try:
                result = await jobs.list_jobs()
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing jobs: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="run_job",
            description=generate_tool_description(
                jobs.run_job,
                API_ENDPOINTS["run_job"]["method"],
                API_ENDPOINTS["run_job"]["endpoint"]
            ),
        )
        async def run_job(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Running job with params: {params}")
            try:
                notebook_params = params.get("notebook_params", {})
                result = await jobs.run_job(params.get("job_id"), notebook_params)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error running job: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="update_job",
            description=generate_tool_description(
                jobs.update_job,
                API_ENDPOINTS["update_job"]["method"],
                API_ENDPOINTS["update_job"]["endpoint"]
            ),
        )
        async def update_job(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating job with params: {params}")
            try:
                job_id = params.get("job_id")
                new_settings = params.get("new_settings")
                result = await jobs.update_job(job_id, new_settings)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating job: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="reset_job",
            description=generate_tool_description(
                jobs.reset_job,
                API_ENDPOINTS["reset_job"]["method"],
                API_ENDPOINTS["reset_job"]["endpoint"]
            ),
        )
        async def reset_job(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Resetting job with params: {params}")
            try:
                job_id = params.get("job_id")
                result = await jobs.reset_job(job_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error resetting job: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_job_run_output",
            description=generate_tool_description(
                jobs.get_run_output,
                API_ENDPOINTS["get_run_output"]["method"],
                API_ENDPOINTS["get_run_output"]["endpoint"]
            ),
        )
        async def get_job_run_output(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting job run output with params: {params}")
            try:
                run_id = params.get("run_id")
                result = await jobs.get_run_output(run_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting job run output: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="list_job_runs",
            description=generate_tool_description(
                jobs.list_runs,
                API_ENDPOINTS["list_runs"]["method"],
                API_ENDPOINTS["list_runs"]["endpoint"]
            ),
        )
        async def list_job_runs(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing job runs with params: {params}")
            try:
                job_id = params.get("job_id")
                active_only = params.get("active_only", False)
                completed_only = params.get("completed_only", False)
                offset = params.get("offset", 0)
                limit = params.get("limit", 20)
                result = await jobs.list_runs(job_id, active_only, completed_only, offset, limit)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing job runs: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="cancel_job_run",
            description=generate_tool_description(
                jobs.cancel_run,
                API_ENDPOINTS["cancel_run"]["method"],
                API_ENDPOINTS["cancel_run"]["endpoint"]
            ),
        )
        async def cancel_job_run(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Canceling job run with params: {params}")
            try:
                run_id = params.get("run_id")
                result = await jobs.cancel_run(run_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error canceling job run: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_job_run",
            description=generate_tool_description(
                jobs.delete_run,
                API_ENDPOINTS["delete_run"]["method"],
                API_ENDPOINTS["delete_run"]["endpoint"]
            ),
        )
        async def delete_job_run(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting job run with params: {params}")
            try:
                run_id = params.get("run_id")
                result = await jobs.delete_run(run_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting job run: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_job",
            description=generate_tool_description(
                jobs.delete_job,
                API_ENDPOINTS["delete_job"]["method"],
                API_ENDPOINTS["delete_job"]["endpoint"]
            ),
        )
        async def delete_job(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting job with params: {params}")
            try:
                job_id = params.get("job_id")
                result = await jobs.delete_job(job_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting job: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Notebook management tools
        @self.tool(
            name="list_notebooks",
            description=generate_tool_description(
                notebooks.list_notebooks,
                API_ENDPOINTS["list_notebooks"]["method"],
                API_ENDPOINTS["list_notebooks"]["endpoint"]
            ),
        )
        async def list_notebooks(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing notebooks with params: {params}")
            try:
                result = await notebooks.list_notebooks(params.get("path"))
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing notebooks: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="export_notebook",
            description=generate_tool_description(
                notebooks.export_notebook,
                API_ENDPOINTS["export_notebook"]["method"],
                API_ENDPOINTS["export_notebook"]["endpoint"]
            ),
        )
        async def export_notebook(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Exporting notebook with params: {params}")
            try:
                format_type = params.get("format", "SOURCE")
                result = await notebooks.export_notebook(params.get("path"), format_type)
                
                # For notebooks, we might want to trim the response for readability
                content = result.get("content", "")
                if len(content) > 1000:
                    summary = f"{content[:1000]}... [content truncated, total length: {len(content)} characters]"
                    result["content"] = summary
                
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error exporting notebook: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # DBFS tools
        @self.tool(
            name="list_files",
            description=generate_tool_description(
                dbfs.list_files,
                API_ENDPOINTS["list_dbfs_files"]["method"],
                API_ENDPOINTS["list_dbfs_files"]["endpoint"]
            ),
        )
        async def list_files(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing files with params: {params}")
            try:
                result = await dbfs.list_files(params.get("dbfs_path"))
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing files: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="import_file",
            description=generate_tool_description(
                dbfs.import_file,
                API_ENDPOINTS["import_file"]["method"],
                API_ENDPOINTS["import_file"]["endpoint"]
            ),
        )
        async def import_file(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Importing file with params: {params}")
            try:
                source_path = params.get("source_path")
                target_path = params.get("target_path")
                overwrite = params.get("overwrite", False)
                result = await dbfs.import_file(source_path, target_path, overwrite)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error importing file: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="create_directory",
            description=generate_tool_description(
                dbfs.create_directory,
                API_ENDPOINTS["create_dbfs_directory"]["method"],
                API_ENDPOINTS["create_dbfs_directory"]["endpoint"]
            ),
        )
        async def create_directory(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating directory with params: {params}")
            try:
                path = params.get("path")
                result = await dbfs.create_directory(path)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating directory: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_dbfs_file",
            description=generate_tool_description(
                dbfs.delete_file,
                API_ENDPOINTS["delete_dbfs_file"]["method"],
                API_ENDPOINTS["delete_dbfs_file"]["endpoint"]
            ),
        )
        async def delete_dbfs_file(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting DBFS file with params: {params}")
            try:
                path = params.get("path")
                recursive = params.get("recursive", False)
                result = await dbfs.delete_file(path, recursive)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting DBFS file: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="read_dbfs_file",
            description=generate_tool_description(
                dbfs.read_file,
                API_ENDPOINTS["read_dbfs_file"]["method"],
                API_ENDPOINTS["read_dbfs_file"]["endpoint"]
            ),
        )
        async def read_dbfs_file(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Reading DBFS file with params: {params}")
            try:
                path = params.get("path")
                offset = params.get("offset", 0)
                length = params.get("length", 1024 * 1024)  # Default to 1MB
                result = await dbfs.read_file(path, offset, length)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error reading DBFS file: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="move_dbfs_file",
            description=generate_tool_description(
                dbfs.move_file,
                API_ENDPOINTS["move_file"]["method"],
                API_ENDPOINTS["move_file"]["endpoint"]
            ),
        )
        async def move_dbfs_file(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Moving DBFS file with params: {params}")
            try:
                source_path = params.get("source_path")
                target_path = params.get("target_path")
                result = await dbfs.move_file(source_path, target_path)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error moving DBFS file: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Workspace tools
        @self.tool(
            name="list_workspace_files",
            description=generate_tool_description(
                workspace.list_files, 
                API_ENDPOINTS["list_files"]["method"],
                API_ENDPOINTS["list_files"]["endpoint"]
            ),
        )
        async def list_workspace_files(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing workspace files with params: {params}")
            try:
                result = await workspace.list_files(params.get("path"))
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing workspace files: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="import_workspace_file",
            description=generate_tool_description(
                workspace.import_files, 
                API_ENDPOINTS["import_files"]["method"],
                API_ENDPOINTS["import_files"]["endpoint"]
            ),
        )
        async def import_workspace_file(params: Dict[str, Any]) -> List[TextContent]:
            """Import a file to the workspace."""
            try:
                # Validate required parameters
                required_params = ["path", "format", "content"]
                for param in required_params:
                    if param not in params:
                        raise ValueError(f"Required parameter '{param}' is missing")
                
                # Validate content is base64 encoded
                if "content" in params:
                    content = params["content"]
                    try:
                        # Try decoding to ensure it's valid base64
                        import base64
                        base64.b64decode(content)
                    except Exception:
                        raise ValueError("The 'content' parameter must be base64 encoded")
                
                # Validate format is one of the allowed values
                if "format" in params:
                    allowed_formats = ["SOURCE", "HTML", "JUPYTER", "DBC", "AUTO"]
                    if params["format"] not in allowed_formats:
                        raise ValueError(f"Format must be one of: {', '.join(allowed_formats)}")
                
                result = await workspace.import_files(**params)
                return [TextContent(text=json.dumps(result))]
            except Exception as e:
                return [TextContent(text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="export_workspace_file",
            description=generate_tool_description(
                workspace.export_files,
                API_ENDPOINTS["export_files"]["method"],
                API_ENDPOINTS["export_files"]["endpoint"]
            ),
        )
        async def export_workspace_file(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Exporting workspace file with params: {params}")
            try:
                path = params.get("path")
                format = params.get("format")
                result = await workspace.export_files(path, format)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error exporting workspace file: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_workspace_file",
            description=generate_tool_description(
                workspace.delete_files,
                API_ENDPOINTS["delete_files"]["method"],
                API_ENDPOINTS["delete_files"]["endpoint"]
            ),
        )
        async def delete_workspace_file(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting workspace file with params: {params}")
            try:
                path = params.get("path")
                recursive = params.get("recursive", False)
                result = await workspace.delete_files(path, recursive)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting workspace file: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_workspace_status",
            description=generate_tool_description(
                workspace.get_file_status,
                API_ENDPOINTS["get_file_status"]["method"],
                API_ENDPOINTS["get_file_status"]["endpoint"]
            ),
        )
        async def get_workspace_status(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting workspace status with params: {params}")
            try:
                path = params.get("path")
                result = await workspace.get_file_status(path)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting workspace status: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="create_workspace_directory",
            description=generate_tool_description(
                workspace.mkdirs,
                API_ENDPOINTS["mkdirs"]["method"],
                API_ENDPOINTS["mkdirs"]["endpoint"]
            ),
        )
        async def create_workspace_directory(params: Dict[str, Any]) -> List[TextContent]:
            """Create a directory in the workspace."""
            try:
                path = params.get("path")
                result = await workspace.mkdirs(path)
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error creating workspace directory: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        # SQL tools
        @self.tool(
            name="execute_sql",
            description=generate_tool_description(
                sql.execute_sql,
                API_ENDPOINTS["execute_sql"]["method"],
                API_ENDPOINTS["execute_sql"]["endpoint"]
            ),
        )
        async def execute_sql(params: Dict[str, Any]) -> List[TextContent]:
            """Execute a SQL statement."""
            try:
                # Validate required parameters
                required_params = ["statement", "warehouse_id"]
                for param in required_params:
                    if param not in params:
                        raise ValueError(f"Required parameter '{param}' is missing")
                
                # Validate statement is a non-empty string
                if not params.get("statement") or not isinstance(params.get("statement"), str):
                    raise ValueError("'statement' must be a non-empty string")
                
                # Validate warehouse_id is a string
                if not isinstance(params.get("warehouse_id"), str):
                    raise ValueError("'warehouse_id' must be a string")
                
                # Validate row_limit is a positive integer if provided
                if "row_limit" in params and (
                    not isinstance(params["row_limit"], int) or params["row_limit"] <= 0
                ):
                    raise ValueError("'row_limit' must be a positive integer")
                
                result = await sql.execute_sql(**params)
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        # Unity Catalog tools
        
        # Catalog operations
        @self.tool(
            name="list_catalogs",
            description=generate_tool_description(
                unity_catalog.list_catalogs,
                API_ENDPOINTS["list_catalogs"]["method"],
                API_ENDPOINTS["list_catalogs"]["endpoint"]
            ),
        )
        async def list_catalogs(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing catalogs with params: {params}")
            try:
                max_results = params.get("max_results")
                result = await unity_catalog.list_catalogs(max_results)
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error listing catalogs: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="create_catalog",
            description=generate_tool_description(
                unity_catalog.create_catalog,
                API_ENDPOINTS["create_catalog"]["method"],
                API_ENDPOINTS["create_catalog"]["endpoint"]
            ),
        )
        async def create_catalog(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating catalog with params: {params}")
            try:
                name = params.get("name")
                comment = params.get("comment")
                result = await unity_catalog.create_catalog(name, comment)
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error creating catalog: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="get_catalog",
            description=generate_tool_description(
                unity_catalog.get_catalog,
                API_ENDPOINTS["get_catalog"]["method"],
                API_ENDPOINTS["get_catalog"]["endpoint"]
            ),
        )
        async def get_catalog(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting catalog with params: {params}")
            try:
                name = params.get("name")
                result = await unity_catalog.get_catalog(name)
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error getting catalog: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="update_catalog",
            description=generate_tool_description(
                unity_catalog.update_catalog,
                API_ENDPOINTS["update_catalog"]["method"],
                API_ENDPOINTS["update_catalog"]["endpoint"]
            ),
        )
        async def update_catalog(params: Dict[str, Any]) -> List[TextContent]:
            """Update a catalog in the Unity Catalog."""
            try:
                # Validate required parameter
                if "name" not in params:
                    raise ValueError("Required parameter 'name' is missing")
                
                # Validate parameter types
                if not isinstance(params["name"], str) or not params["name"]:
                    raise ValueError("name must be a non-empty string")
                
                # Validate new_name if provided
                if "new_name" in params and (not isinstance(params["new_name"], str) or not params["new_name"]):
                    raise ValueError("new_name must be a non-empty string")
                
                # Validate comment is a string if provided
                if "comment" in params and not isinstance(params["comment"], str):
                    raise ValueError("comment must be a string")
                
                result = await unity_catalog.update_catalog(**params)
                return [TextContent(text=json.dumps(result))]
            except Exception as e:
                return [TextContent(text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="delete_catalog",
            description=generate_tool_description(
                unity_catalog.delete_catalog,
                API_ENDPOINTS["delete_catalog"]["method"],
                API_ENDPOINTS["delete_catalog"]["endpoint"]
            ),
        )
        async def delete_catalog(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting catalog with params: {params}")
            try:
                name = params.get("name")
                result = await unity_catalog.delete_catalog(name)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting catalog: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Schema operations
        @self.tool(
            name="list_schemas",
            description=generate_tool_description(
                unity_catalog.list_schemas,
                API_ENDPOINTS["list_schemas"]["method"],
                API_ENDPOINTS["list_schemas"]["endpoint"]
            ),
        )
        async def list_schemas(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing schemas with params: {params}")
            try:
                catalog_name = params.get("catalog_name")
                max_results = params.get("max_results")
                result = await unity_catalog.list_schemas(catalog_name, max_results)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing schemas: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="create_schema",
            description=generate_tool_description(
                unity_catalog.create_schema,
                API_ENDPOINTS["create_schema"]["method"],
                API_ENDPOINTS["create_schema"]["endpoint"]
            ),
        )
        async def create_schema(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating schema with params: {params}")
            try:
                name = params.get("name")
                catalog_name = params.get("catalog_name")
                comment = params.get("comment")
                result = await unity_catalog.create_schema(name, catalog_name, comment)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating schema: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_schema",
            description=generate_tool_description(
                unity_catalog.get_schema,
                API_ENDPOINTS["get_schema"]["method"],
                API_ENDPOINTS["get_schema"]["endpoint"]
            ),
        )
        async def get_schema(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting schema with params: {params}")
            try:
                full_name = params.get("full_name")
                result = await unity_catalog.get_schema(full_name)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting schema: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="update_schema",
            description=generate_tool_description(
                unity_catalog.update_schema,
                API_ENDPOINTS["update_schema"]["method"],
                API_ENDPOINTS["update_schema"]["endpoint"]
            ),
        )
        async def update_schema(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating schema with params: {params}")
            try:
                full_name = params.get("full_name")
                new_name = params.get("new_name")
                comment = params.get("comment")
                result = await unity_catalog.update_schema(full_name, new_name, comment)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating schema: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_schema",
            description=generate_tool_description(
                unity_catalog.delete_schema,
                API_ENDPOINTS["delete_schema"]["method"],
                API_ENDPOINTS["delete_schema"]["endpoint"]
            ),
        )
        async def delete_schema(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting schema with params: {params}")
            try:
                full_name = params.get("full_name")
                result = await unity_catalog.delete_schema(full_name)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting schema: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Table operations
        @self.tool(
            name="list_tables",
            description=generate_tool_description(
                unity_catalog.list_tables,
                API_ENDPOINTS["list_tables"]["method"],
                API_ENDPOINTS["list_tables"]["endpoint"]
            ),
        )
        async def list_tables(params: Dict[str, Any]) -> List[TextContent]:
            """List tables in a Unity Catalog schema."""
            try:
                # Validate catalog and schema parameters
                if "catalog_name" not in params:
                    raise ValueError("Required parameter 'catalog_name' is missing")
                
                if "schema_name" not in params:
                    raise ValueError("Required parameter 'schema_name' is missing")
                
                # Validate parameter types
                if not isinstance(params["catalog_name"], str) or not params["catalog_name"]:
                    raise ValueError("catalog_name must be a non-empty string")
                
                if not isinstance(params["schema_name"], str) or not params["schema_name"]:
                    raise ValueError("schema_name must be a non-empty string")
                
                # Validate max_results if provided
                if "max_results" in params:
                    if not isinstance(params["max_results"], int) or params["max_results"] <= 0:
                        raise ValueError("max_results must be a positive integer")
                
                result = await unity_catalog.list_tables(**params)
                return [TextContent(text=json.dumps(result))]
            except Exception as e:
                return [TextContent(text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="get_table",
            description=generate_tool_description(
                unity_catalog.get_table,
                API_ENDPOINTS["get_table"]["method"],
                API_ENDPOINTS["get_table"]["endpoint"]
            ),
        )
        async def get_table(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting table with params: {params}")
            try:
                full_name = params.get("full_name")
                result = await unity_catalog.get_table(full_name)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting table: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # SQL Queries tools
        @self.tool(
            name="create_sql_query",
            description=generate_tool_description(
                sql_queries.create_query,
                API_ENDPOINTS["create_query"]["method"],
                API_ENDPOINTS["create_query"]["endpoint"]
            ),
        )
        async def create_sql_query(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating SQL query with params: {params}")
            try:
                name = params.get("name")
                description = params.get("description")
                query = params.get("query")
                parent = params.get("parent")
                run_as_role = params.get("run_as_role")
                result = await sql_queries.create_query(name, description, query, parent, run_as_role)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating SQL query: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="list_sql_queries",
            description=generate_tool_description(
                sql_queries.list_queries,
                API_ENDPOINTS["list_queries"]["method"],
                API_ENDPOINTS["list_queries"]["endpoint"]
            ),
        )
        async def list_sql_queries(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing SQL queries with params: {params}")
            try:
                page_size = params.get("page_size")
                page = params.get("page")
                order = params.get("order")
                q = params.get("q")
                result = await sql_queries.list_queries(page_size, page, order, q)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing SQL queries: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_sql_query",
            description=generate_tool_description(
                sql_queries.get_query,
                API_ENDPOINTS["get_query"]["method"],
                API_ENDPOINTS["get_query"]["endpoint"]
            ),
        )
        async def get_sql_query(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting SQL query with params: {params}")
            try:
                query_id = params.get("query_id")
                result = await sql_queries.get_query(query_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting SQL query: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="update_sql_query",
            description=generate_tool_description(
                sql_queries.update_query,
                API_ENDPOINTS["update_query"]["method"],
                API_ENDPOINTS["update_query"]["endpoint"]
            ),
        )
        async def update_sql_query(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating SQL query with params: {params}")
            try:
                query_id = params.get("query_id")
                name = params.get("name")
                description = params.get("description")
                query = params.get("query")
                run_as_role = params.get("run_as_role")
                result = await sql_queries.update_query(query_id, name, description, query, run_as_role)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating SQL query: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_sql_query",
            description=generate_tool_description(
                sql_queries.delete_query,
                API_ENDPOINTS["delete_query"]["method"],
                API_ENDPOINTS["delete_query"]["endpoint"]
            ),
        )
        async def delete_sql_query(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting SQL query with params: {params}")
            try:
                query_id = params.get("query_id")
                result = await sql_queries.delete_query(query_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting SQL query: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="run_sql_query",
            description=generate_tool_description(
                sql_queries.run_query,
                API_ENDPOINTS["run_query"]["method"],
                API_ENDPOINTS["run_query"]["endpoint"]
            ),
        )
        async def run_sql_query(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Running SQL query with params: {params}")
            try:
                query_id = params.get("query_id")
                parameters = params.get("parameters")
                warehouse_id = params.get("warehouse_id")
                result = await sql_queries.run_query(query_id, parameters, warehouse_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error running SQL query: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # SQL Dashboard tools
        @self.tool(
            name="create_dashboard",
            description=generate_tool_description(
                sql_queries.create_dashboard,
                API_ENDPOINTS["create_dashboard"]["method"],
                API_ENDPOINTS["create_dashboard"]["endpoint"]
            ),
        )
        async def create_dashboard(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating dashboard with params: {params}")
            try:
                name = params.get("name")
                description = params.get("description")
                tags = params.get("tags")
                result = await sql_queries.create_dashboard(name, description, tags)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating dashboard: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="list_dashboards",
            description=generate_tool_description(
                sql_queries.list_dashboards,
                API_ENDPOINTS["list_dashboards"]["method"],
                API_ENDPOINTS["list_dashboards"]["endpoint"]
            ),
        )
        async def list_dashboards(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing dashboards with params: {params}")
            try:
                page_size = params.get("page_size")
                page = params.get("page")
                order = params.get("order")
                q = params.get("q")
                result = await sql_queries.list_dashboards(page_size, page, order, q)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing dashboards: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_dashboard",
            description=generate_tool_description(
                sql_queries.get_dashboard,
                API_ENDPOINTS["get_dashboard"]["method"],
                API_ENDPOINTS["get_dashboard"]["endpoint"]
            ),
        )
        async def get_dashboard(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting dashboard with params: {params}")
            try:
                dashboard_id = params.get("dashboard_id")
                result = await sql_queries.get_dashboard(dashboard_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting dashboard: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="update_dashboard",
            description=generate_tool_description(
                sql_queries.update_dashboard,
                API_ENDPOINTS["update_dashboard"]["method"],
                API_ENDPOINTS["update_dashboard"]["endpoint"]
            ),
        )
        async def update_dashboard(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating dashboard with params: {params}")
            try:
                dashboard_id = params.get("dashboard_id")
                name = params.get("name")
                description = params.get("description")
                tags = params.get("tags")
                result = await sql_queries.update_dashboard(dashboard_id, name, description, tags)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating dashboard: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_dashboard",
            description=generate_tool_description(
                sql_queries.delete_dashboard,
                API_ENDPOINTS["delete_dashboard"]["method"],
                API_ENDPOINTS["delete_dashboard"]["endpoint"]
            ),
        )
        async def delete_dashboard(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting dashboard with params: {params}")
            try:
                dashboard_id = params.get("dashboard_id")
                result = await sql_queries.delete_dashboard(dashboard_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting dashboard: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Alert tools
        @self.tool(
            name="create_alert",
            description=generate_tool_description(
                sql_queries.create_alert,
                API_ENDPOINTS["create_alert"]["method"],
                API_ENDPOINTS["create_alert"]["endpoint"]
            ),
        )
        async def create_alert(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating alert with params: {params}")
            try:
                query_id = params.get("query_id")
                name = params.get("name")
                options = params.get("options")
                result = await sql_queries.create_alert(query_id, name, options)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating alert: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="list_alerts",
            description=generate_tool_description(
                sql_queries.list_alerts,
                API_ENDPOINTS["list_alerts"]["method"],
                API_ENDPOINTS["list_alerts"]["endpoint"]
            ),
        )
        async def list_alerts(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing alerts with params: {params}")
            try:
                page_size = params.get("page_size")
                page = params.get("page")
                result = await sql_queries.list_alerts(page_size, page)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing alerts: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_alert",
            description=generate_tool_description(
                sql_queries.get_alert,
                API_ENDPOINTS["get_alert"]["method"],
                API_ENDPOINTS["get_alert"]["endpoint"]
            ),
        )
        async def get_alert(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting alert with params: {params}")
            try:
                alert_id = params.get("alert_id")
                result = await sql_queries.get_alert(alert_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting alert: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="update_alert",
            description=generate_tool_description(
                sql_queries.update_alert,
                API_ENDPOINTS["update_alert"]["method"],
                API_ENDPOINTS["update_alert"]["endpoint"]
            ),
        )
        async def update_alert(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating alert with params: {params}")
            try:
                alert_id = params.get("alert_id")
                name = params.get("name")
                options = params.get("options")
                result = await sql_queries.update_alert(alert_id, name, options)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating alert: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_alert",
            description=generate_tool_description(
                sql_queries.delete_alert,
                API_ENDPOINTS["delete_alert"]["method"],
                API_ENDPOINTS["delete_alert"]["endpoint"]
            ),
        )
        async def delete_alert(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting alert with params: {params}")
            try:
                alert_id = params.get("alert_id")
                result = await sql_queries.delete_alert(alert_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting alert: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Visualization tools
        @self.tool(
            name="create_visualization",
            description=generate_tool_description(
                sql_queries.create_visualization,
                API_ENDPOINTS["create_visualization"]["method"],
                API_ENDPOINTS["create_visualization"]["endpoint"]
            ),
        )
        async def create_visualization(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating visualization with params: {params}")
            try:
                query_id = params.get("query_id")
                visualization_type = params.get("visualization_type")
                name = params.get("name")
                options = params.get("options")
                description = params.get("description")
                result = await sql_queries.create_visualization(query_id, visualization_type, name, options, description)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating visualization: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="update_visualization",
            description=generate_tool_description(
                sql_queries.update_visualization,
                API_ENDPOINTS["update_visualization"]["method"],
                API_ENDPOINTS["update_visualization"]["endpoint"]
            ),
        )
        async def update_visualization(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating visualization with params: {params}")
            try:
                visualization_id = params.get("visualization_id")
                visualization_type = params.get("visualization_type")
                name = params.get("name")
                options = params.get("options")
                description = params.get("description")
                result = await sql_queries.update_visualization(visualization_id, visualization_type, name, options, description)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating visualization: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_visualization",
            description=generate_tool_description(
                sql_queries.delete_visualization,
                API_ENDPOINTS["delete_visualization"]["method"],
                API_ENDPOINTS["delete_visualization"]["endpoint"]
            ),
        )
        async def delete_visualization(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting visualization with params: {params}")
            try:
                visualization_id = params.get("visualization_id")
                result = await sql_queries.delete_visualization(visualization_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting visualization: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Command execution tools
        @self.tool(
            name="create_command_context",
            description=generate_tool_description(
                commands.create_context,
                API_ENDPOINTS["create_command_context"]["method"],
                API_ENDPOINTS["create_command_context"]["endpoint"]
            ),
        )
        async def create_command_context(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating command context with params: {params}")
            try:
                cluster_id = params.get("cluster_id")
                language = params.get("language", "python")
                result = await commands.create_context(cluster_id, language)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating command context: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="execute_command",
            description=generate_tool_description(
                commands.execute_command,
                API_ENDPOINTS["execute_command"]["method"],
                API_ENDPOINTS["execute_command"]["endpoint"]
            ),
        )
        async def execute_command(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Executing command with params: {params}")
            try:
                context_id = params.get("context_id")
                command = params.get("command")
                cluster_id = params.get("cluster_id")
                result = await commands.execute_command(context_id, command, cluster_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error executing command: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_command_status",
            description=generate_tool_description(
                commands.get_command_status,
                API_ENDPOINTS["get_command_status"]["method"],
                API_ENDPOINTS["get_command_status"]["endpoint"]
            ),
        )
        async def get_command_status(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting command status with params: {params}")
            try:
                command_id = params.get("command_id")
                context_id = params.get("context_id")
                cluster_id = params.get("cluster_id")
                result = await commands.get_command_status(command_id, context_id, cluster_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting command status: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="cancel_command",
            description=generate_tool_description(
                commands.cancel_command,
                API_ENDPOINTS["cancel_command"]["method"],
                API_ENDPOINTS["cancel_command"]["endpoint"]
            ),
        )
        async def cancel_command(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Canceling command with params: {params}")
            try:
                command_id = params.get("command_id")
                context_id = params.get("context_id")
                cluster_id = params.get("cluster_id")
                result = await commands.cancel_command(command_id, context_id, cluster_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error canceling command: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Library management tools
        @self.tool(
            name="install_libraries",
            description=generate_tool_description(
                libraries.install_libraries,
                API_ENDPOINTS["install_libraries"]["method"],
                API_ENDPOINTS["install_libraries"]["endpoint"]
            ),
        )
        async def install_libraries(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Installing libraries with params: {params}")
            try:
                cluster_id = params.get("cluster_id")
                libraries_config = params.get("libraries")
                result = await libraries.install_libraries(cluster_id, libraries_config)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error installing libraries: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="uninstall_libraries",
            description=generate_tool_description(
                libraries.uninstall_libraries,
                API_ENDPOINTS["uninstall_libraries"]["method"],
                API_ENDPOINTS["uninstall_libraries"]["endpoint"]
            ),
        )
        async def uninstall_libraries(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Uninstalling libraries with params: {params}")
            try:
                cluster_id = params.get("cluster_id")
                libraries_config = params.get("libraries")
                result = await libraries.uninstall_libraries(cluster_id, libraries_config)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error uninstalling libraries: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="list_libraries",
            description=generate_tool_description(
                libraries.list_libraries,
                API_ENDPOINTS["list_libraries"]["method"],
                API_ENDPOINTS["list_libraries"]["endpoint"]
            ),
        )
        async def list_libraries(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing libraries with params: {params}")
            try:
                cluster_id = params.get("cluster_id")
                result = await libraries.list_libraries(cluster_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing libraries: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="list_library_statuses",
            description=generate_tool_description(
                libraries.get_library_statuses,
                API_ENDPOINTS["list_library_statuses"]["method"],
                API_ENDPOINTS["list_library_statuses"]["endpoint"]
            ),
        )
        async def list_library_statuses(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing library statuses with params: {params}")
            try:
                cluster_id = params.get("cluster_id")
                result = await libraries.get_library_statuses(cluster_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing library statuses: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Storage Credentials operations
        @self.tool(
            name="create_storage_credential",
            description=generate_tool_description(
                unity_catalog.create_storage_credential,
                API_ENDPOINTS["create_storage_credential"]["method"],
                API_ENDPOINTS["create_storage_credential"]["endpoint"]
            ),
        )
        async def create_storage_credential(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating storage credential with params: {params}")
            try:
                name = params.get("name")
                aws_iam_role = params.get("aws_iam_role")
                azure_service_principal = params.get("azure_service_principal")
                comment = params.get("comment")
                result = await unity_catalog.create_storage_credential(name, aws_iam_role, azure_service_principal, comment)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating storage credential: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_storage_credential",
            description=generate_tool_description(
                unity_catalog.get_storage_credential,
                API_ENDPOINTS["get_storage_credential"]["method"],
                API_ENDPOINTS["get_storage_credential"]["endpoint"]
            ),
        )
        async def get_storage_credential(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting storage credential with params: {params}")
            try:
                name = params.get("name")
                result = await unity_catalog.get_storage_credential(name)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting storage credential: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="update_storage_credential",
            description=generate_tool_description(
                unity_catalog.update_storage_credential,
                API_ENDPOINTS["update_storage_credential"]["method"],
                API_ENDPOINTS["update_storage_credential"]["endpoint"]
            ),
        )
        async def update_storage_credential(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating storage credential with params: {params}")
            try:
                name = params.get("name")
                new_name = params.get("new_name")
                aws_iam_role = params.get("aws_iam_role")
                azure_service_principal = params.get("azure_service_principal")
                comment = params.get("comment")
                result = await unity_catalog.update_storage_credential(name, new_name, aws_iam_role, azure_service_principal, comment)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating storage credential: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_storage_credential",
            description=generate_tool_description(
                unity_catalog.delete_storage_credential,
                API_ENDPOINTS["delete_storage_credential"]["method"],
                API_ENDPOINTS["delete_storage_credential"]["endpoint"]
            ),
        )
        async def delete_storage_credential(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting storage credential with params: {params}")
            try:
                name = params.get("name")
                result = await unity_catalog.delete_storage_credential(name)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting storage credential: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="list_storage_credentials",
            description=generate_tool_description(
                unity_catalog.list_storage_credentials,
                API_ENDPOINTS["list_storage_credentials"]["method"],
                API_ENDPOINTS["list_storage_credentials"]["endpoint"]
            ),
        )
        async def list_storage_credentials(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing storage credentials with params: {params}")
            try:
                result = await unity_catalog.list_storage_credentials()
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing storage credentials: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Volume operations
        @self.tool(
            name="create_volume",
            description=generate_tool_description(
                unity_catalog.create_volume,
                API_ENDPOINTS["create_volume"]["method"],
                API_ENDPOINTS["create_volume"]["endpoint"]
            ),
        )
        async def create_volume(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating volume with params: {params}")
            try:
                name = params.get("name")
                catalog_name = params.get("catalog_name")
                schema_name = params.get("schema_name")
                volume_type = params.get("volume_type")
                comment = params.get("comment")
                result = await unity_catalog.create_volume(name, catalog_name, schema_name, volume_type, comment)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating volume: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="update_volume",
            description=generate_tool_description(
                unity_catalog.update_volume,
                API_ENDPOINTS["update_volume"]["method"],
                API_ENDPOINTS["update_volume"]["endpoint"]
            ),
        )
        async def update_volume(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating volume with params: {params}")
            try:
                full_name = params.get("full_name")
                new_name = params.get("new_name")
                owner = params.get("owner")
                comment = params.get("comment")
                result = await unity_catalog.update_volume(full_name, new_name, owner, comment)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating volume: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_volume",
            description=generate_tool_description(
                unity_catalog.delete_volume,
                API_ENDPOINTS["delete_volume"]["method"],
                API_ENDPOINTS["delete_volume"]["endpoint"]
            ),
        )
        async def delete_volume(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting volume with params: {params}")
            try:
                full_name = params.get("full_name")
                result = await unity_catalog.delete_volume(full_name)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting volume: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="list_volumes",
            description=generate_tool_description(
                unity_catalog.list_volumes,
                API_ENDPOINTS["list_volumes"]["method"],
                API_ENDPOINTS["list_volumes"]["endpoint"]
            ),
        )
        async def list_volumes(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing volumes with params: {params}")
            try:
                catalog_name = params.get("catalog_name")
                schema_name = params.get("schema_name")
                max_results = params.get("max_results")
                page_token = params.get("page_token")
                result = await unity_catalog.list_volumes(catalog_name, schema_name, max_results, page_token)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing volumes: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Connection operations
        @self.tool(
            name="create_connection",
            description=generate_tool_description(
                unity_catalog.create_connection,
                API_ENDPOINTS["create_connection"]["method"],
                API_ENDPOINTS["create_connection"]["endpoint"]
            ),
        )
        async def create_connection(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating connection with params: {params}")
            try:
                name = params.get("name")
                connection_type = params.get("connection_type")
                options = params.get("options")
                comment = params.get("comment")
                result = await unity_catalog.create_connection(name, connection_type, options, comment)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating connection: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="list_connections",
            description=generate_tool_description(
                unity_catalog.list_connections,
                API_ENDPOINTS["list_connections"]["method"],
                API_ENDPOINTS["list_connections"]["endpoint"]
            ),
        )
        async def list_connections(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing connections with params: {params}")
            try:
                connection_type = params.get("connection_type")
                result = await unity_catalog.list_connections(connection_type)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing connections: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="update_connection",
            description=generate_tool_description(
                unity_catalog.update_connection,
                API_ENDPOINTS["update_connection"]["method"],
                API_ENDPOINTS["update_connection"]["endpoint"]
            ),
        )
        async def update_connection(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating connection with params: {params}")
            try:
                name = params.get("name")
                new_name = params.get("new_name")
                options = params.get("options")
                comment = params.get("comment")
                result = await unity_catalog.update_connection(name, new_name, options, comment)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating connection: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_connection",
            description=generate_tool_description(
                unity_catalog.delete_connection,
                API_ENDPOINTS["delete_connection"]["method"],
                API_ENDPOINTS["delete_connection"]["endpoint"]
            ),
        )
        async def delete_connection(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting connection with params: {params}")
            try:
                name = params.get("name")
                result = await unity_catalog.delete_connection(name)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting connection: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Credential operations
        @self.tool(
            name="create_credential",
            description=generate_tool_description(
                unity_catalog.create_credential,
                API_ENDPOINTS["create_credential"]["method"],
                API_ENDPOINTS["create_credential"]["endpoint"]
            ),
        )
        async def create_credential(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating credential with params: {params}")
            try:
                name = params.get("name")
                credential_type = params.get("credential_type")
                credential_info = params.get("credential_info")
                comment = params.get("comment")
                result = await unity_catalog.create_credential(name, credential_type, credential_info, comment)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating credential: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="list_credentials",
            description=generate_tool_description(
                unity_catalog.list_credentials,
                API_ENDPOINTS["list_credentials"]["method"],
                API_ENDPOINTS["list_credentials"]["endpoint"]
            ),
        )
        async def list_credentials(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing credentials with params: {params}")
            try:
                credential_type = params.get("credential_type")
                result = await unity_catalog.list_credentials(credential_type)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing credentials: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="update_credential",
            description=generate_tool_description(
                unity_catalog.update_credential,
                API_ENDPOINTS["update_credential"]["method"],
                API_ENDPOINTS["update_credential"]["endpoint"]
            ),
        )
        async def update_credential(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating credential with params: {params}")
            try:
                name = params.get("name")
                new_name = params.get("new_name")
                credential_info = params.get("credential_info")
                comment = params.get("comment")
                result = await unity_catalog.update_credential(name, new_name, credential_info, comment)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating credential: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_credential",
            description=generate_tool_description(
                unity_catalog.delete_credential,
                API_ENDPOINTS["delete_credential"]["method"],
                API_ENDPOINTS["delete_credential"]["endpoint"]
            ),
        )
        async def delete_credential(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting credential with params: {params}")
            try:
                name = params.get("name")
                result = await unity_catalog.delete_credential(name)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting credential: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Delta Live Tables (Pipelines) tools
        @self.tool(
            name="create_pipeline",
            description=generate_tool_description(
                pipelines.create_pipeline,
                API_ENDPOINTS["create_pipeline"]["method"],
                API_ENDPOINTS["create_pipeline"]["endpoint"]
            ),
        )
        async def create_pipeline(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating pipeline with params: {params}")
            try:
                name = params.get("name")
                target = params.get("target")
                configuration = params.get("configuration", {})
                libraries = params.get("libraries", [])
                continuous = params.get("continuous", False)
                result = await pipelines.create_pipeline(name, target, configuration, libraries, continuous)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating pipeline: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="list_pipelines",
            description=generate_tool_description(
                pipelines.list_pipelines,
                API_ENDPOINTS["list_pipelines"]["method"],
                API_ENDPOINTS["list_pipelines"]["endpoint"]
            ),
        )
        async def list_pipelines(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing pipelines with params: {params}")
            try:
                max_results = params.get("max_results")
                page_token = params.get("page_token")
                result = await pipelines.list_pipelines(max_results, page_token)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing pipelines: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_pipeline",
            description=generate_tool_description(
                pipelines.get_pipeline,
                API_ENDPOINTS["get_pipeline"]["method"],
                API_ENDPOINTS["get_pipeline"]["endpoint"]
            ),
        )
        async def get_pipeline(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting pipeline with params: {params}")
            try:
                pipeline_id = params.get("pipeline_id")
                result = await pipelines.get_pipeline(pipeline_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting pipeline: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="update_pipeline",
            description=generate_tool_description(
                pipelines.update_pipeline,
                API_ENDPOINTS["update_pipeline"]["method"],
                API_ENDPOINTS["update_pipeline"]["endpoint"]
            ),
        )
        async def update_pipeline(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating pipeline with params: {params}")
            try:
                pipeline_id = params.get("pipeline_id")
                name = params.get("name")
                target = params.get("target")
                configuration = params.get("configuration")
                libraries = params.get("libraries")
                continuous = params.get("continuous")
                result = await pipelines.update_pipeline(pipeline_id, name, target, configuration, libraries, continuous)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating pipeline: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_pipeline",
            description=generate_tool_description(
                pipelines.delete_pipeline,
                API_ENDPOINTS["delete_pipeline"]["method"],
                API_ENDPOINTS["delete_pipeline"]["endpoint"]
            ),
        )
        async def delete_pipeline(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting pipeline with params: {params}")
            try:
                pipeline_id = params.get("pipeline_id")
                result = await pipelines.delete_pipeline(pipeline_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting pipeline: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="start_pipeline_update",
            description=generate_tool_description(
                pipelines.start_pipeline_update,
                API_ENDPOINTS["start_pipeline_update"]["method"],
                API_ENDPOINTS["start_pipeline_update"]["endpoint"]
            ),
        )
        async def start_pipeline_update(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Starting pipeline update with params: {params}")
            try:
                pipeline_id = params.get("pipeline_id")
                full_refresh = params.get("full_refresh", False)
                result = await pipelines.start_pipeline_update(pipeline_id, full_refresh)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error starting pipeline update: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_pipeline_update",
            description=generate_tool_description(
                pipelines.get_pipeline_update,
                API_ENDPOINTS["get_pipeline_update"]["method"],
                API_ENDPOINTS["get_pipeline_update"]["endpoint"]
            ),
        )
        async def get_pipeline_update(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting pipeline update with params: {params}")
            try:
                pipeline_id = params.get("pipeline_id")
                update_id = params.get("update_id")
                result = await pipelines.get_pipeline_update(pipeline_id, update_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting pipeline update: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="list_pipeline_updates",
            description=generate_tool_description(
                pipelines.list_pipeline_updates,
                API_ENDPOINTS["list_pipeline_updates"]["method"],
                API_ENDPOINTS["list_pipeline_updates"]["endpoint"]
            ),
        )
        async def list_pipeline_updates(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing pipeline updates with params: {params}")
            try:
                pipeline_id = params.get("pipeline_id")
                max_results = params.get("max_results")
                page_token = params.get("page_token")
                result = await pipelines.list_pipeline_updates(pipeline_id, max_results, page_token)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing pipeline updates: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Service Principal tools
        @self.tool(
            name="create_service_principal",
            description=generate_tool_description(
                service_principals.create_service_principal,
                API_ENDPOINTS["create_service_principal"]["method"],
                API_ENDPOINTS["create_service_principal"]["endpoint"]
            ),
        )
        async def create_service_principal(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating service principal with params: {params}")
            try:
                display_name = params.get("display_name")
                application_id = params.get("application_id")
                allow_cluster_create = params.get("allow_cluster_create", False)
                result = await service_principals.create_service_principal(display_name, application_id, allow_cluster_create)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating service principal: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="list_service_principals",
            description=generate_tool_description(
                service_principals.list_service_principals,
                API_ENDPOINTS["list_service_principals"]["method"],
                API_ENDPOINTS["list_service_principals"]["endpoint"]
            ),
        )
        async def list_service_principals(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing service principals with params: {params}")
            try:
                page_size = params.get("page_size")
                page_token = params.get("page_token")
                result = await service_principals.list_service_principals(page_size, page_token)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing service principals: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_service_principal",
            description=generate_tool_description(
                service_principals.get_service_principal,
                API_ENDPOINTS["get_service_principal"]["method"],
                API_ENDPOINTS["get_service_principal"]["endpoint"]
            ),
        )
        async def get_service_principal(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting service principal with params: {params}")
            try:
                sp_id = params.get("id")
                result = await service_principals.get_service_principal(sp_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting service principal: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="update_service_principal",
            description=generate_tool_description(
                service_principals.update_service_principal,
                API_ENDPOINTS["update_service_principal"]["method"],
                API_ENDPOINTS["update_service_principal"]["endpoint"]
            ),
        )
        async def update_service_principal(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating service principal with params: {params}")
            try:
                sp_id = params.get("id")
                display_name = params.get("display_name")
                allow_cluster_create = params.get("allow_cluster_create")
                result = await service_principals.update_service_principal(sp_id, display_name, allow_cluster_create)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating service principal: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_service_principal",
            description=generate_tool_description(
                service_principals.delete_service_principal,
                API_ENDPOINTS["delete_service_principal"]["method"],
                API_ENDPOINTS["delete_service_principal"]["endpoint"]
            ),
        )
        async def delete_service_principal(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting service principal with params: {params}")
            try:
                sp_id = params.get("id")
                result = await service_principals.delete_service_principal(sp_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting service principal: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # LakeView (AI/BI) tools
        @self.tool(
            name="list_lakeviews",
            description=generate_tool_description(
                lakeview.list_lakeviews,
                API_ENDPOINTS["list_lakeviews"]["method"],
                API_ENDPOINTS["list_lakeviews"]["endpoint"]
            ),
        )
        async def list_lakeviews(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing Lakeviews")
            try:
                result = await lakeview.list_lakeviews()
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing Lakeviews: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]

        @self.tool(
            name="create_lakeview",
            description=generate_tool_description(
                lakeview.create_lakeview,
                API_ENDPOINTS["create_lakeview"]["method"],
                API_ENDPOINTS["create_lakeview"]["endpoint"]
            ),
        )
        async def create_lakeview(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating Lakeview with params: {params}")
            try:
                name = params.get("name")
                definition = params.get("definition")
                description = params.get("description")
                
                if not name or not definition:
                    return [{"text": json.dumps({"error": "name and definition are required parameters"})}]
                
                result = await lakeview.create_lakeview(name, definition, description)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating Lakeview: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_lakeview",
            description=generate_tool_description(
                lakeview.get_lakeview,
                API_ENDPOINTS["get_lakeview"]["method"],
                API_ENDPOINTS["get_lakeview"]["endpoint"]
            ),
        )
        async def get_lakeview(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting Lakeview with params: {params}")
            try:
                lakeview_id = params.get("id")
                
                if not lakeview_id:
                    return [{"text": json.dumps({"error": "id is a required parameter"})}]
                
                result = await lakeview.get_lakeview(lakeview_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting Lakeview: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="update_lakeview",
            description=generate_tool_description(
                lakeview.update_lakeview,
                API_ENDPOINTS["update_lakeview"]["method"],
                API_ENDPOINTS["update_lakeview"]["endpoint"]
            ),
        )
        async def update_lakeview(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating Lakeview with params: {params}")
            try:
                lakeview_id = params.get("id")
                
                if not lakeview_id:
                    return [{"text": json.dumps({"error": "id is a required parameter"})}]
                
                name = params.get("name")
                definition = params.get("definition")
                description = params.get("description")
                
                result = await lakeview.update_lakeview(lakeview_id, name, definition, description)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating Lakeview: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_lakeview",
            description=generate_tool_description(
                lakeview.delete_lakeview,
                API_ENDPOINTS["delete_lakeview"]["method"],
                API_ENDPOINTS["delete_lakeview"]["endpoint"]
            ),
        )
        async def delete_lakeview(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting Lakeview with params: {params}")
            try:
                lakeview_id = params.get("id")
                
                if not lakeview_id:
                    return [{"text": json.dumps({"error": "id is a required parameter"})}]
                
                result = await lakeview.delete_lakeview(lakeview_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting Lakeview: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="list_warehouses",
            description=generate_tool_description(
                warehouses.list_warehouses,
                API_ENDPOINTS["list_warehouses"]["method"],
                API_ENDPOINTS["list_warehouses"]["endpoint"]
            ),
        )
        async def list_warehouses(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing SQL warehouses")
            try:
                result = await warehouses.list_warehouses()
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing SQL warehouses: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]

        @self.tool(
            name="get_warehouse",
            description=generate_tool_description(
                warehouses.get_warehouse,
                API_ENDPOINTS["get_warehouse"]["method"],
                API_ENDPOINTS["get_warehouse"]["endpoint"]
            ),
        )
        async def get_warehouse(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting SQL warehouse with params: {params}")
            try:
                warehouse_id = params.get("id")
                result = await warehouses.get_warehouse(warehouse_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting SQL warehouse: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]

        @self.tool(
            name="create_warehouse",
            description=generate_tool_description(
                warehouses.create_warehouse,
                API_ENDPOINTS["create_warehouse"]["method"],
                API_ENDPOINTS["create_warehouse"]["endpoint"]
            ),
        )
        async def create_warehouse(params: Dict[str, Any]) -> List[TextContent]:
            """Create a new SQL warehouse."""
            try:
                # Validate required parameters
                required_params = ["name", "cluster_size"]
                for param in required_params:
                    if param not in params:
                        raise ValueError(f"Required parameter '{param}' is missing")
                
                # Validate name format
                if not isinstance(params["name"], str) or not params["name"]:
                    raise ValueError("name must be a non-empty string")
                
                # Validate cluster_size is a valid size
                valid_sizes = ["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"]
                if params["cluster_size"] not in valid_sizes:
                    raise ValueError(f"cluster_size must be one of: {', '.join(valid_sizes)}")
                
                # Validate auto_stop_mins is a positive integer if provided
                if "auto_stop_mins" in params:
                    if not isinstance(params["auto_stop_mins"], int) or params["auto_stop_mins"] < 0:
                        raise ValueError("auto_stop_mins must be a non-negative integer")
                
                # Validate min_num_clusters and max_num_clusters if provided
                if "min_num_clusters" in params and (
                    not isinstance(params["min_num_clusters"], int) or params["min_num_clusters"] < 1
                ):
                    raise ValueError("min_num_clusters must be a positive integer")
                
                if "max_num_clusters" in params and (
                    not isinstance(params["max_num_clusters"], int) or params["max_num_clusters"] < 1
                ):
                    raise ValueError("max_num_clusters must be a positive integer")
                
                # Validate min_num_clusters <= max_num_clusters if both are provided
                if "min_num_clusters" in params and "max_num_clusters" in params:
                    if params["min_num_clusters"] > params["max_num_clusters"]:
                        raise ValueError("min_num_clusters cannot be greater than max_num_clusters")
                
                result = await warehouses.create_warehouse(**params)
                return [TextContent(text=json.dumps(result))]
            except Exception as e:
                return [TextContent(text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="update_warehouse",
            description=generate_tool_description(
                warehouses.update_warehouse,
                API_ENDPOINTS["update_warehouse"]["method"],
                API_ENDPOINTS["update_warehouse"]["endpoint"]
            ),
        )
        async def update_warehouse(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating SQL warehouse with params: {params}")
            try:
                warehouse_id = params.get("id")
                
                if not warehouse_id:
                    return [{"text": json.dumps({"error": "id is a required parameter"})}]
                
                name = params.get("name")
                cluster_size = params.get("cluster_size")
                auto_stop_mins = params.get("auto_stop_mins")
                min_num_clusters = params.get("min_num_clusters")
                max_num_clusters = params.get("max_num_clusters")
                enable_photon = params.get("enable_photon")
                
                result = await warehouses.update_warehouse(
                    warehouse_id,
                    name, 
                    cluster_size, 
                    auto_stop_mins, 
                    min_num_clusters, 
                    max_num_clusters, 
                    enable_photon
                )
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating SQL warehouse: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_warehouse",
            description=generate_tool_description(
                warehouses.delete_warehouse,
                API_ENDPOINTS["delete_warehouse"]["method"],
                API_ENDPOINTS["delete_warehouse"]["endpoint"]
            ),
        )
        async def delete_warehouse(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting SQL warehouse with params: {params}")
            try:
                warehouse_id = params.get("id")
                
                if not warehouse_id:
                    return [{"text": json.dumps({"error": "id is a required parameter"})}]
                
                result = await warehouses.delete_warehouse(warehouse_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting SQL warehouse: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="start_warehouse",
            description=generate_tool_description(
                warehouses.start_warehouse,
                API_ENDPOINTS["start_warehouse"]["method"],
                API_ENDPOINTS["start_warehouse"]["endpoint"]
            ),
        )
        async def start_warehouse(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Starting SQL warehouse with params: {params}")
            try:
                warehouse_id = params.get("id")
                
                if not warehouse_id:
                    return [{"text": json.dumps({"error": "id is a required parameter"})}]
                
                result = await warehouses.start_warehouse(warehouse_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error starting SQL warehouse: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="stop_warehouse",
            description=generate_tool_description(
                warehouses.stop_warehouse,
                API_ENDPOINTS["stop_warehouse"]["method"],
                API_ENDPOINTS["stop_warehouse"]["endpoint"]
            ),
        )
        async def stop_warehouse(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Stopping SQL warehouse with params: {params}")
            try:
                warehouse_id = params.get("id")
                
                if not warehouse_id:
                    return [{"text": json.dumps({"error": "id is a required parameter"})}]
                
                result = await warehouses.stop_warehouse(warehouse_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error stopping SQL warehouse: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Budget tools
        @self.tool(
            name="list_budgets",
            description=generate_tool_description(
                budgets.list_budgets,
                API_ENDPOINTS["list_budgets"]["method"],
                API_ENDPOINTS["list_budgets"]["endpoint"]
            ),
        )
        async def list_budgets(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing budgets")
            try:
                result = await budgets.list_budgets()
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing budgets: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]

        @self.tool(
            name="create_budget",
            description=generate_tool_description(
                budgets.create_budget,
                API_ENDPOINTS["create_budget"]["method"],
                API_ENDPOINTS["create_budget"]["endpoint"]
            ),
        )
        async def create_budget(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating budget with params: {params}")
            try:
                name = params.get("name")
                amount = params.get("amount")
                period = params.get("period")
                filters = params.get("filters")
                start_date = params.get("start_date")
                
                if not name or not amount or not period:
                    return [{"text": json.dumps({"error": "name, amount, and period are required parameters"})}]
                
                result = await budgets.create_budget(name, amount, period, filters, start_date)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating budget: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_budget",
            description=generate_tool_description(
                budgets.get_budget,
                API_ENDPOINTS["get_budget"]["method"],
                API_ENDPOINTS["get_budget"]["endpoint"]
            ),
        )
        async def get_budget(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting budget with params: {params}")
            try:
                budget_id = params.get("budget_id")
                
                if not budget_id:
                    return [{"text": json.dumps({"error": "budget_id is a required parameter"})}]
                
                result = await budgets.get_budget(budget_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting budget: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="update_budget",
            description=generate_tool_description(
                budgets.update_budget,
                API_ENDPOINTS["update_budget"]["method"],
                API_ENDPOINTS["update_budget"]["endpoint"]
            ),
        )
        async def update_budget(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating budget with params: {params}")
            try:
                budget_id = params.get("budget_id")
                
                if not budget_id:
                    return [{"text": json.dumps({"error": "budget_id is a required parameter"})}]
                
                name = params.get("name")
                amount = params.get("amount")
                period = params.get("period")
                filters = params.get("filters")
                start_date = params.get("start_date")
                
                result = await budgets.update_budget(budget_id, name, amount, period, filters, start_date)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating budget: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_budget",
            description=generate_tool_description(
                budgets.delete_budget,
                API_ENDPOINTS["delete_budget"]["method"],
                API_ENDPOINTS["delete_budget"]["endpoint"]
            ),
        )
        async def delete_budget(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting budget with params: {params}")
            try:
                budget_id = params.get("budget_id")
                
                if not budget_id:
                    return [TextContent(type="text", text=json.dumps({"error": "budget_id is a required parameter"}))]
                
                result = await budgets.delete_budget(budget_id)
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error deleting budget: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        # External Locations tools
        @self.tool(
            name="list_external_locations",
            description=generate_tool_description(
                external_locations.mcp_list_external_locations,  # Use the MCP wrapper to handle params properly
                API_ENDPOINTS["list_external_locations"]["method"],
                API_ENDPOINTS["list_external_locations"]["endpoint"]
            ),
        )
        async def list_external_locations(params: Dict[str, Any]) -> List[TextContent]:
            """List external locations in Unity Catalog."""
            try:
                # Version 4 - direct implementation without any intermediary functions
                version = 4
                logger.info(f"List external locations direct implementation, version: {version}")
                
                # Ensure params is not None
                params = params or {}
                logger.info(f"Parameters: {params}")
                
                # Validate max_results is a positive integer if provided
                if "max_results" in params:
                    max_results = params["max_results"]
                    if not isinstance(max_results, int):
                        error_response = {"error": "'max_results' must be an integer", "version": version}
                        return [TextContent(type="text", text=json.dumps(error_response))]
                    if max_results < 0:
                        error_response = {"error": "'max_results' must be a non-negative integer", "version": version}
                        return [TextContent(type="text", text=json.dumps(error_response))]
                    if max_results > 1000:
                        error_response = {"error": "'max_results' cannot exceed 1000", "version": version}
                        return [TextContent(type="text", text=json.dumps(error_response))]
                
                # Get Databricks credentials
                host = os.environ.get("DATABRICKS_HOST")
                token = os.environ.get("DATABRICKS_TOKEN")
                
                if not host or not token:
                    error_response = {"error": "DATABRICKS_HOST and DATABRICKS_TOKEN must be set", "version": version}
                    return [TextContent(type="text", text=json.dumps(error_response))]
                
                # Construct URL and headers
                url = f"{host}/api/2.1/unity-catalog/external-locations"
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                }

                # Import here to avoid module level dependency
                import aiohttp
                
                # Make API request using session
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers, params=params) as response:
                        status = response.status
                        response_body = await response.json()
                        
                        if status == 200:
                            response_body["version"] = version
                            return [TextContent(type="text", text=json.dumps(response_body))]
                        else:
                            error_response = {
                                "error": f"API error: {status} - {response_body.get('message', 'Unknown error')}",
                                "version": version
                            }
                            return [TextContent(type="text", text=json.dumps(error_response))]
                
            except Exception as e:
                logger.error(f"Error listing external locations: {str(e)}")
                error_response = {"error": str(e), "version": 4}
                return [TextContent(type="text", text=json.dumps(error_response))]

        @self.tool(
            name="create_external_location",
            description=generate_tool_description(
                external_locations.create_external_location,
                API_ENDPOINTS["create_external_location"]["method"],
                API_ENDPOINTS["create_external_location"]["endpoint"]
            ),
        )
        async def create_external_location(params: Dict[str, Any]) -> List[TextContent]:
            """Create a new external location in Unity Catalog."""
            try:
                # Validate required parameters
                required_params = ["name", "url", "credential_name"]
                for param in required_params:
                    if param not in params:
                        raise ValueError(f"Required parameter '{param}' is missing")
                
                # Validate name format (common Unity Catalog naming rules)
                if not isinstance(params["name"], str) or not params["name"]:
                    raise ValueError("'name' must be a non-empty string")
                
                # Validate URL format
                if not isinstance(params["url"], str) or not params["url"]:
                    raise ValueError("'url' must be a non-empty string")
                
                # Validate read_only is boolean if provided
                if "read_only" in params and not isinstance(params["read_only"], bool):
                    raise ValueError("'read_only' must be a boolean value")
                
                result = await external_locations.create_external_location(**params)
                return [TextContent(text=json.dumps(result))]
            except Exception as e:
                return [TextContent(text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="get_external_location",
            description=generate_tool_description(
                external_locations.get_external_location,
                API_ENDPOINTS["get_external_location"]["method"],
                API_ENDPOINTS["get_external_location"]["endpoint"]
            ),
        )
        async def get_external_location(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting external location with params: {params}")
            try:
                name = params.get("name")
                
                if not name:
                    return [{"text": json.dumps({"error": "name is a required parameter"})}]
                
                result = await external_locations.get_external_location(name)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting external location: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="update_external_location",
            description=generate_tool_description(
                external_locations.update_external_location,
                API_ENDPOINTS["update_external_location"]["method"],
                API_ENDPOINTS["update_external_location"]["endpoint"]
            ),
        )
        async def update_external_location(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating external location with params: {params}")
            try:
                name = params.get("name")
                
                if not name:
                    return [{"text": json.dumps({"error": "name is a required parameter"})}]
                
                new_name = params.get("new_name")
                url = params.get("url")
                credential_name = params.get("credential_name")
                comment = params.get("comment")
                owner = params.get("owner")
                
                result = await external_locations.update_external_location(name, new_name, url, credential_name, comment, owner)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating external location: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_external_location",
            description=generate_tool_description(
                external_locations.delete_external_location,
                API_ENDPOINTS["delete_external_location"]["method"],
                API_ENDPOINTS["delete_external_location"]["endpoint"]
            ),
        )
        async def delete_external_location(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting external location with params: {params}")
            try:
                name = params.get("name")
                
                if not name:
                    return [{"text": json.dumps({"error": "name is a required parameter"})}]
                
                result = await external_locations.delete_external_location(name)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting external location: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]

async def main() -> None:
    """Main entry point for the Databricks MCP server."""
    logger.info("Starting Databricks MCP server")
    try:
        server = DatabricksMCPServer()
        # Use the built-in method for running the server via stdio
        await server.run_stdio_async()
    except Exception as e:
        logger.error(f"Error in Databricks MCP server: {str(e)}", exc_info=True)
        # Re-raise to indicate error to caller
        raise

if __name__ == "__main__":
    try:
        # Turn off buffering in stdout if supported
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(line_buffering=True)
        
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1) 