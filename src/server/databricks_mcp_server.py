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

from src.api import clusters, dbfs, jobs, notebooks, sql, unity_catalog, sql_queries, commands, libraries, workspace, pipelines, service_principals, lakeview, warehouses, budgets, external_locations
from src.core.config import settings

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
            description="List all Databricks clusters",
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
            description="Create a new Databricks cluster with parameters: cluster_name (required), spark_version (required), node_type_id (required), num_workers, autotermination_minutes",
        )
        async def create_cluster(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating cluster with params: {params}")
            try:
                result = await clusters.create_cluster(params)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating cluster: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="terminate_cluster",
            description="Terminate a Databricks cluster with parameter: cluster_id (required)",
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
            description="Get information about a specific Databricks cluster with parameter: cluster_id (required)",
        )
        async def get_cluster(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting cluster info with params: {params}")
            try:
                result = await clusters.get_cluster(params.get("cluster_id"))
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting cluster info: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="start_cluster",
            description="Start a terminated Databricks cluster with parameter: cluster_id (required)",
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
            description="Restart a Databricks cluster with parameter: cluster_id (required)",
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
            description="Resize a Databricks cluster with parameters: cluster_id (required), num_workers (required)",
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
            description="Permanently delete a Databricks cluster with parameter: cluster_id (required)",
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
            description="List all Databricks jobs",
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
            description="Run a Databricks job with parameters: job_id (required), notebook_params (optional)",
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
            description="Update an existing job with parameters: job_id (required), new_settings (required)",
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
            description="Reset a job with parameter: job_id (required)",
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
            description="Get output for a job run with parameter: run_id (required)",
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
            description="List runs for a job with parameters: job_id (required), active_only (optional), completed_only (optional), offset (optional), limit (optional)",
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
            description="Cancel a job run with parameter: run_id (required)",
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
            description="Delete a job run with parameter: run_id (required)",
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
            description="Delete a job with parameter: job_id (required)",
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
            description="List notebooks in a workspace directory with parameter: path (required)",
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
            description="Export a notebook from the workspace with parameters: path (required), format (optional, one of: SOURCE, HTML, JUPYTER, DBC)",
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
            description="List files and directories in a DBFS path with parameter: dbfs_path (required)",
        )
        async def list_files(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing files with params: {params}")
            try:
                result = await dbfs.list_files(params.get("dbfs_path"))
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing files: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Workspace tools
        @self.tool(
            name="list_workspace_files",
            description="List files and directories in a workspace path with parameter: path (required)",
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
            description="Import a file to the workspace with parameters: path (required), format (required), content (required), language (optional), overwrite (optional)",
        )
        async def import_workspace_file(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Importing workspace file with params: {params}")
            try:
                path = params.get("path")
                format = params.get("format")
                content = params.get("content")
                language = params.get("language")
                overwrite = params.get("overwrite", False)
                result = await workspace.import_files(path, format, content, language, overwrite)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error importing workspace file: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="export_workspace_file",
            description="Export a file from the workspace with parameters: path (required), format (required)",
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
            description="Delete a file or directory from the workspace with parameters: path (required), recursive (optional)",
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
            description="Get the status of a file or directory in the workspace with parameter: path (required)",
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
            description="Create directories in the workspace with parameter: path (required)",
        )
        async def create_workspace_directory(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating workspace directory with params: {params}")
            try:
                path = params.get("path")
                result = await workspace.mkdirs(path)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating workspace directory: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # SQL tools
        @self.tool(
            name="execute_sql",
            description="Execute a SQL statement with parameters: statement (required), warehouse_id (required), catalog (optional), schema (optional)",
        )
        async def execute_sql(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Executing SQL with params: {params}")
            try:
                statement = params.get("statement")
                warehouse_id = params.get("warehouse_id")
                catalog = params.get("catalog")
                schema = params.get("schema")
                
                result = await sql.execute_sql(statement, warehouse_id, catalog, schema)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error executing SQL: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Unity Catalog tools
        
        # Catalog operations
        @self.tool(
            name="list_catalogs",
            description="List catalogs in the Unity Catalog with optional parameter: max_results",
        )
        async def list_catalogs(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing catalogs with params: {params}")
            try:
                max_results = params.get("max_results")
                result = await unity_catalog.list_catalogs(max_results)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing catalogs: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="create_catalog",
            description="Create a new catalog in the Unity Catalog with parameters: name (required), comment (optional)",
        )
        async def create_catalog(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating catalog with params: {params}")
            try:
                name = params.get("name")
                comment = params.get("comment")
                result = await unity_catalog.create_catalog(name, comment)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating catalog: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_catalog",
            description="Get details of a catalog in the Unity Catalog with parameter: name (required)",
        )
        async def get_catalog(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting catalog with params: {params}")
            try:
                name = params.get("name")
                result = await unity_catalog.get_catalog(name)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting catalog: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="update_catalog",
            description="Update a catalog in the Unity Catalog with parameters: name (required), new_name (optional), comment (optional)",
        )
        async def update_catalog(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating catalog with params: {params}")
            try:
                name = params.get("name")
                new_name = params.get("new_name")
                comment = params.get("comment")
                result = await unity_catalog.update_catalog(name, new_name, comment)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating catalog: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_catalog",
            description="Delete a catalog from the Unity Catalog with parameter: name (required)",
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
            description="List schemas in the Unity Catalog with parameters: catalog_name (optional), max_results (optional)",
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
            description="Create a new schema in the Unity Catalog with parameters: name (required), catalog_name (required), comment (optional)",
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
            description="Get details of a schema in the Unity Catalog with parameter: full_name (required, format: catalog.schema)",
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
            description="Update a schema in the Unity Catalog with parameters: full_name (required), new_name (optional), comment (optional)",
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
            description="Delete a schema from the Unity Catalog with parameter: full_name (required, format: catalog.schema)",
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
            description="List tables in the Unity Catalog with parameters: catalog_name (optional), schema_name (optional), max_results (optional), page_token (optional)",
        )
        async def list_tables(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing tables with params: {params}")
            try:
                catalog_name = params.get("catalog_name")
                schema_name = params.get("schema_name")
                max_results = params.get("max_results")
                page_token = params.get("page_token")
                result = await unity_catalog.list_tables(catalog_name, schema_name, max_results, page_token)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing tables: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_table",
            description="Get details of a table in the Unity Catalog with parameter: full_name (required, format: catalog.schema.table)",
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
            description="Create a new SQL query with parameters: name (required), description (optional), query (optional), parent (optional), run_as_role (optional)",
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
            description="List SQL queries with parameters: page_size (optional), page (optional), order (optional), q (optional)",
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
            description="Get a specific SQL query by ID with parameter: query_id (required)",
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
            description="Update an existing SQL query with parameters: query_id (required), name (optional), description (optional), query (optional), run_as_role (optional)",
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
            description="Delete a SQL query with parameter: query_id (required)",
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
            description="Run a SQL query and return results with parameters: query_id (required), parameters (optional), warehouse_id (optional)",
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
            description="Create a new SQL dashboard with parameters: name (required), description (optional), tags (optional)",
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
            description="List SQL dashboards with parameters: page_size (optional), page (optional), order (optional), q (optional)",
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
            description="Get a specific dashboard by ID with parameter: dashboard_id (required)",
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
            description="Update an existing dashboard with parameters: dashboard_id (required), name (optional), description (optional), tags (optional)",
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
            description="Delete a dashboard with parameter: dashboard_id (required)",
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
            description="Create a new alert for a SQL query with parameters: query_id (required), name (required), options (required)",
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
            description="List SQL alerts with parameters: page_size (optional), page (optional)",
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
            description="Get a specific alert by ID with parameter: alert_id (required)",
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
            description="Update an existing alert with parameters: alert_id (required), name (optional), options (optional)",
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
            description="Delete an alert with parameter: alert_id (required)",
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
            description="Create a new visualization for a SQL query with parameters: query_id (required), visualization_type (required), name (required), options (required), description (optional)",
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
            description="Update an existing visualization with parameters: visualization_id (required), visualization_type (optional), name (optional), options (optional), description (optional)",
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
            description="Delete a visualization with parameter: visualization_id (required)",
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
            description="Create a command execution context with parameters: cluster_id (required), language (optional)",
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
            description="Execute a command in a context with parameters: context_id (required), command (required), cluster_id (required)",
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
            description="Get status of a command with parameters: command_id (required), context_id (required), cluster_id (required)",
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
            description="Cancel a command with parameters: command_id (required), context_id (required), cluster_id (required)",
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
            description="Install libraries on a cluster with parameters: cluster_id (required), libraries (required)",
        )
        async def install_libraries(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Installing libraries with params: {params}")
            try:
                cluster_id = params.get("cluster_id")
                libraries_list = params.get("libraries")
                result = await libraries.install_libraries(cluster_id, libraries_list)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error installing libraries: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="uninstall_libraries",
            description="Uninstall libraries from a cluster with parameters: cluster_id (required), libraries (required)",
        )
        async def uninstall_libraries(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Uninstalling libraries with params: {params}")
            try:
                cluster_id = params.get("cluster_id")
                libraries_list = params.get("libraries")
                result = await libraries.uninstall_libraries(cluster_id, libraries_list)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error uninstalling libraries: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="list_libraries",
            description="List libraries installed on a cluster with parameter: cluster_id (required)",
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
            description="Get status of libraries on a cluster with parameter: cluster_id (required)",
        )
        async def list_library_statuses(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing library statuses with params: {params}")
            try:
                cluster_id = params.get("cluster_id")
                result = await libraries.list_library_statuses(cluster_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing library statuses: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # Storage Credentials operations
        @self.tool(
            name="create_storage_credential",
            description="Create a storage credential in Unity Catalog with parameters: name (required), aws_iam_role (optional), azure_service_principal (optional), comment (optional)",
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
            description="Get details of a storage credential with parameter: name (required)",
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
            description="Update a storage credential with parameters: name (required), new_name (optional), aws_iam_role (optional), azure_service_principal (optional), comment (optional)",
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
            description="Delete a storage credential with parameter: name (required)",
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
            description="List storage credentials in Unity Catalog",
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
            description="Create a volume in Unity Catalog with parameters: name (required), catalog_name (required), schema_name (required), volume_type (optional), comment (optional)",
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
            description="Update a volume with parameters: full_name (required), new_name (optional), owner (optional), comment (optional)",
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
            description="Delete a volume with parameter: full_name (required)",
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
            description="List volumes in Unity Catalog with parameters: catalog_name (optional), schema_name (optional), max_results (optional), page_token (optional)",
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
            description="Create a connection in Unity Catalog with parameters: name (required), connection_type (required), options (required), comment (optional)",
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
            description="List connections in Unity Catalog with parameter: connection_type (optional)",
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
            description="Update a connection with parameters: name (required), new_name (optional), options (optional), comment (optional)",
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
            description="Delete a connection with parameter: name (required)",
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
            description="Create a credential in Unity Catalog with parameters: name (required), credential_type (required), credential_info (required), comment (optional)",
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
            description="List credentials in Unity Catalog with parameter: credential_type (optional)",
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
            description="Update a credential with parameters: name (required), new_name (optional), credential_info (optional), comment (optional)",
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
            description="Delete a credential with parameter: name (required)",
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
            description="Create a Delta Live Table pipeline with parameters: name (required), target (required), configuration (optional), libraries (optional), continuous (optional)",
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
            description="List Delta Live Table pipelines with parameters: max_results (optional), page_token (optional)",
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
            description="Get details of a Delta Live Table pipeline with parameter: pipeline_id (required)",
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
            description="Update a Delta Live Table pipeline with parameters: pipeline_id (required), name (optional), target (optional), configuration (optional), libraries (optional), continuous (optional)",
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
            description="Delete a Delta Live Table pipeline with parameter: pipeline_id (required)",
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
            description="Start an update for a Delta Live Table pipeline with parameters: pipeline_id (required), full_refresh (optional)",
        )
        async def start_pipeline_update(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Starting pipeline update with params: {params}")
            try:
                pipeline_id = params.get("pipeline_id")
                full_refresh = params.get("full_refresh", False)
                result = await pipelines.start_update(pipeline_id, full_refresh)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error starting pipeline update: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_pipeline_update",
            description="Get details of a Delta Live Table pipeline update with parameters: pipeline_id (required), update_id (required)",
        )
        async def get_pipeline_update(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting pipeline update with params: {params}")
            try:
                pipeline_id = params.get("pipeline_id")
                update_id = params.get("update_id")
                result = await pipelines.get_update(pipeline_id, update_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting pipeline update: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="list_pipeline_updates",
            description="List updates for a Delta Live Table pipeline with parameters: pipeline_id (required), max_results (optional), page_token (optional)",
        )
        async def list_pipeline_updates(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing pipeline updates with params: {params}")
            try:
                pipeline_id = params.get("pipeline_id")
                max_results = params.get("max_results")
                page_token = params.get("page_token")
                result = await pipelines.list_updates(pipeline_id, max_results, page_token)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing pipeline updates: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        # DBFS tools (additional)
        @self.tool(
            name="import_file",
            description="Import a file to DBFS with parameters: source_path (required), target_path (required), overwrite (optional)",
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
            description="Create a directory in DBFS with parameter: path (required)",
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
            description="Delete a file or directory from DBFS with parameters: path (required), recursive (optional)",
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
            description="Read a file from DBFS with parameters: path (required), offset (optional), length (optional)",
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
            description="Move a file in DBFS with parameters: source_path (required), target_path (required)",
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
        
        # Service Principal tools
        @self.tool(
            name="create_service_principal",
            description="Create a service principal with parameters: display_name (required), application_id (required), allow_cluster_create (optional)",
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
            description="List service principals with parameters: page_size (optional), page_token (optional)",
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
            description="Get details of a service principal with parameter: id (required)",
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
            description="Update a service principal with parameters: id (required), display_name (optional), allow_cluster_create (optional)",
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
            description="Delete a service principal with parameter: id (required)",
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
            description="List LakeViews with parameters: max_results (optional), page_token (optional)",
        )
        async def list_lakeviews(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing LakeViews with params: {params}")
            try:
                max_results = params.get("max_results")
                page_token = params.get("page_token")
                result = await lakeview.list_lakeviews(max_results, page_token)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error listing LakeViews: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="create_lakeview",
            description="Create a LakeView with parameters: name (required), catalog_name (required), schema_name (required), table_name (required), description (optional)",
        )
        async def create_lakeview(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating LakeView with params: {params}")
            try:
                name = params.get("name")
                catalog_name = params.get("catalog_name")
                schema_name = params.get("schema_name")
                table_name = params.get("table_name")
                description = params.get("description")
                result = await lakeview.create_lakeview(name, catalog_name, schema_name, table_name, description)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error creating LakeView: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="get_lakeview",
            description="Get details of a LakeView with parameter: lakeview_id (required)",
        )
        async def get_lakeview(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting LakeView with params: {params}")
            try:
                lakeview_id = params.get("lakeview_id")
                result = await lakeview.get_lakeview(lakeview_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error getting LakeView: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="update_lakeview",
            description="Update a LakeView with parameters: lakeview_id (required), name (optional), description (optional)",
        )
        async def update_lakeview(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Updating LakeView with params: {params}")
            try:
                lakeview_id = params.get("lakeview_id")
                name = params.get("name")
                description = params.get("description")
                result = await lakeview.update_lakeview(lakeview_id, name, description)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error updating LakeView: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]
        
        @self.tool(
            name="delete_lakeview",
            description="Delete a LakeView with parameter: lakeview_id (required)",
        )
        async def delete_lakeview(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Deleting LakeView with params: {params}")
            try:
                lakeview_id = params.get("lakeview_id")
                result = await lakeview.delete_lakeview(lakeview_id)
                return [{"text": json.dumps(result)}]
            except Exception as e:
                logger.error(f"Error deleting LakeView: {str(e)}")
                return [{"text": json.dumps({"error": str(e)})}]

        # Budget Management Tools
        @self.tool(
            name="create_budget",
            description="Create a new budget with parameters: name (required), budget_configuration (required), description (optional)",
        )
        async def create_budget(params: Dict[str, Any]) -> List[TextContent]:
            try:
                name = params.get("name")
                budget_configuration = params.get("budget_configuration")
                description = params.get("description")
                
                if not name:
                    return [TextContent(type="text", text="Error: name is required")]
                if not budget_configuration:
                    return [TextContent(type="text", text="Error: budget_configuration is required")]
                
                result = await budgets.create_budget(name, budget_configuration, description)
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                logger.error(f"Error creating budget: {str(e)}")
                return [TextContent(type="text", text=f"Error creating budget: {str(e)}")]

        @self.tool(
            name="list_budgets",
            description="List all budgets",
        )
        async def list_budgets(params: Dict[str, Any]) -> List[TextContent]:
            try:
                result = await budgets.list_budgets()
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                logger.error(f"Error listing budgets: {str(e)}")
                return [TextContent(type="text", text=f"Error listing budgets: {str(e)}")]

        @self.tool(
            name="get_budget",
            description="Get details of a budget with parameter: budget_id (required)",
        )
        async def get_budget(params: Dict[str, Any]) -> List[TextContent]:
            try:
                budget_id = params.get("budget_id")
                if not budget_id:
                    return [TextContent(type="text", text="Error: budget_id is required")]
                
                result = await budgets.get_budget(budget_id)
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                logger.error(f"Error getting budget: {str(e)}")
                return [TextContent(type="text", text=f"Error getting budget: {str(e)}")]

        @self.tool(
            name="update_budget",
            description="Update a budget with parameters: budget_id (required), name (optional), budget_configuration (optional), description (optional)",
        )
        async def update_budget(params: Dict[str, Any]) -> List[TextContent]:
            try:
                budget_id = params.get("budget_id")
                name = params.get("name")
                budget_configuration = params.get("budget_configuration")
                description = params.get("description")
                
                if not budget_id:
                    return [TextContent(type="text", text="Error: budget_id is required")]
                
                result = await budgets.update_budget(budget_id, name, budget_configuration, description)
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                logger.error(f"Error updating budget: {str(e)}")
                return [TextContent(type="text", text=f"Error updating budget: {str(e)}")]

        @self.tool(
            name="delete_budget",
            description="Delete a budget with parameter: budget_id (required)",
        )
        async def delete_budget(params: Dict[str, Any]) -> List[TextContent]:
            try:
                budget_id = params.get("budget_id")
                if not budget_id:
                    return [TextContent(type="text", text="Error: budget_id is required")]
                
                result = await budgets.delete_budget(budget_id)
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                logger.error(f"Error deleting budget: {str(e)}")
                return [TextContent(type="text", text=f"Error deleting budget: {str(e)}")]

        # External Location Tools
        @self.tool(
            name="create_external_location",
            description="Create an external location with parameters: name (required), url (required), credential_name (required), comment (optional), read_only (optional)",
        )
        async def create_external_location(params: Dict[str, Any]) -> List[TextContent]:
            try:
                name = params.get("name")
                url = params.get("url")
                credential_name = params.get("credential_name")
                comment = params.get("comment")
                read_only = params.get("read_only")
                
                if not name:
                    return [TextContent(type="text", text="Error: name is required")]
                if not url:
                    return [TextContent(type="text", text="Error: url is required")]
                if not credential_name:
                    return [TextContent(type="text", text="Error: credential_name is required")]
                
                result = await external_locations.create_external_location(name, url, credential_name, comment, read_only)
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                logger.error(f"Error creating external location: {str(e)}")
                return [TextContent(type="text", text=f"Error creating external location: {str(e)}")]

        @self.tool(
            name="list_external_locations",
            description="List external locations with parameter: max_results (optional)",
        )
        async def list_external_locations(params: Dict[str, Any]) -> List[TextContent]:
            try:
                max_results = params.get("max_results")
                
                result = await external_locations.list_external_locations(max_results)
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                logger.error(f"Error listing external locations: {str(e)}")
                return [TextContent(type="text", text=f"Error listing external locations: {str(e)}")]

        @self.tool(
            name="get_external_location",
            description="Get details of an external location with parameter: name (required)",
        )
        async def get_external_location(params: Dict[str, Any]) -> List[TextContent]:
            try:
                name = params.get("name")
                if not name:
                    return [TextContent(type="text", text="Error: name is required")]
                
                result = await external_locations.get_external_location(name)
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                logger.error(f"Error getting external location: {str(e)}")
                return [TextContent(type="text", text=f"Error getting external location: {str(e)}")]

        @self.tool(
            name="update_external_location",
            description="Update an external location with parameters: name (required), new_name (optional), url (optional), credential_name (optional), comment (optional), owner (optional), read_only (optional)",
        )
        async def update_external_location(params: Dict[str, Any]) -> List[TextContent]:
            try:
                name = params.get("name")
                new_name = params.get("new_name")
                url = params.get("url")
                credential_name = params.get("credential_name")
                comment = params.get("comment")
                owner = params.get("owner")
                read_only = params.get("read_only")
                
                if not name:
                    return [TextContent(type="text", text="Error: name is required")]
                
                result = await external_locations.update_external_location(name, new_name, url, credential_name, comment, owner, read_only)
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                logger.error(f"Error updating external location: {str(e)}")
                return [TextContent(type="text", text=f"Error updating external location: {str(e)}")]

        @self.tool(
            name="delete_external_location",
            description="Delete an external location with parameter: name (required)",
        )
        async def delete_external_location(params: Dict[str, Any]) -> List[TextContent]:
            try:
                name = params.get("name")
                if not name:
                    return [TextContent(type="text", text="Error: name is required")]
                
                result = await external_locations.delete_external_location(name)
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                logger.error(f"Error deleting external location: {str(e)}")
                return [TextContent(type="text", text=f"Error deleting external location: {str(e)}")]

        # SQL Warehouse Tools
        @self.tool(
            name="create_warehouse",
            description="Create a SQL warehouse with parameters: name (required), cluster_size (required), auto_stop_mins (optional), min_num_clusters (optional), max_num_clusters (optional), enable_photon (optional)",
        )
        async def create_warehouse(params: Dict[str, Any]) -> List[TextContent]:
            try:
                name = params.get("name")
                cluster_size = params.get("cluster_size")
                auto_stop_mins = params.get("auto_stop_mins")
                min_num_clusters = params.get("min_num_clusters")
                max_num_clusters = params.get("max_num_clusters")
                enable_photon = params.get("enable_photon")
                additional_params = params.get("additional_params")
                
                if not name:
                    return [TextContent(type="text", text="Error: name is required")]
                if not cluster_size:
                    return [TextContent(type="text", text="Error: cluster_size is required")]
                
                result = await warehouses.create_warehouse(
                    name, 
                    cluster_size, 
                    auto_stop_mins, 
                    min_num_clusters, 
                    max_num_clusters, 
                    enable_photon, 
                    additional_params
                )
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                logger.error(f"Error creating SQL warehouse: {str(e)}")
                return [TextContent(type="text", text=f"Error creating SQL warehouse: {str(e)}")]

        @self.tool(
            name="start_warehouse",
            description="Start a SQL warehouse with parameter: id (required)",
        )
        async def start_warehouse(params: Dict[str, Any]) -> List[TextContent]:
            try:
                warehouse_id = params.get("id")
                if not warehouse_id:
                    return [TextContent(type="text", text="Error: id is required")]
                
                result = await warehouses.start_warehouse(warehouse_id)
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                logger.error(f"Error starting SQL warehouse: {str(e)}")
                return [TextContent(type="text", text=f"Error starting SQL warehouse: {str(e)}")]

        @self.tool(
            name="update_warehouse",
            description="Update a SQL warehouse with parameters: id (required), name (optional), cluster_size (optional), auto_stop_mins (optional), min_num_clusters (optional), max_num_clusters (optional), enable_photon (optional)",
        )
        async def update_warehouse(params: Dict[str, Any]) -> List[TextContent]:
            try:
                warehouse_id = params.get("id")
                name = params.get("name")
                cluster_size = params.get("cluster_size")
                auto_stop_mins = params.get("auto_stop_mins")
                min_num_clusters = params.get("min_num_clusters")
                max_num_clusters = params.get("max_num_clusters")
                enable_photon = params.get("enable_photon")
                additional_params = params.get("additional_params")
                
                if not warehouse_id:
                    return [TextContent(type="text", text="Error: id is required")]
                
                result = await warehouses.update_warehouse(
                    warehouse_id, 
                    name, 
                    cluster_size, 
                    auto_stop_mins, 
                    min_num_clusters, 
                    max_num_clusters, 
                    enable_photon, 
                    additional_params
                )
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                logger.error(f"Error updating SQL warehouse: {str(e)}")
                return [TextContent(type="text", text=f"Error updating SQL warehouse: {str(e)}")]

        @self.tool(
            name="delete_warehouse",
            description="Delete a SQL warehouse with parameter: id (required)",
        )
        async def delete_warehouse(params: Dict[str, Any]) -> List[TextContent]:
            try:
                warehouse_id = params.get("id")
                if not warehouse_id:
                    return [TextContent(type="text", text="Error: id is required")]
                
                result = await warehouses.delete_warehouse(warehouse_id)
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                logger.error(f"Error deleting SQL warehouse: {str(e)}")
                return [TextContent(type="text", text=f"Error deleting SQL warehouse: {str(e)}")]

        @self.tool(
            name="stop_warehouse",
            description="Stop a SQL warehouse with parameter: id (required)",
        )
        async def stop_warehouse(params: Dict[str, Any]) -> List[TextContent]:
            try:
                warehouse_id = params.get("id")
                if not warehouse_id:
                    return [TextContent(type="text", text="Error: id is required")]
                
                result = await warehouses.stop_warehouse(warehouse_id)
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                logger.error(f"Error stopping SQL warehouse: {str(e)}")
                return [TextContent(type="text", text=f"Error stopping SQL warehouse: {str(e)}")]

        @self.tool(
            name="list_warehouses",
            description="List all SQL warehouses",
        )
        async def list_warehouses(params: Dict[str, Any]) -> List[TextContent]:
            try:
                result = await warehouses.list_warehouses()
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                logger.error(f"Error listing SQL warehouses: {str(e)}")
                return [TextContent(type="text", text=f"Error listing SQL warehouses: {str(e)}")]

        @self.tool(
            name="get_warehouse",
            description="Get information about a specific SQL warehouse with parameter: id (required)",
        )
        async def get_warehouse(params: Dict[str, Any]) -> List[TextContent]:
            try:
                warehouse_id = params.get("id")
                if not warehouse_id:
                    return [TextContent(type="text", text="Error: id is required")]
                
                result = await warehouses.get_warehouse(warehouse_id)
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                logger.error(f"Error getting SQL warehouse: {str(e)}")
                return [TextContent(type="text", text=f"Error getting SQL warehouse: {str(e)}")]


async def main():
    """Main entry point for the MCP server."""
    try:
        logger.info("Starting Databricks MCP server")
        server = DatabricksMCPServer()
        
        # Use the built-in method for stdio servers
        # This is the recommended approach for MCP servers
        await server.run_stdio_async()
            
    except Exception as e:
        logger.error(f"Error in Databricks MCP server: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    # Turn off buffering in stdout
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(line_buffering=True)
    
    asyncio.run(main()) 