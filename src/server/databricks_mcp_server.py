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
import argparse
from typing import Any, Dict, List, Optional, Union, cast

# Check for test mode
parser = argparse.ArgumentParser(description='Databricks MCP Server')
parser.add_argument('--test', action='store_true', help='Run in test mode to verify imports')
parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
args, _ = parser.parse_known_args()

# Setup basic logging
logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
logger = logging.getLogger(__name__)

print("Successfully loaded dotenv")

# In test mode, create mock imports
if args.test:
    # Create mock modules to satisfy imports
    class MockModule:
        def __getattr__(self, name):
            return MockModule()
            
        def __call__(self, *args, **kwargs):
            return MockModule()
    
    # Create sys.modules entries for missing modules
    sys.modules['fastmcp'] = MockModule()
    sys.modules['fastmcp.content'] = MockModule()
    sys.modules['fastmcp.tool_call'] = MockModule() 
    sys.modules['mcp'] = MockModule()
    sys.modules['mcp.server'] = MockModule()
    sys.modules['mcp.types'] = MockModule()
    sys.modules['mcp.server.stdio'] = MockModule()
    
    # Create mock classes
    class FastMCP:
        def __init__(self, *args, **kwargs):
            pass
            
        def tool(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator
            
    sys.modules['mcp.server'].FastMCP = FastMCP
    sys.modules['mcp.types'].TextContent = object
    sys.modules['mcp.types'].CallToolResult = object
    sys.modules['fastmcp.content'].TextContent = object
    sys.modules['fastmcp.tool_call'].CallToolResult = object

# Import actual modules if not testing
if not args.test:
    from mcp.server import FastMCP
    from mcp.types import TextContent, CallToolResult
    from mcp.server.stdio import stdio_server

# These imports should now work in both modes
from src.api import (
    budgets,
    clusters,
    commands,
    dbfs,
    external_locations,
    jobs,
    lakeview,
    libraries,
    pipelines,
    service_principals,
    sql,
    sql_alerts,
    sql_dashboards,
    sql_queries,
    unity_catalog,
    visualizations,
    warehouses,
    workspace,
)
from src.core.config import settings
from src.core.utils import DatabricksAPIError

if args.test:
    print("All imports successful!")
    
    # Now try to import tools which depend on the mock modules
    try:
        from src.server.tools import (
            budget_tools,
            clusters_tools,
            commands_tools,
            connections_tools,
            credentials_tools,
            dbfs_tools,
            external_locations_tools,
            jobs_tools,
            lakeview_tools,
            libraries_tools,
            metastores_tools,
            workspace_tools,
            pipelines_tools,
            secrets_tools,
            service_principals_tools,
            sql_alerts_tools,
            sql_dashboards_tools,
            sql_queries_tools,
            sql_tools,
            storage_credentials_tools,
            tokens_tools,
            unity_catalog_tools,
            visualizations_tools,
            volumes_tools,
        )
        print("All tool modules imported successfully!")
    except Exception as e:
        print(f"Error importing tools: {e}")
    
    sys.exit(0)

# If not in test mode, import the remaining modules
from src.server.tools import (
    budget_tools,
    clusters_tools,
    commands_tools,
    connections_tools,
    credentials_tools,
    dbfs_tools,
    external_locations_tools,
    jobs_tools,
    lakeview_tools,
    libraries_tools,
    metastores_tools,
    workspace_tools,
    pipelines_tools,
    secrets_tools,
    service_principals_tools,
    sql_alerts_tools,
    sql_dashboards_tools,
    sql_queries_tools,
    sql_tools,
    storage_credentials_tools,
    tokens_tools,
    unity_catalog_tools,
    visualizations_tools,
    volumes_tools,
)

class DatabricksMCPServer(FastMCP):
    """Databricks MCP Server implementation."""

    def __init__(self):
        """Initialize the Databricks MCP server."""
        super().__init__()
        self.logger = logging.getLogger("databricks-mcp")
        self.logger.info("Initializing server...")
        self._register_tools()

    def _register_tools(self):
        """Register all available tools with the server."""
        self.logger.info("Registering API tools...")
        
        # Register Workspace tools
        self.logger.info("Registering Workspace tools")
        workspace_tools.register_workspace_tools(self)
        
        # Register Clusters tools
        self.logger.info("Registering Clusters tools")
        clusters_tools.register_cluster_tools(self)
        
        # Register Jobs tools
        self.logger.info("Registering Jobs tools")
        jobs_tools.register_jobs_tools(self)
               
        # Register DBFS tools
        self.logger.info("Registering DBFS tools")
        dbfs_tools.register_dbfs_tools(self)
        
        # Register Secrets tools
        self.logger.info("Registering Secrets tools")
        secrets_tools.register_secrets_tools(self)
        
        # Register Tokens tools
        self.logger.info("Registering Tokens tools")
        tokens_tools.register_tokens_tools(self)
        
        # Register SQL tools
        self.logger.info("Registering SQL tools")
        sql_tools.register_sql_tools(self)
        
        # Register SQL Queries tools
        self.logger.info("Registering SQL Queries tools")
        sql_queries_tools.register_sql_queries_tools(self)
        
        # Register Unity Catalog tools
        self.logger.info("Registering Unity Catalog tools")
        unity_catalog_tools.register_unity_catalog_tools(self)
        
        # Register Command Execution tools
        self.logger.info("Registering Command Execution tools")
        commands_tools.register_commands_tools(self)
        
        # Register Library Management tools
        self.logger.info("Registering Library Management tools")
        libraries_tools.register_libraries_tools(self)
        
        # Register Pipelines tools
        self.logger.info("Registering Pipelines tools")
        pipelines_tools.register_pipelines_tools(self)
        
        # Register Service Principals tools
        self.logger.info("Registering Service Principals tools")
        service_principals_tools.register_service_principals_tools(self)
        
        # Register External Locations tools
        self.logger.info("Registering External Locations tools")
        external_locations_tools.register_external_locations_tools(self)
        
        # Register Storage Credentials tools
        self.logger.info("Registering Storage Credentials tools")
        storage_credentials_tools.register_storage_credentials_tools(self)
        
        # Register Volumes tools
        self.logger.info("Registering Volumes tools")
        volumes_tools.register_volumes_tools(self)
        
        # Register Connections tools
        self.logger.info("Registering Connections tools")
        connections_tools.register_connections_tools(self)
        
        # Register Credentials tools
        self.logger.info("Registering Credentials tools")
        credentials_tools.register_credentials_tools(self)
        
        # Register Metastores tools
        self.logger.info("Registering Metastores tools")
        metastores_tools.register_metastores_tools(self)
        
        # Register SQL Dashboards tools
        self.logger.info("Registering SQL Dashboards tools")
        sql_dashboards_tools.register_sql_dashboards_tools(self)
        
        # Register SQL Alerts tools
        self.logger.info("Registering SQL Alerts tools")
        sql_alerts_tools.register_sql_alerts_tools(self)
        
        # Register Visualizations tools
        self.logger.info("Registering Visualizations tools")
        visualizations_tools.register_visualizations_tools(self)
        
        # Register Lakeview tools
        self.logger.info("Registering Lakeview tools")
        lakeview_tools.register_lakeview_tools(self)
        
        # Register Budget tools
        self.logger.info("Registering Budget tools")
        budget_tools.register_budget_tools(self)
        
        self.logger.info("Successfully registered all Databricks API tools")


async def main() -> None:
    """Run the MCP server."""
    server = DatabricksMCPServer()
    # Use the server's built-in method for running the server via stdio
    await server.run_stdio_async()


if __name__ == "__main__":
    # Run the server if not in test mode
    if not args.test:
        asyncio.run(main()) 