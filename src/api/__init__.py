"""
Databricks API Package.

This package provides modules for interacting with various Databricks APIs.
It organizes the APIs into logical categories such as compute, storage, workspace, etc.
"""

# Import modules that are expected by databricks_mcp_server.py
from src.api.workspace import workspace, notebooks
from src.api.compute import clusters, libraries
from src.api.workflow import jobs, pipelines
from src.api.iam import service_principals, tokens, secrets, credentials
from src.api.sql import warehouses, sql
from src.api.sql import dashboards as sql_dashboards
from src.api.sql import queries as sql_queries
from src.api.sql import alerts as sql_alerts
from src.api.sql import visualizations
from src.api.storage import dbfs, external_locations
from src.api.catalog import catalogs, schemas, tables, metastores, storage_credentials, volumes, connections
from src.api.unity_catalog import unity_catalog
from src.api.misc import budgets, commands, lakeview

# Export all modules
__all__ = [
    "budgets",
    "clusters",
    "commands",
    "connections",
    "credentials",
    "dbfs",
    "external_locations",
    "jobs",
    "lakeview",
    "libraries",
    "notebooks",
    "pipelines",
    "secrets",
    "service_principals",
    "sql",
    "sql_alerts",
    "sql_dashboards",
    "sql_queries",
    "storage_credentials",
    "tokens",
    "unity_catalog",
    "visualizations",
    "volumes",
    "warehouses",
    "workspace",
]
