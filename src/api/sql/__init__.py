"""
Databricks SQL API Package.

This package provides modules for interacting with Databricks SQL-related APIs
such as SQL Warehouses, Queries, Dashboards, Alerts, and Visualizations.
"""

from src.api.sql.warehouses import (
    list_warehouses,
    create_warehouse,
    get_warehouse,
    delete_warehouse,
    update_warehouse,
    start_warehouse,
    stop_warehouse
)

from src.api.sql.queries import (
    create_query,
    list_queries,
    get_query,
    update_query,
    delete_query,
    run_query
)

from src.api.sql.visualizations import (
    create_visualization,
    update_visualization,
    delete_visualization
)

from src.api.sql.dashboards import (
    create_dashboard,
    list_dashboards,
    get_dashboard,
    update_dashboard,
    delete_dashboard
)

from src.api.sql.alerts import (
    create_alert,
    list_alerts,
    get_alert,
    update_alert,
    delete_alert
)

# Aliases for backward compatibility
import src.api.sql.queries as sql_queries
import src.api.sql.dashboards as sql_dashboards
import src.api.sql.alerts as sql_alerts

__all__ = [
    # Warehouses
    "list_warehouses",
    "create_warehouse",
    "get_warehouse",
    "delete_warehouse",
    "update_warehouse", 
    "start_warehouse",
    "stop_warehouse",
    
    # Queries
    "create_query",
    "list_queries",
    "get_query",
    "update_query",
    "delete_query",
    "run_query",
    
    # Visualizations
    "create_visualization",
    "update_visualization",
    "delete_visualization",
    
    # Dashboards
    "create_dashboard",
    "list_dashboards",
    "get_dashboard",
    "update_dashboard",
    "delete_dashboard",
    
    # Alerts
    "create_alert",
    "list_alerts",
    "get_alert",
    "update_alert",
    "delete_alert",
    
    # Alias exports for backward compatibility
    "sql_queries",
    "sql_dashboards",
    "sql_alerts"
] 