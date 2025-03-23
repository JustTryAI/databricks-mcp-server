"""
Utility functions for the Databricks MCP server.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Union, Callable
import inspect

import requests
from requests.exceptions import RequestException

from src.core.config import get_api_headers, get_databricks_api_url

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class DatabricksAPIError(Exception):
    """Exception raised for errors in the Databricks API."""

    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[Any] = None):
        self.message = message
        self.status_code = status_code
        self.response = response
        super().__init__(self.message)


def make_api_request(
    method: str,
    endpoint: str,
    data: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    files: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Make a request to the Databricks API.
    
    Args:
        method: HTTP method ("GET", "POST", "PUT", "DELETE")
        endpoint: API endpoint path
        data: Request body data
        params: Query parameters
        files: Files to upload
        
    Returns:
        Response data as a dictionary
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    url = get_databricks_api_url(endpoint)
    headers = get_api_headers()
    
    try:
        # Log the request (omit sensitive information)
        safe_data = "**REDACTED**" if data else None
        logger.debug(f"API Request: {method} {url} Params: {params} Data: {safe_data}")
        
        # Convert data to JSON string if provided
        json_data = json.dumps(data) if data and not files else data
        
        # Make the request
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            data=json_data if not files else data,
            files=files,
        )
        
        # Check for HTTP errors
        response.raise_for_status()
        
        # Parse response
        if response.content:
            return response.json()
        return {}
        
    except RequestException as e:
        # Handle request exceptions
        status_code = getattr(e.response, "status_code", None) if hasattr(e, "response") else None
        error_msg = f"API request failed: {str(e)}"
        
        # Try to extract error details from response
        error_response = None
        if hasattr(e, "response") and e.response is not None:
            try:
                error_response = e.response.json()
                error_msg = f"{error_msg} - {error_response.get('error', '')}"
            except ValueError:
                error_response = e.response.text
        
        # Log the error
        logger.error(f"API Error: {error_msg}", exc_info=True)
        
        # Raise custom exception
        raise DatabricksAPIError(error_msg, status_code, error_response) from e


def format_response(
    success: bool, 
    data: Optional[Union[Dict[str, Any], List[Any]]] = None, 
    error: Optional[str] = None,
    status_code: int = 200
) -> Dict[str, Any]:
    """
    Format a standardized response.
    
    Args:
        success: Whether the operation was successful
        data: Response data
        error: Error message if not successful
        status_code: HTTP status code
        
    Returns:
        Formatted response dictionary
    """
    response = {
        "success": success,
        "status_code": status_code,
    }
    
    if data is not None:
        response["data"] = data
        
    if error:
        response["error"] = error
        
    return response 

def generate_tool_description(api_function: Callable, http_method: str, api_endpoint: str) -> str:
    """Generate a comprehensive description for an MCP tool from API function docstring.
    
    Args:
        api_function: The API function to generate a description for
        http_method: The HTTP method (e.g., "POST", "GET")
        api_endpoint: The API endpoint (e.g., "/api/2.0/workspace/import")
        
    Returns:
        A formatted description string with API details and parameter information
    """
    # Get the function's signature
    sig = inspect.signature(api_function)
    
    # Parse the docstring
    docstring = api_function.__doc__ or ""
    
    # Extract docstring parts
    lines = docstring.strip().split("\n")
    short_description = lines[0].strip() if lines else ""
    
    # Start with API endpoint information
    description_parts = [f"{http_method} {api_endpoint}", short_description]
    
    # Extract parameter information from docstring
    param_lines = []
    in_params_section = False
    param_info = {}
    
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("Args:"):
            in_params_section = True
            continue
        elif in_params_section and (stripped.startswith("Returns:") or not stripped):
            in_params_section = False
        elif in_params_section:
            # Try to extract parameter name and description
            parts = stripped.split(":", 1)
            if len(parts) == 2:
                param_name = parts[0].strip()
                param_desc = parts[1].strip()
                param_info[param_name] = param_desc
    
    # Format parameters with required/optional info
    params = []
    for name, param in sig.parameters.items():
        if name == "self":
            continue
            
        is_required = param.default == inspect.Parameter.empty
        required_text = "(required)" if is_required else "(optional)"
        
        # Get description from parsed docstring
        desc = param_info.get(name, "")
        
        # Add parameter info to the list
        params.append(f"{name} {required_text}: {desc}")
    
    # Add parameters section
    if params:
        description_parts.append("Parameters:")
        for param in params:
            description_parts.append(f"- {param}")
    
    # Join all parts with newlines
    return "\n".join(description_parts)

# Dictionary mapping API functions to their endpoints
API_ENDPOINTS = {
    # Workspace endpoints
    "import_files": {"method": "POST", "endpoint": "/api/2.0/workspace/import"},
    "export_files": {"method": "GET", "endpoint": "/api/2.0/workspace/export"},
    "list_files": {"method": "GET", "endpoint": "/api/2.0/workspace/list"},
    "delete_files": {"method": "POST", "endpoint": "/api/2.0/workspace/delete"},
    "get_file_status": {"method": "GET", "endpoint": "/api/2.0/workspace/get-status"},
    "mkdirs": {"method": "POST", "endpoint": "/api/2.0/workspace/mkdirs"},
    
    # Clusters endpoints
    "list_clusters": {"method": "GET", "endpoint": "/api/2.0/clusters/list"},
    "create_cluster": {"method": "POST", "endpoint": "/api/2.0/clusters/create"},
    "terminate_cluster": {"method": "POST", "endpoint": "/api/2.0/clusters/delete"},
    "get_cluster": {"method": "GET", "endpoint": "/api/2.0/clusters/get"},
    "start_cluster": {"method": "POST", "endpoint": "/api/2.0/clusters/start"},
    "restart_cluster": {"method": "POST", "endpoint": "/api/2.0/clusters/restart"},
    "resize_cluster": {"method": "POST", "endpoint": "/api/2.0/clusters/resize"},
    "permanent-delete": {"method": "POST", "endpoint": "/api/2.1/clusters/permanent-delete"},
    
    # Jobs endpoints
    "create_job": {"method": "POST", "endpoint": "/api/2.0/jobs/create"},
    "list_jobs": {"method": "GET", "endpoint": "/api/2.1/jobs/list"},
    "run_job": {"method": "POST", "endpoint": "/api/2.1/jobs/run-now"},
    "update_job": {"method": "POST", "endpoint": "/api/2.1/jobs/update"},
    "reset_job": {"method": "POST", "endpoint": "/api/2.1/jobs/reset"},
    "get_job": {"method": "GET", "endpoint": "/api/2.0/jobs/get"},
    "get_run": {"method": "GET", "endpoint": "/api/2.0/jobs/runs/get"},
    "get_run_output": {"method": "GET", "endpoint": "/api/2.1/jobs/runs/get-output"},
    "list_runs": {"method": "GET", "endpoint": "/api/2.1/jobs/runs/list"},
    "cancel_run": {"method": "POST", "endpoint": "/api/2.1/jobs/runs/cancel"},
    "delete_run": {"method": "POST", "endpoint": "/api/2.1/jobs/runs/delete"},
    "delete_job": {"method": "POST", "endpoint": "/api/2.1/jobs/delete"},
    
    # Notebook endpoints
    "list_notebooks": {"method": "GET", "endpoint": "/api/2.0/workspace/list"},
    "export_notebook": {"method": "GET", "endpoint": "/api/2.0/workspace/export"},
    
    # DBFS endpoints
    "list_dbfs_files": {"method": "GET", "endpoint": "/api/2.0/dbfs/list"},
    "read_dbfs_file": {"method": "GET", "endpoint": "/api/2.0/dbfs/read"},
    "create_dbfs_directory": {"method": "POST", "endpoint": "/api/2.0/dbfs/mkdirs"},
    "delete_dbfs_file": {"method": "POST", "endpoint": "/api/2.0/dbfs/delete"},
    "import_file": {"method": "POST", "endpoint": "/api/2.0/dbfs/put"},
    "move_file": {"method": "POST", "endpoint": "/api/2.0/dbfs/move"},
    
    # SQL endpoints
    "execute_sql": {"method": "POST", "endpoint": "/api/2.0/sql/statements/execute"},
    
    # SQL Warehouses endpoints
    "list_warehouses": {"method": "GET", "endpoint": "/api/2.0/sql/warehouses"},
    "get_warehouse": {"method": "GET", "endpoint": "/api/2.0/sql/warehouses/{id}"},
    "create_warehouse": {"method": "POST", "endpoint": "/api/2.0/sql/warehouses"},
    "update_warehouse": {"method": "POST", "endpoint": "/api/2.0/sql/warehouses/{id}/edit"},
    "delete_warehouse": {"method": "DELETE", "endpoint": "/api/2.0/sql/warehouses/{id}"},
    "start_warehouse": {"method": "POST", "endpoint": "/api/2.0/sql/warehouses/{id}/start"},
    "stop_warehouse": {"method": "POST", "endpoint": "/api/2.0/sql/warehouses/{id}/stop"},
    
    # SQL Queries endpoints
    "create_query": {"method": "POST", "endpoint": "/api/2.0/preview/sql/queries"},
    "list_queries": {"method": "GET", "endpoint": "/api/2.0/preview/sql/queries"},
    "get_query": {"method": "GET", "endpoint": "/api/2.0/preview/sql/queries/{query_id}"},
    "update_query": {"method": "POST", "endpoint": "/api/2.0/preview/sql/queries/{query_id}"},
    "delete_query": {"method": "DELETE", "endpoint": "/api/2.0/preview/sql/queries/{query_id}"},
    "run_query": {"method": "POST", "endpoint": "/api/2.0/preview/sql/queries/{query_id}/run"},
    
    # Unity Catalog endpoints
    "list_catalogs": {"method": "GET", "endpoint": "/api/2.1/unity-catalog/catalogs"},
    "create_catalog": {"method": "POST", "endpoint": "/api/2.1/unity-catalog/catalogs"},
    "get_catalog": {"method": "GET", "endpoint": "/api/2.1/unity-catalog/catalogs/{name}"},
    "update_catalog": {"method": "PATCH", "endpoint": "/api/2.1/unity-catalog/catalogs/{name}"},
    "delete_catalog": {"method": "DELETE", "endpoint": "/api/2.1/unity-catalog/catalogs/{name}"},
    
    "list_schemas": {"method": "GET", "endpoint": "/api/2.1/unity-catalog/schemas"},
    "create_schema": {"method": "POST", "endpoint": "/api/2.1/unity-catalog/schemas"},
    "get_schema": {"method": "GET", "endpoint": "/api/2.1/unity-catalog/schemas/{full_name}"},
    "update_schema": {"method": "PATCH", "endpoint": "/api/2.1/unity-catalog/schemas/{full_name}"},
    "delete_schema": {"method": "DELETE", "endpoint": "/api/2.1/unity-catalog/schemas/{full_name}"},
    
    "list_tables": {"method": "GET", "endpoint": "/api/2.1/unity-catalog/tables"},
    "get_table": {"method": "GET", "endpoint": "/api/2.1/unity-catalog/tables/{full_name}"},
    
    # SQL Dashboard endpoints
    "create_dashboard": {"method": "POST", "endpoint": "/api/2.0/preview/sql/dashboards"},
    "list_dashboards": {"method": "GET", "endpoint": "/api/2.0/preview/sql/dashboards"},
    "get_dashboard": {"method": "GET", "endpoint": "/api/2.0/preview/sql/dashboards/{dashboard_id}"},
    "update_dashboard": {"method": "POST", "endpoint": "/api/2.0/preview/sql/dashboards/{dashboard_id}"},
    "delete_dashboard": {"method": "DELETE", "endpoint": "/api/2.0/preview/sql/dashboards/{dashboard_id}"},
    
    # SQL Alert endpoints
    "create_alert": {"method": "POST", "endpoint": "/api/2.0/preview/sql/alerts"},
    "list_alerts": {"method": "GET", "endpoint": "/api/2.0/preview/sql/alerts"},
    "get_alert": {"method": "GET", "endpoint": "/api/2.0/preview/sql/alerts/{alert_id}"},
    "update_alert": {"method": "POST", "endpoint": "/api/2.0/preview/sql/alerts/{alert_id}"},
    "delete_alert": {"method": "DELETE", "endpoint": "/api/2.0/preview/sql/alerts/{alert_id}"},
    
    # Visualization endpoints
    "create_visualization": {"method": "POST", "endpoint": "/api/2.0/preview/sql/visualizations"},
    "update_visualization": {"method": "POST", "endpoint": "/api/2.0/preview/sql/visualizations/{visualization_id}"},
    "delete_visualization": {"method": "DELETE", "endpoint": "/api/2.0/preview/sql/visualizations/{visualization_id}"},
    
    # Command execution endpoints
    "create_command_context": {"method": "POST", "endpoint": "/api/1.2/contexts/create"},
    "execute_command": {"method": "POST", "endpoint": "/api/1.2/commands/execute"},
    "get_command_status": {"method": "GET", "endpoint": "/api/1.2/commands/status"},
    "cancel_command": {"method": "POST", "endpoint": "/api/1.2/commands/cancel"},
    
    # Library management endpoints
    "install_libraries": {"method": "POST", "endpoint": "/api/2.0/libraries/install"},
    "uninstall_libraries": {"method": "POST", "endpoint": "/api/2.0/libraries/uninstall"},
    "list_libraries": {"method": "GET", "endpoint": "/api/2.0/libraries/all-cluster-statuses"},
    "list_library_statuses": {"method": "GET", "endpoint": "/api/2.0/libraries/cluster-status"},
    
    # Storage credential endpoints
    "create_storage_credential": {"method": "POST", "endpoint": "/api/2.1/unity-catalog/storage-credentials"},
    "get_storage_credential": {"method": "GET", "endpoint": "/api/2.1/unity-catalog/storage-credentials/{name}"},
    "update_storage_credential": {"method": "PATCH", "endpoint": "/api/2.1/unity-catalog/storage-credentials/{name}"},
    "delete_storage_credential": {"method": "DELETE", "endpoint": "/api/2.1/unity-catalog/storage-credentials/{name}"},
    "list_storage_credentials": {"method": "GET", "endpoint": "/api/2.1/unity-catalog/storage-credentials"},
    
    # Volume endpoints
    "create_volume": {"method": "POST", "endpoint": "/api/2.1/unity-catalog/volumes"},
    "update_volume": {"method": "PATCH", "endpoint": "/api/2.1/unity-catalog/volumes/{full_name}"},
    "delete_volume": {"method": "DELETE", "endpoint": "/api/2.1/unity-catalog/volumes/{full_name}"},
    "list_volumes": {"method": "GET", "endpoint": "/api/2.1/unity-catalog/volumes"},
    
    # Connection endpoints
    "create_connection": {"method": "POST", "endpoint": "/api/2.1/unity-catalog/connections"},
    "list_connections": {"method": "GET", "endpoint": "/api/2.1/unity-catalog/connections"},
    "update_connection": {"method": "PATCH", "endpoint": "/api/2.1/unity-catalog/connections/{name}"},
    "delete_connection": {"method": "DELETE", "endpoint": "/api/2.1/unity-catalog/connections/{name}"},
    
    # Credential endpoints
    "create_credential": {"method": "POST", "endpoint": "/api/2.1/unity-catalog/credentials"},
    "list_credentials": {"method": "GET", "endpoint": "/api/2.1/unity-catalog/credentials"},
    "update_credential": {"method": "PATCH", "endpoint": "/api/2.1/unity-catalog/credentials/{name}"},
    "delete_credential": {"method": "DELETE", "endpoint": "/api/2.1/unity-catalog/credentials/{name}"},
    
    # Pipeline endpoints
    "create_pipeline": {"method": "POST", "endpoint": "/api/2.0/pipelines"},
    "list_pipelines": {"method": "GET", "endpoint": "/api/2.0/pipelines"},
    "get_pipeline": {"method": "GET", "endpoint": "/api/2.0/pipelines/{pipeline_id}"},
    "update_pipeline": {"method": "PATCH", "endpoint": "/api/2.0/pipelines/{pipeline_id}"},
    "delete_pipeline": {"method": "DELETE", "endpoint": "/api/2.0/pipelines/{pipeline_id}"},
    "start_pipeline_update": {"method": "POST", "endpoint": "/api/2.0/pipelines/{pipeline_id}/updates"},
    "get_pipeline_update": {"method": "GET", "endpoint": "/api/2.0/pipelines/{pipeline_id}/updates/{update_id}"},
    "list_pipeline_updates": {"method": "GET", "endpoint": "/api/2.0/pipelines/{pipeline_id}/updates"},
    
    # Service Principal endpoints
    "create_service_principal": {"method": "POST", "endpoint": "/api/2.0/preview/scim/v2/ServicePrincipals"},
    "list_service_principals": {"method": "GET", "endpoint": "/api/2.0/preview/scim/v2/ServicePrincipals"},
    "get_service_principal": {"method": "GET", "endpoint": "/api/2.0/preview/scim/v2/ServicePrincipals/{id}"},
    "update_service_principal": {"method": "PATCH", "endpoint": "/api/2.0/preview/scim/v2/ServicePrincipals/{id}"},
    "delete_service_principal": {"method": "DELETE", "endpoint": "/api/2.0/preview/scim/v2/ServicePrincipals/{id}"},
    
    # Lakeview endpoints
    "list_lakeviews": {"method": "GET", "endpoint": "/api/2.0/lakeview/lakeviews"},
    "create_lakeview": {"method": "POST", "endpoint": "/api/2.0/lakeview/lakeviews"},
    "get_lakeview": {"method": "GET", "endpoint": "/api/2.0/lakeview/lakeviews/{lakeview_id}"},
    "update_lakeview": {"method": "PATCH", "endpoint": "/api/2.0/lakeview/lakeviews/{lakeview_id}"},
    "delete_lakeview": {"method": "DELETE", "endpoint": "/api/2.0/lakeview/lakeviews/{lakeview_id}"},
    
    # Budget endpoints
    "create_budget": {"method": "POST", "endpoint": "/api/2.0/budgets"},
    "list_budgets": {"method": "GET", "endpoint": "/api/2.0/budgets"},
    "get_budget": {"method": "GET", "endpoint": "/api/2.0/budgets/{budget_id}"},
    "update_budget": {"method": "PATCH", "endpoint": "/api/2.0/budgets/{budget_id}"},
    "delete_budget": {"method": "DELETE", "endpoint": "/api/2.0/budgets/{budget_id}"},
    
    # External Location endpoints
    "create_external_location": {"method": "POST", "endpoint": "/api/2.1/unity-catalog/external-locations"},
    "list_external_locations": {"method": "GET", "endpoint": "/api/2.1/unity-catalog/external-locations"},
    "get_external_location": {"method": "GET", "endpoint": "/api/2.1/unity-catalog/external-locations/{name}"},
    "update_external_location": {"method": "PATCH", "endpoint": "/api/2.1/unity-catalog/external-locations/{name}"},
    "delete_external_location": {"method": "DELETE", "endpoint": "/api/2.1/unity-catalog/external-locations/{name}"}
} 