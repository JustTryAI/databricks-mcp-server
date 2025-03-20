"""
API for managing Databricks Unity Catalog resources.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

# Catalogs
async def create_catalog(name: str, comment: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a new catalog in the Unity Catalog.
    
    Args:
        name: Name of the catalog
        comment: Optional comment for the catalog
        
    Returns:
        Response containing the created catalog info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new catalog: {name}")
    
    data = {"name": name}
    if comment:
        data["comment"] = comment
    
    return make_api_request("POST", "/api/2.1/unity-catalog/catalogs", data=data)


async def list_catalogs(max_results: Optional[int] = None) -> Dict[str, Any]:
    """
    List catalogs in the Unity Catalog.
    
    Args:
        max_results: Optional maximum number of catalogs to return
        
    Returns:
        Response containing a list of catalogs
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing catalogs")
    
    params = {}
    if max_results:
        params["max_results"] = max_results
    
    return make_api_request("GET", "/api/2.1/unity-catalog/catalogs", params=params)


async def get_catalog(name: str) -> Dict[str, Any]:
    """
    Get details of a catalog in the Unity Catalog.
    
    Args:
        name: Name of the catalog
        
    Returns:
        Response containing catalog details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting catalog details: {name}")
    return make_api_request("GET", f"/api/2.1/unity-catalog/catalogs/{name}")


async def update_catalog(name: str, new_name: Optional[str] = None, comment: Optional[str] = None) -> Dict[str, Any]:
    """
    Update a catalog in the Unity Catalog.
    
    Args:
        name: Name of the catalog to update
        new_name: Optional new name for the catalog
        comment: Optional new comment for the catalog
        
    Returns:
        Response containing the updated catalog info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating catalog: {name}")
    
    data = {}
    if new_name:
        data["name"] = new_name
    if comment:
        data["comment"] = comment
    
    return make_api_request("PATCH", f"/api/2.1/unity-catalog/catalogs/{name}", data=data)


async def delete_catalog(name: str) -> Dict[str, Any]:
    """
    Delete a catalog from the Unity Catalog.
    
    Args:
        name: Name of the catalog to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting catalog: {name}")
    return make_api_request("DELETE", f"/api/2.1/unity-catalog/catalogs/{name}")


# Schemas
async def create_schema(name: str, catalog_name: str, comment: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a new schema in the Unity Catalog.
    
    Args:
        name: Name of the schema
        catalog_name: Name of the catalog to create the schema in
        comment: Optional comment for the schema
        
    Returns:
        Response containing the created schema info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new schema: {catalog_name}.{name}")
    
    data = {
        "name": name,
        "catalog_name": catalog_name
    }
    if comment:
        data["comment"] = comment
    
    return make_api_request("POST", "/api/2.1/unity-catalog/schemas", data=data)


async def list_schemas(catalog_name: Optional[str] = None, max_results: Optional[int] = None) -> Dict[str, Any]:
    """
    List schemas in the Unity Catalog.
    
    Args:
        catalog_name: Optional catalog name to filter schemas by
        max_results: Optional maximum number of schemas to return
        
    Returns:
        Response containing a list of schemas
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Listing schemas{f' in catalog {catalog_name}' if catalog_name else ''}")
    
    params = {}
    if catalog_name:
        params["catalog_name"] = catalog_name
    if max_results:
        params["max_results"] = max_results
    
    return make_api_request("GET", "/api/2.1/unity-catalog/schemas", params=params)


async def get_schema(full_name: str) -> Dict[str, Any]:
    """
    Get details of a schema in the Unity Catalog.
    
    Args:
        full_name: Full name of the schema (catalog.schema)
        
    Returns:
        Response containing schema details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting schema details: {full_name}")
    return make_api_request("GET", f"/api/2.1/unity-catalog/schemas/{full_name}")


async def update_schema(full_name: str, new_name: Optional[str] = None, comment: Optional[str] = None) -> Dict[str, Any]:
    """
    Update a schema in the Unity Catalog.
    
    Args:
        full_name: Full name of the schema to update (catalog.schema)
        new_name: Optional new name for the schema
        comment: Optional new comment for the schema
        
    Returns:
        Response containing the updated schema info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating schema: {full_name}")
    
    data = {}
    if new_name:
        data["name"] = new_name
    if comment:
        data["comment"] = comment
    
    return make_api_request("PATCH", f"/api/2.1/unity-catalog/schemas/{full_name}", data=data)


async def delete_schema(full_name: str) -> Dict[str, Any]:
    """
    Delete a schema from the Unity Catalog.
    
    Args:
        full_name: Full name of the schema to delete (catalog.schema)
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting schema: {full_name}")
    return make_api_request("DELETE", f"/api/2.1/unity-catalog/schemas/{full_name}")


# Tables
async def create_table(
    name: str,
    schema_name: str,
    catalog_name: str,
    columns: List[Dict[str, Any]],
    comment: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Create a new table in the Unity Catalog.
    
    Args:
        name: Name of the table
        schema_name: Name of the schema to create the table in
        catalog_name: Name of the catalog containing the schema
        columns: List of column definitions
        comment: Optional comment for the table
        properties: Optional table properties
        
    Returns:
        Response containing the created table info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new table: {catalog_name}.{schema_name}.{name}")
    
    data = {
        "name": name,
        "schema_name": schema_name,
        "catalog_name": catalog_name,
        "columns": columns
    }
    if comment:
        data["comment"] = comment
    if properties:
        data["properties"] = properties
    
    return make_api_request("POST", "/api/2.1/unity-catalog/tables", data=data)


async def list_tables(
    catalog_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    max_results: Optional[int] = None,
    page_token: Optional[str] = None
) -> Dict[str, Any]:
    """
    List tables in the Unity Catalog.
    
    Args:
        catalog_name: Optional catalog name to filter tables by
        schema_name: Optional schema name to filter tables by (requires catalog_name)
        max_results: Optional maximum number of tables to return
        page_token: Optional pagination token for large result sets
        
    Returns:
        Response containing a list of tables
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    if schema_name and not catalog_name:
        raise ValueError("catalog_name is required when schema_name is provided")
    
    logger.info(f"Listing tables{f' in {catalog_name}.{schema_name}' if schema_name else f' in catalog {catalog_name}' if catalog_name else ''}")
    
    params = {}
    if catalog_name:
        params["catalog_name"] = catalog_name
    if schema_name:
        params["schema_name"] = schema_name
    if max_results:
        params["max_results"] = max_results
    if page_token:
        params["page_token"] = page_token
    
    return make_api_request("GET", "/api/2.1/unity-catalog/tables", params=params)


async def get_table(full_name: str) -> Dict[str, Any]:
    """
    Get details of a table in the Unity Catalog.
    
    Args:
        full_name: Full name of the table (catalog.schema.table)
        
    Returns:
        Response containing table details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting table details: {full_name}")
    return make_api_request("GET", f"/api/2.1/unity-catalog/tables/{full_name}")


async def update_table(
    full_name: str,
    new_name: Optional[str] = None,
    comment: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Update a table in the Unity Catalog.
    
    Args:
        full_name: Full name of the table to update (catalog.schema.table)
        new_name: Optional new name for the table
        comment: Optional new comment for the table
        properties: Optional new properties for the table
        
    Returns:
        Response containing the updated table info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating table: {full_name}")
    
    data = {}
    if new_name:
        data["name"] = new_name
    if comment:
        data["comment"] = comment
    if properties:
        data["properties"] = properties
    
    return make_api_request("PATCH", f"/api/2.1/unity-catalog/tables/{full_name}", data=data)


async def delete_table(full_name: str) -> Dict[str, Any]:
    """
    Delete a table from the Unity Catalog.
    
    Args:
        full_name: Full name of the table to delete (catalog.schema.table)
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting table: {full_name}")
    return make_api_request("DELETE", f"/api/2.1/unity-catalog/tables/{full_name}")


# Storage Credentials
async def create_storage_credential(
    name: str,
    aws_credentials: Optional[Dict[str, Any]] = None,
    azure_service_principal: Optional[Dict[str, Any]] = None,
    azure_managed_identity: Optional[Dict[str, Any]] = None,
    gcp_credentials: Optional[Dict[str, Any]] = None,
    comment: Optional[str] = None,
    read_only: Optional[bool] = None,
    skip_validation: Optional[bool] = None
) -> Dict[str, Any]:
    """
    Create a new storage credential in the Unity Catalog.
    
    Args:
        name: Name of the storage credential (required)
        aws_credentials: Optional AWS credentials configuration
            - Must include "access_key" and "secret_key" keys
        azure_service_principal: Optional Azure service principal credentials
            - Must include "directory_id", "application_id", and "client_secret" keys
        azure_managed_identity: Optional Azure managed identity credentials
            - Must include "access_connector_id" key in format:
              /subscriptions/{guid}/resourceGroups/{rg-name}/providers/Microsoft.Databricks/accessConnectors/{connector-name}
            - May optionally include "managed_identity_id" for user-assigned identities
        gcp_credentials: Optional GCP credentials configuration
            - Must include "email" and "privateKey" keys
        comment: Optional comment for the storage credential
        read_only: Optional flag to specify if credential is read-only
        skip_validation: Optional flag to skip validation of the credential
        
    Returns:
        Response containing the created storage credential info
        
    Raises:
        ValueError: If required parameters are missing or invalid
        DatabricksAPIError: If the API request fails
    """
    if not name:
        raise ValueError("Storage credential name is required")
    
    logger.info(f"Creating new storage credential: {name}")
    
    # Validate that at least one credential type is provided
    credential_types = [aws_credentials, azure_service_principal, azure_managed_identity, gcp_credentials]
    if not any(credential_types):
        raise ValueError(
            "At least one credential type must be provided: "
            "aws_credentials, azure_service_principal, azure_managed_identity, or gcp_credentials"
        )
    
    # Check for multiple credential types being provided
    provided_credentials = [cred_type for cred_type in credential_types if cred_type is not None]
    if len(provided_credentials) > 1:
        raise ValueError("Only one credential type can be specified at a time")
    
    data = {"name": name}
    
    # Add and validate credentials
    if aws_credentials:
        if not isinstance(aws_credentials, dict):
            raise ValueError("aws_credentials must be a dictionary")
        data["aws_credentials"] = aws_credentials
    
    if azure_service_principal:
        if not isinstance(azure_service_principal, dict):
            raise ValueError("azure_service_principal must be a dictionary")
        
        # Check required fields for Azure service principal
        required_fields = ["directory_id", "application_id"]
        for field in required_fields:
            if field not in azure_service_principal:
                raise ValueError(f"Required field '{field}' missing in azure_service_principal")
        
        data["azure_service_principal"] = azure_service_principal
    
    if azure_managed_identity:
        if not isinstance(azure_managed_identity, dict):
            raise ValueError("azure_managed_identity must be a dictionary")
        
        # Check required field for Azure managed identity
        if "access_connector_id" not in azure_managed_identity:
            raise ValueError("Required field 'access_connector_id' missing in azure_managed_identity")
        
        # Validate format of access_connector_id
        access_connector_id = azure_managed_identity.get("access_connector_id")
        if not access_connector_id.startswith("/subscriptions/"):
            logger.warning(
                f"access_connector_id format may be invalid: {access_connector_id}. "
                f"Expected format: /subscriptions/{{guid}}/resourceGroups/{{rg-name}}/"
                f"providers/Microsoft.Databricks/accessConnectors/{{connector-name}}"
            )
        
        data["azure_managed_identity"] = azure_managed_identity
    
    if gcp_credentials:
        if not isinstance(gcp_credentials, dict):
            raise ValueError("gcp_credentials must be a dictionary")
        data["gcp_service_account"] = gcp_credentials
    
    # Add optional parameters
    if comment:
        data["comment"] = comment
    if read_only is not None:
        data["read_only"] = read_only
    if skip_validation is not None:
        data["skip_validation"] = skip_validation
    
    # Make the API request
    try:
        return make_api_request("POST", "/api/2.1/unity-catalog/storage-credentials", data=data)
    except DatabricksAPIError as e:
        logger.error(f"API error creating storage credential: {str(e)}")
        # Enhance error message with potential solutions
        if "400" in str(e):
            if azure_service_principal and "client_secret" not in azure_service_principal:
                logger.error("Azure service principal is missing client_secret which is required by the API")
            if azure_managed_identity and "access_connector_id" in azure_managed_identity:
                logger.error("Check that the access_connector_id format is correct and the connector exists")
        raise


async def get_storage_credential(name: str) -> Dict[str, Any]:
    """
    Get details of a storage credential in the Unity Catalog.
    
    Args:
        name: Name of the storage credential
        
    Returns:
        Response containing storage credential details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting storage credential details: {name}")
    return make_api_request("GET", f"/api/2.1/unity-catalog/storage-credentials/{name}")


async def update_storage_credential(
    name: str,
    new_name: Optional[str] = None,
    aws_credentials: Optional[Dict[str, Any]] = None,
    azure_credentials: Optional[Dict[str, Any]] = None,
    gcp_credentials: Optional[Dict[str, Any]] = None,
    comment: Optional[str] = None
) -> Dict[str, Any]:
    """
    Update a storage credential in the Unity Catalog.
    
    Args:
        name: Name of the storage credential to update
        new_name: Optional new name for the storage credential
        aws_credentials: Optional new AWS credentials configuration
        azure_credentials: Optional new Azure credentials configuration
        gcp_credentials: Optional new GCP credentials configuration
        comment: Optional new comment for the storage credential
        
    Returns:
        Response containing the updated storage credential info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating storage credential: {name}")
    
    data = {}
    
    if new_name:
        data["name"] = new_name
        
    if aws_credentials:
        data["aws_credentials"] = aws_credentials
    elif azure_credentials:
        data["azure_managed_identity"] = azure_credentials
    elif gcp_credentials:
        data["gcp_service_account"] = gcp_credentials
    
    if comment:
        data["comment"] = comment
    
    return make_api_request("PATCH", f"/api/2.1/unity-catalog/storage-credentials/{name}", data=data)


async def delete_storage_credential(name: str) -> Dict[str, Any]:
    """
    Delete a storage credential from the Unity Catalog.
    
    Args:
        name: Name of the storage credential to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting storage credential: {name}")
    return make_api_request("DELETE", f"/api/2.1/unity-catalog/storage-credentials/{name}")


async def list_storage_credentials(max_results: Optional[int] = None) -> Dict[str, Any]:
    """
    List storage credentials in the Unity Catalog.
    
    Args:
        max_results: Optional maximum number of storage credentials to return
        
    Returns:
        Response containing a list of storage credentials
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing storage credentials")
    
    params = {}
    if max_results:
        params["max_results"] = max_results
    
    return make_api_request("GET", "/api/2.1/unity-catalog/storage-credentials", params=params)


# Volumes
async def create_volume(
    name: str,
    schema_name: str,
    catalog_name: str,
    volume_type: str,
    storage_location: Optional[str] = None,
    comment: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create a new volume in the Unity Catalog.
    
    Args:
        name: Name of the volume
        schema_name: Name of the schema to create the volume in
        catalog_name: Name of the catalog containing the schema
        volume_type: Type of the volume (MANAGED or EXTERNAL)
        storage_location: Optional storage location for external volumes
        comment: Optional comment for the volume
        
    Returns:
        Response containing the created volume info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new volume: {catalog_name}.{schema_name}.{name}")
    
    data = {
        "name": name,
        "schema_name": schema_name,
        "catalog_name": catalog_name,
        "volume_type": volume_type
    }
    
    if storage_location:
        data["storage_location"] = storage_location
    
    if comment:
        data["comment"] = comment
    
    return make_api_request("POST", "/api/2.1/unity-catalog/volumes", data=data)


async def get_volume(full_name: str) -> Dict[str, Any]:
    """
    Get details of a volume in the Unity Catalog.
    
    Args:
        full_name: Full name of the volume (catalog.schema.volume)
        
    Returns:
        Response containing volume details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting volume details: {full_name}")
    return make_api_request("GET", f"/api/2.1/unity-catalog/volumes/{full_name}")


async def update_volume(
    full_name: str,
    new_name: Optional[str] = None,
    comment: Optional[str] = None
) -> Dict[str, Any]:
    """
    Update a volume in the Unity Catalog.
    
    Args:
        full_name: Full name of the volume to update (catalog.schema.volume)
        new_name: Optional new name for the volume
        comment: Optional new comment for the volume
        
    Returns:
        Response containing the updated volume info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating volume: {full_name}")
    
    data = {}
    if new_name:
        data["name"] = new_name
    if comment:
        data["comment"] = comment
    
    return make_api_request("PATCH", f"/api/2.1/unity-catalog/volumes/{full_name}", data=data)


async def delete_volume(full_name: str) -> Dict[str, Any]:
    """
    Delete a volume from the Unity Catalog.
    
    Args:
        full_name: Full name of the volume to delete (catalog.schema.volume)
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting volume: {full_name}")
    return make_api_request("DELETE", f"/api/2.1/unity-catalog/volumes/{full_name}")


async def list_volumes(
    catalog_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    max_results: Optional[int] = None,
    page_token: Optional[str] = None
) -> Dict[str, Any]:
    """
    List volumes in the Unity Catalog.
    
    Args:
        catalog_name: Optional catalog name to filter volumes by
        schema_name: Optional schema name to filter volumes by (requires catalog_name)
        max_results: Optional maximum number of volumes to return
        page_token: Optional pagination token for large result sets
        
    Returns:
        Response containing a list of volumes
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    if schema_name and not catalog_name:
        raise ValueError("catalog_name is required when schema_name is provided")
    
    logger.info(f"Listing volumes{f' in {catalog_name}.{schema_name}' if schema_name else f' in catalog {catalog_name}' if catalog_name else ''}")
    
    params = {}
    if catalog_name:
        params["catalog_name"] = catalog_name
    if schema_name:
        params["schema_name"] = schema_name
    if max_results:
        params["max_results"] = max_results
    if page_token:
        params["page_token"] = page_token
    
    return make_api_request("GET", "/api/2.1/unity-catalog/volumes", params=params)


# Connections
async def create_connection(
    name: str,
    connection_type: str,
    options: Dict[str, str],
    comment: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create a new connection in the Unity Catalog.
    
    Args:
        name: Name of the connection
        connection_type: Type of the connection
        options: Connection options
        comment: Optional comment for the connection
        
    Returns:
        Response containing the created connection info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new connection: {name}")
    
    data = {
        "name": name,
        "connection_type": connection_type,
        "options": options
    }
    
    if comment:
        data["comment"] = comment
    
    return make_api_request("POST", "/api/2.1/unity-catalog/connections", data=data)


async def list_connections() -> Dict[str, Any]:
    """
    List all connections in the Unity Catalog.
    
    Returns:
        Response containing a list of connections
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing connections")
    return make_api_request("GET", "/api/2.1/unity-catalog/connections")


async def update_connection(
    name: str,
    new_name: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    comment: Optional[str] = None
) -> Dict[str, Any]:
    """
    Update a connection in the Unity Catalog.
    
    Args:
        name: Name of the connection to update
        new_name: Optional new name for the connection
        options: Optional new connection options
        comment: Optional new comment for the connection
        
    Returns:
        Response containing the updated connection info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating connection: {name}")
    
    data = {}
    if new_name:
        data["name"] = new_name
    if options:
        data["options"] = options
    if comment:
        data["comment"] = comment
    
    return make_api_request("PATCH", f"/api/2.1/unity-catalog/connections/{name}", data=data)


async def delete_connection(name: str) -> Dict[str, Any]:
    """
    Delete a connection from the Unity Catalog.
    
    Args:
        name: Name of the connection to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting connection: {name}")
    return make_api_request("DELETE", f"/api/2.1/unity-catalog/connections/{name}")


# Credentials
async def create_credential(
    name: str,
    aws_credentials: Optional[Dict[str, Any]] = None,
    azure_credentials: Optional[Dict[str, Any]] = None,
    comment: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create a new credential in the Unity Catalog.
    
    Args:
        name: Name of the credential
        aws_credentials: Optional AWS credentials configuration
        azure_credentials: Optional Azure credentials configuration
        comment: Optional comment for the credential
        
    Returns:
        Response containing the created credential info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating new credential: {name}")
    
    data = {"name": name}
    
    if aws_credentials:
        data["aws_credentials"] = aws_credentials
    elif azure_credentials:
        data["azure_service_principal"] = azure_credentials
    
    if comment:
        data["comment"] = comment
    
    return make_api_request("POST", "/api/2.1/unity-catalog/credentials", data=data)


async def list_credentials() -> Dict[str, Any]:
    """
    List all credentials in the Unity Catalog.
    
    Returns:
        Response containing a list of credentials
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing credentials")
    return make_api_request("GET", "/api/2.1/unity-catalog/credentials")


async def update_credential(
    name: str,
    new_name: Optional[str] = None,
    aws_credentials: Optional[Dict[str, Any]] = None,
    azure_credentials: Optional[Dict[str, Any]] = None,
    comment: Optional[str] = None
) -> Dict[str, Any]:
    """
    Update a credential in the Unity Catalog.
    
    Args:
        name: Name of the credential to update
        new_name: Optional new name for the credential
        aws_credentials: Optional new AWS credentials configuration
        azure_credentials: Optional new Azure credentials configuration
        comment: Optional new comment for the credential
        
    Returns:
        Response containing the updated credential info
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating credential: {name}")
    
    data = {}
    
    if new_name:
        data["name"] = new_name
        
    if aws_credentials:
        data["aws_credentials"] = aws_credentials
    elif azure_credentials:
        data["azure_service_principal"] = azure_credentials
    
    if comment:
        data["comment"] = comment
    
    return make_api_request("PATCH", f"/api/2.1/unity-catalog/credentials/{name}", data=data)


async def delete_credential(name: str) -> Dict[str, Any]:
    """
    Delete a credential from the Unity Catalog.
    
    Args:
        name: Name of the credential to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting credential: {name}")
    return make_api_request("DELETE", f"/api/2.1/unity-catalog/credentials/{name}") 