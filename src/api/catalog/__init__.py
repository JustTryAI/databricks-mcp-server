"""
Databricks Catalog API Package.

This package provides modules for interacting with Databricks Catalog APIs
including Unity Catalog functionality such as Catalogs, Schemas, Tables, and
External Locations.
"""

from src.api.catalog.catalogs import (
    list_catalogs,
    create_catalog,
    get_catalog,
    update_catalog,
    delete_catalog
)

from src.api.catalog.schemas import (
    list_schemas,
    create_schema, 
    get_schema,
    update_schema,
    delete_schema
)

from src.api.catalog.tables import (
    list_tables,
    get_table
)

from src.api.catalog.storage_credentials import (
    list_storage_credentials,
    create_storage_credential,
    get_storage_credential,
    update_storage_credential,
    delete_storage_credential
)

from src.api.catalog.metastores import (
    list_metastores,
    create_metastore,
    get_metastore,
    update_metastore,
    delete_metastore
)

from src.api.catalog.volumes import (
    list_volumes,
    create_volume,
    get_volume,
    update_volume,
    delete_volume
)

from src.api.catalog.connections import (
    list_connections,
    create_connection,
    get_connection,
    update_connection,
    delete_connection
)

# Create alias for backward compatibility
from src.api.unity_catalog import unity_catalog

__all__ = [
    # Catalogs
    "list_catalogs",
    "create_catalog",
    "get_catalog",
    "update_catalog",
    "delete_catalog",
    
    # Schemas
    "list_schemas",
    "create_schema",
    "get_schema",
    "update_schema",
    "delete_schema",
    
    # Tables
    "list_tables",
    "get_table",
    
    # Storage Credentials
    "list_storage_credentials",
    "create_storage_credential",
    "get_storage_credential",
    "update_storage_credential", 
    "delete_storage_credential",
    
    # Metastores
    "list_metastores",
    "create_metastore",
    "get_metastore",
    "update_metastore",
    "delete_metastore",
    
    # Volumes
    "list_volumes",
    "create_volume",
    "get_volume",
    "update_volume",
    "delete_volume",
    
    # Connections
    "list_connections",
    "create_connection",
    "get_connection",
    "update_connection",
    "delete_connection",
    
    # Backward compatibility
    "unity_catalog"
] 