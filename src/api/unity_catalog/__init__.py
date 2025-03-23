"""
Databricks Unity Catalog API Package.

This module re-exports functionality from the catalog module to maintain
compatibility with tools that import from src.api.unity_catalog.
"""

from src.api.catalog import catalogs, schemas, tables, metastores, storage_credentials, volumes, connections

# Create a class that forwards all method calls to the appropriate modules
class UnityCatalog:
    # Catalog operations
    def list_catalogs(self, *args, **kwargs):
        return catalogs.list_catalogs(*args, **kwargs)
    
    def create_catalog(self, *args, **kwargs):
        return catalogs.create_catalog(*args, **kwargs)
    
    def get_catalog(self, *args, **kwargs):
        return catalogs.get_catalog(*args, **kwargs)
    
    def update_catalog(self, *args, **kwargs):
        return catalogs.update_catalog(*args, **kwargs)
    
    def delete_catalog(self, *args, **kwargs):
        return catalogs.delete_catalog(*args, **kwargs)
    
    # Schema operations
    def list_schemas(self, *args, **kwargs):
        return schemas.list_schemas(*args, **kwargs)
    
    def create_schema(self, *args, **kwargs):
        return schemas.create_schema(*args, **kwargs)
    
    def get_schema(self, *args, **kwargs):
        return schemas.get_schema(*args, **kwargs)
    
    def update_schema(self, *args, **kwargs):
        return schemas.update_schema(*args, **kwargs)
    
    def delete_schema(self, *args, **kwargs):
        return schemas.delete_schema(*args, **kwargs)
    
    # Table operations
    def list_tables(self, *args, **kwargs):
        return tables.list_tables(*args, **kwargs)
    
    def get_table(self, *args, **kwargs):
        return tables.get_table(*args, **kwargs)
    
    # Add additional methods as needed for other catalog operations

# Create a singleton instance
unity_catalog = UnityCatalog()

# Export the unity_catalog instance
__all__ = ["unity_catalog"] 