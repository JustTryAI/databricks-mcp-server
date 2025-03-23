"""
Databricks Workspace API Package.

This package provides modules for interacting with Databricks Workspace-related APIs
such as Notebooks, Workspace management, and Git repositories.
"""

# Import from workspace module
from src.api.workspace.workspace import (
    list_workspace,
    import_workspace,
    export_workspace,
    delete_workspace,
    get_workspace_status,
    create_workspace_directory,
    is_base64,
    
    # Aliases for backward compatibility
    list_files,
    import_files,
    export_files,
    delete_files,
    mkdirs,
)

# Import from notebooks module
from src.api.workspace.notebooks import (
    import_notebook,
    export_notebook,
    list_notebooks,
    delete_notebook,
    create_directory,
    get_status,
    mkdirs as notebooks_mkdirs,
)

# Import from repos module
from src.api.workspace.repos import (
    create_repo,
    get_repo,
    list_repos,
    update_repo,
    delete_repo,
    get_repo_permissions,
    update_repo_permissions,
)

__all__ = [
    # Workspace API
    "list_workspace",
    "import_workspace",
    "export_workspace",
    "delete_workspace",
    "get_workspace_status",
    "create_workspace_directory",
    "is_base64",
    "list_files",
    "import_files",
    "export_files",
    "delete_files",
    "mkdirs",
    
    # Notebooks API
    "import_notebook",
    "export_notebook",
    "list_notebooks",
    "delete_notebook",
    "create_directory",
    "get_status",
    
    # Repos API
    "create_repo",
    "get_repo",
    "list_repos",
    "update_repo",
    "delete_repo",
    "get_repo_permissions",
    "update_repo_permissions",
] 