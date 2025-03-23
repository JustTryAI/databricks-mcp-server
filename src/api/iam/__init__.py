"""
Databricks IAM API Package.

This package includes modules for interacting with IAM-related APIs:
- Tokens
- Secrets
- Credentials
- Service Principals
"""

# Import from tokens module
from src.api.iam.tokens import (
    create_token,
    list_tokens,
    revoke_token,
    create_pat,
    list_pats,
    revoke_pat,
)

# Import from secrets module
from src.api.iam.secrets import (
    create_scope,
    list_scopes,
    delete_scope,
    put_secret,
    delete_secret,
    list_secrets,
    get_secret,
)

# Import from credentials module
from src.api.iam.credentials import (
    list_credentials,
    create_credentials,
    get_credentials,
    update_credentials,
    delete_credentials
)

# Import from service principals module
from src.api.iam.service_principals import (
    create_service_principal,
    list_service_principals,
    get_service_principal,
    update_service_principal,
    delete_service_principal
)

__all__ = [
    # Tokens API
    "create_token",
    "list_tokens",
    "revoke_token",
    "create_pat",
    "list_pats",
    "revoke_pat",
    
    # Secrets API
    "create_scope",
    "list_scopes",
    "delete_scope",
    "put_secret",
    "delete_secret",
    "list_secrets",
    "get_secret",
    
    # Credentials
    "list_credentials",
    "create_credentials",
    "get_credentials",
    "update_credentials",
    "delete_credentials",
    
    # Service Principals
    "create_service_principal",
    "list_service_principals",
    "get_service_principal",
    "update_service_principal",
    "delete_service_principal"
]  # Will be expanded as modules are moved 