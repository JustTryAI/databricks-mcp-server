from src.api import workspace
from src.core.utils import generate_tool_description, API_ENDPOINTS

# Get the description for import_files
description = generate_tool_description(
    workspace.import_files,
    API_ENDPOINTS["import_files"]["method"],
    API_ENDPOINTS["import_files"]["endpoint"]
)

print("Description for import_files:")
print(description)

# Get the description for list_files
description = generate_tool_description(
    workspace.list_files,
    API_ENDPOINTS["list_files"]["method"],
    API_ENDPOINTS["list_files"]["endpoint"]
)

print("\nDescription for list_files:")
print(description)

# Get the description for list_clusters
from src.api import clusters
description = generate_tool_description(
    clusters.list_clusters,
    API_ENDPOINTS["list_clusters"]["method"],
    API_ENDPOINTS["list_clusters"]["endpoint"]
)

print("\nDescription for list_clusters:")
print(description) 