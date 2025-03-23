import re
import os

def extract_api_endpoints():
    # Extract API_ENDPOINTS keys from src/core/utils.py
    with open('src/core/utils.py', 'r') as f:
        utils_content = f.read()
    
    # Find all keys in API_ENDPOINTS dictionary
    endpoint_pattern = r'"([a-zA-Z_]+)":\s*{"method"'
    endpoints = re.findall(endpoint_pattern, utils_content)
    return set(endpoints)

def extract_tool_implementations():
    # Extract tool implementations from databricks_mcp_server.py
    with open('src/server/databricks_mcp_server.py', 'r') as f:
        server_content = f.read()
    
    # Find all tool names
    tool_pattern = r'name="([a-zA-Z_]+)"'
    tools = re.findall(tool_pattern, server_content)
    return set(tools)

def check_endpoints_with_special_handling():
    # Some endpoints might have different names in the server file
    # This function checks API_ENDPOINTS references directly
    with open('src/server/databricks_mcp_server.py', 'r') as f:
        server_content = f.read()
    
    endpoints = extract_api_endpoints()
    used_endpoints = set()
    
    for endpoint in endpoints:
        if re.search(f'API_ENDPOINTS\\[\"{endpoint}\"\\]', server_content):
            used_endpoints.add(endpoint)
    
    return used_endpoints

if __name__ == '__main__':
    api_endpoints = extract_api_endpoints()
    tool_implementations = extract_tool_implementations()
    special_handling = check_endpoints_with_special_handling()
    
    # Combine all used endpoints
    used_endpoints = tool_implementations.union(special_handling)
    missing_endpoints = api_endpoints - used_endpoints
    
    print(f"Found {len(api_endpoints)} API endpoints in API_ENDPOINTS dictionary")
    print(f"Found {len(tool_implementations)} tool implementations in server file")
    print(f"Found {len(special_handling)} endpoints referenced directly")
    
    if missing_endpoints:
        print("\nMissing implementations for the following endpoints:")
        for endpoint in sorted(missing_endpoints):
            print(f"- {endpoint}")
    else:
        print("\nAll endpoints have implementations!")
    
    # Check for special name mappings (endpoints with different names in the server)
    special_name_mappings = {}
    for endpoint in api_endpoints:
        for tool in tool_implementations:
            if endpoint not in used_endpoints and endpoint in tool:
                special_name_mappings[endpoint] = tool
    
    if special_name_mappings:
        print("\nPossible special name mappings:")
        for endpoint, tool in special_name_mappings.items():
            print(f"- {endpoint} -> {tool}") 