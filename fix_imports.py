"""
Script to fix imports across the codebase.

This script replaces all instances of 'from src.api.exceptions import DatabricksAPIError'
with 'from src.core.utils import DatabricksAPIError'.
"""

import os
import re

def fix_imports(directory):
    """
    Fix imports in all Python files in the directory recursively.
    
    Args:
        directory: Root directory to start searching
    """
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                
                # Read file content
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Replace import statement
                new_content = re.sub(
                    r'from src\.api\.exceptions import DatabricksAPIError', 
                    'from src.core.utils import DatabricksAPIError',
                    content
                )
                
                # Write back if changes were made
                if new_content != content:
                    print(f"Fixing imports in: {file_path}")
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(new_content)


if __name__ == "__main__":
    # Start from the src directory
    fix_imports("src")
    print("Import fixing complete!") 