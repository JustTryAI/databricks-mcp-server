"""
API for managing Databricks File System (DBFS).
"""

import base64
import logging
import os
from typing import Any, Dict, List, Optional, BinaryIO

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def put_file(
    dbfs_path: str,
    file_content: bytes,
    overwrite: bool = True,
) -> Dict[str, Any]:
    """
    Upload a file to DBFS.
    
    Args:
        dbfs_path: The path where the file should be stored in DBFS
        file_content: The content of the file as bytes
        overwrite: Whether to overwrite an existing file
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Uploading file to DBFS path: {dbfs_path}")
    
    # Convert bytes to base64
    content_base64 = base64.b64encode(file_content).decode("utf-8")
    
    return make_api_request(
        "POST",
        "/api/2.0/dbfs/put",
        data={
            "path": dbfs_path,
            "contents": content_base64,
            "overwrite": overwrite,
        },
    )


async def upload_large_file(
    dbfs_path: str,
    local_file_path: str,
    overwrite: bool = True,
    buffer_size: int = 1024 * 1024,  # 1MB chunks
) -> Dict[str, Any]:
    """
    Upload a large file to DBFS in chunks.
    
    Args:
        dbfs_path: The path where the file should be stored in DBFS
        local_file_path: Local path to the file to upload
        overwrite: Whether to overwrite an existing file
        buffer_size: Size of chunks to upload
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
        FileNotFoundError: If the local file does not exist
    """
    logger.info(f"Uploading large file from {local_file_path} to DBFS path: {dbfs_path}")
    
    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"Local file not found: {local_file_path}")
    
    # Create a handle for the upload
    create_response = make_api_request(
        "POST",
        "/api/2.0/dbfs/create",
        data={
            "path": dbfs_path,
            "overwrite": overwrite,
        },
    )
    
    handle = create_response.get("handle")
    
    try:
        with open(local_file_path, "rb") as f:
            chunk_index = 0
            while True:
                chunk = f.read(buffer_size)
                if not chunk:
                    break
                    
                # Convert chunk to base64
                chunk_base64 = base64.b64encode(chunk).decode("utf-8")
                
                # Add to handle
                make_api_request(
                    "POST",
                    "/api/2.0/dbfs/add-block",
                    data={
                        "handle": handle,
                        "data": chunk_base64,
                    },
                )
                
                chunk_index += 1
                logger.debug(f"Uploaded chunk {chunk_index}")
        
        # Close the handle
        return make_api_request(
            "POST",
            "/api/2.0/dbfs/close",
            data={"handle": handle},
        )
        
    except Exception as e:
        # Attempt to abort the upload on error
        try:
            make_api_request(
                "POST",
                "/api/2.0/dbfs/close",
                data={"handle": handle},
            )
        except Exception:
            pass
        
        logger.error(f"Error uploading file: {str(e)}")
        raise


async def get_file(
    dbfs_path: str,
    offset: int = 0,
    length: int = 1024 * 1024,  # Default to 1MB
) -> Dict[str, Any]:
    """
    Get the contents of a file from DBFS.
    
    Args:
        dbfs_path: The path of the file in DBFS
        offset: Starting byte position
        length: Number of bytes to read
        
    Returns:
        Response containing the file content
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Reading file from DBFS path: {dbfs_path}")
    
    response = make_api_request(
        "GET",
        "/api/2.0/dbfs/read",
        params={
            "path": dbfs_path,
            "offset": offset,
            "length": length,
        },
    )
    
    # Decode base64 content
    if "data" in response:
        try:
            response["decoded_data"] = base64.b64decode(response["data"])
        except Exception as e:
            logger.warning(f"Failed to decode file content: {str(e)}")
            
    return response


# Alias get_file as read_file for backward compatibility
read_file = get_file


async def list_files(dbfs_path: str) -> Dict[str, Any]:
    """
    List files and directories in a DBFS path.
    
    Args:
        dbfs_path: The path to list
        
    Returns:
        Response containing the directory listing
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Listing files in DBFS path: {dbfs_path}")
    return make_api_request("GET", "/api/2.0/dbfs/list", params={"path": dbfs_path})


async def delete_file(
    dbfs_path: str,
    recursive: bool = False,
) -> Dict[str, Any]:
    """
    Delete a file or directory from DBFS.
    
    Args:
        dbfs_path: The path to delete
        recursive: Whether to recursively delete directories
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting DBFS path: {dbfs_path}")
    return make_api_request(
        "POST",
        "/api/2.0/dbfs/delete",
        data={
            "path": dbfs_path,
            "recursive": recursive,
        },
    )


async def get_status(dbfs_path: str) -> Dict[str, Any]:
    """
    Get the status of a file or directory.
    
    Args:
        dbfs_path: The path to check
        
    Returns:
        Response containing file status
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting status of DBFS path: {dbfs_path}")
    return make_api_request("GET", "/api/2.0/dbfs/get-status", params={"path": dbfs_path})


async def create_directory(dbfs_path: str) -> Dict[str, Any]:
    """
    Create a directory in DBFS.
    
    Args:
        dbfs_path: The path to create
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating DBFS directory: {dbfs_path}")
    return make_api_request("POST", "/api/2.0/dbfs/mkdirs", data={"path": dbfs_path})


async def import_file(
    source_path: str,
    target_path: str,
    overwrite: bool = False,
) -> Dict[str, Any]:
    """
    Import a file to DBFS from local path.
    
    Args:
        source_path: Local path to the file to import
        target_path: The path where the file should be stored in DBFS
        overwrite: Whether to overwrite an existing file
        
    Returns:
        Response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
        FileNotFoundError: If the source file does not exist
    """
    logger.info(f"Importing file from {source_path} to DBFS path: {target_path}")
    
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Local file not found: {source_path}")
    
    # Read the file content
    with open(source_path, "rb") as f:
        file_content = f.read()
    
    # Upload the file
    return await put_file(target_path, file_content, overwrite)


async def move_file(
    source_path: str,
    target_path: str,
) -> Dict[str, Any]:
    """
    Move a file in DBFS.
    
    Args:
        source_path: The current path of the file in DBFS
        target_path: The new path for the file in DBFS
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Moving file from {source_path} to {target_path}")
    return make_api_request(
        "POST",
        "/api/2.0/dbfs/move",
        data={
            "source_path": source_path,
            "destination_path": target_path,
        },
    ) 