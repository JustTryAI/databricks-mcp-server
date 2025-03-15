"""
Test isolated API calls to diagnose issues with the Databricks Jobs API.
"""

import asyncio
import json
import logging
import sys

from src.api import jobs
from src.core.utils import DatabricksAPIError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

async def test_list_jobs_direct():
    """Test the list_jobs function directly to see the raw API response."""
    try:
        logger.info("Testing list_jobs API directly")
        result = await jobs.list_jobs()
        logger.info(f"Raw API response: {json.dumps(result, indent=2)}")
        
        # Check if 'jobs' field exists
        if 'jobs' in result:
            logger.info(f"Found 'jobs' field with {len(result['jobs'])} entries")
        else:
            logger.error("'jobs' field not found in response!")
            logger.info(f"Response keys: {list(result.keys())}")
            
        return result
    except DatabricksAPIError as e:
        logger.error(f"API Error: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return None

async def test_jobs_server_call():
    """Test how the jobs list is processed when called through the server."""
    from src.server.databricks_mcp_server import DatabricksMCPServer
    
    try:
        logger.info("Testing jobs list through server call")
        server = DatabricksMCPServer()
        
        # Call the tool with params
        result = await server.call_tool("list_jobs", {"params": {}})
        
        # Inspect the result
        logger.info(f"Server result type: {type(result)}")
        logger.info(f"Server result: {result}")
        
        # Try to decode any JSON
        if len(result) > 0 and "text" in result[0]:
            text = result[0]["text"]
            logger.info(f"Result text: {text[:100]}...")  # Show first 100 chars
            
            try:
                parsed = json.loads(text)
                logger.info(f"Parsed JSON: {json.dumps(parsed, indent=2)[:200]}...")  # Show first 200 chars
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing JSON: {e}")
        
        return result
    except Exception as e:
        logger.error(f"Error in server call: {e}", exc_info=True)
        return None

async def main():
    """Run all isolated tests."""
    logger.info("Running isolated tests to diagnose API issues")
    
    # Test list_jobs directly
    direct_result = await test_list_jobs_direct()
    
    # Test through server
    server_result = await test_jobs_server_call()
    
    # Compare results
    if direct_result and server_result:
        logger.info("Tests completed - check logs for details")
    else:
        logger.error("Tests failed - check logs for errors")

if __name__ == "__main__":
    asyncio.run(main()) 