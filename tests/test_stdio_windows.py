"""
Test for Windows-specific stdio handling in the Databricks MCP server.

This test verifies that the MCP server can correctly handle stdio
communication on Windows systems.
"""

import asyncio
import json
import logging
import sys
import os
import signal
from typing import Dict, Any, List, Optional
import asyncio.subprocess

import pytest

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filename="test_stdio_windows.log"
)
logger = logging.getLogger(__name__)

# Add console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

async def test_stdio_windows():
    """Test that the server can start and handle stdio on Windows correctly."""
    logger.info("Starting Windows stdio test")
    
    # Get the project root directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    # Path to the start script
    script_path = os.path.join(project_root, "start_mcp_server.ps1")
    
    try:
        # Start the server process
        process = await asyncio.create_subprocess_exec(
            "powershell",
            "-File",
            script_path,
            "-SkipPrompt",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        if not process.stdin or not process.stdout:
            raise RuntimeError("Failed to create process pipes")
            
        # Give the server a moment to start
        await asyncio.sleep(2)
        
        # Send initialization message
        init_message = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "test-client",
                    "version": "1.0.0"
                }
            }
        }
        
        logger.info(f"Sending initialization message: {json.dumps(init_message)}")
        
        # Convert to JSON string with newline
        init_message_str = json.dumps(init_message) + "\n"
        
        # Send the message
        try:
            process.stdin.write(init_message_str.encode())
            await process.stdin.drain()
            
            # Set up concurrent tasks for reading stdout and stderr
            async def read_output():
                assert process.stdout is not None
                while True:
                    line = await process.stdout.readline()
                    if not line:
                        break
                    line_str = line.decode().strip()
                    logger.info(f"Received line: {line_str}")
                    try:
                        response = json.loads(line_str)
                        if "result" in response:
                            logger.info("Successfully received initialization response")
                            return True
                    except json.JSONDecodeError:
                        pass
                return False
                
            async def read_stderr():
                assert process.stderr is not None
                while True:
                    line = await process.stderr.readline()
                    if not line:
                        break
                    line_str = line.decode().strip()
                    if line_str:
                        logger.error(f"Server stderr: {line_str}")
            
            # Run the readers with a timeout
            try:
                stdout_task = asyncio.create_task(read_output())
                stderr_task = asyncio.create_task(read_stderr())
                
                # Wait for response with timeout
                done, pending = await asyncio.wait(
                    [stdout_task, stderr_task],
                    timeout=10,
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # Cancel pending tasks
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                
                if stdout_task in done and await stdout_task:
                    logger.info("Test passed: Server responded to initialization")
                else:
                    logger.error("Test failed: No valid response received from server")
                    
            except asyncio.TimeoutError:
                logger.error("Test timed out waiting for response")
                raise
                
        except Exception as e:
            logger.error(f"Error during communication: {e}")
            raise
            
    except Exception as e:
        logger.error(f"Error during test: {e}")
        raise
        
    finally:
        # Terminate the server process
        logger.info("Terminating server process")
        try:
            if process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
        except Exception as e:
            logger.error(f"Error terminating process: {e}")
        
        logger.info("Test completed")

async def main():
    """Run the Windows stdio test."""
    try:
        await test_stdio_windows()
    except Exception as e:
        logger.error(f"Test failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Only run on Windows
    if sys.platform.startswith('win'):
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            logger.info("Test interrupted by user")
            sys.exit(1)
    else:
        logger.info("Skipping test: Not running on Windows") 