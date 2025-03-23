"""Jobs tools module for Databricks MCP server.

This module provides tool implementations for Databricks Jobs API endpoints.
"""
import json
from typing import Any, Dict, List, Optional, Union, cast

from mcp.server import FastMCP
from mcp.types import TextContent, CallToolResult

from src.api.workflow import jobs
from src.core.utils import DatabricksAPIError
from src.core.utils import API_ENDPOINTS
from src.core.logging import get_logger
from src.server.tools.common import generate_tool_description, handle_api_error, missing_param_error, success_response

logger = get_logger(__name__)


def register_jobs_tools(server):
    """Register all job-related tools with the server.
    
    Args:
        server: The Databricks MCP server instance
    """
    @server.tool(
        name="list_jobs",
        description=generate_tool_description(
            jobs.list_jobs,
            API_ENDPOINTS["list_jobs"]["method"],
            API_ENDPOINTS["list_jobs"]["endpoint"]
        ),
    )
    async def list_jobs(params: Dict[str, Any]) -> List[TextContent]:
        """List all Databricks jobs."""
        try:
            logger.info("Listing jobs")
            result = await jobs.list_jobs(params)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "jobs")

    @server.tool(
        name="create_job",
        description=generate_tool_description(
            jobs.create_job,
            API_ENDPOINTS["create_job"]["method"],
            API_ENDPOINTS["create_job"]["endpoint"]
        ),
    )
    async def create_job(params: Dict[str, Any]) -> List[TextContent]:
        """Create a new Databricks job."""
        try:
            logger.info(f"Creating job with parameters: {params}")
            
            # Validate required parameters
            if "name" not in params:
                return missing_param_error("name")
            if "tasks" not in params:
                return missing_param_error("tasks")
            
            # Start with required parameters
            api_params = {
                "name": params.get("name"),
                "tasks": params.get("tasks")
            }
            
            # Add optional parameters only if they exist
            optional_params = [
                "email_notifications", "timeout_seconds", "schedule", 
                "max_concurrent_runs", "job_clusters", "tags", 
                "format", "continuous", "git_source"
            ]
            
            for param in optional_params:
                if param in params and params[param] is not None:
                    api_params[param] = params[param]
            
            # Add any additional parameters not explicitly listed
            for key, value in params.items():
                if key not in ["name", "tasks"] and key not in optional_params:
                    api_params[key] = value
            
            result = await jobs.create_job(**api_params)
            logger.info(f"Job created: {result}")
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "job creation")
        except Exception as e:
            logger.error(f"Error creating job: {str(e)}")
            return handle_api_error(e)

    @server.tool(
        name="get_job",
        description=generate_tool_description(
            jobs.get_job,
            API_ENDPOINTS["get_job"]["method"],
            API_ENDPOINTS["get_job"]["endpoint"]
        ),
    )
    async def get_job(params: Dict[str, Any]) -> List[TextContent]:
        """Get a specific Databricks job."""
        job_id = params.get("job_id")
        if not job_id:
            return missing_param_error("job_id")
            
        try:
            logger.info(f"Getting job with ID: {job_id}")
            result = await jobs.get_job(job_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "job", job_id)

    @server.tool(
        name="update_job",
        description=generate_tool_description(
            jobs.update_job,
            API_ENDPOINTS["update_job"]["method"],
            API_ENDPOINTS["update_job"]["endpoint"]
        ),
    )
    async def update_job(params: Dict[str, Any]) -> List[TextContent]:
        """Update a Databricks job."""
        try:
            logger.info(f"Updating job with parameters: {params}")
            
            # Validate required parameters
            if "job_id" not in params:
                return missing_param_error("job_id")
            if "new_settings" not in params:
                return missing_param_error("new_settings")
                
            # Start with required parameters
            job_id = params.get("job_id")
            new_settings = params.get("new_settings")
            
            # Convert job_id to int if it's a string
            if isinstance(job_id, str) and job_id.isdigit():
                job_id = int(job_id)
            
            result = await jobs.update_job(job_id=job_id, new_settings=new_settings)
            logger.info(f"Job updated: {result}")
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "job", params.get("job_id"))
        except Exception as e:
            logger.error(f"Error updating job: {str(e)}")
            return handle_api_error(e)

    @server.tool(
        name="delete_job",
        description=generate_tool_description(
            jobs.delete_job,
            API_ENDPOINTS["delete_job"]["method"],
            API_ENDPOINTS["delete_job"]["endpoint"]
        ),
    )
    async def delete_job(params: Dict[str, Any]) -> List[TextContent]:
        """Delete a Databricks job."""
        job_id = params.get("job_id")
        if not job_id:
            return missing_param_error("job_id")
            
        try:
            logger.info(f"Deleting job with ID: {job_id}")
            result = await jobs.delete_job(job_id)
            logger.info(f"Job deleted: {result}")
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "job", job_id)

    @server.tool(
        name="reset_job",
        description=generate_tool_description(
            jobs.reset_job,
            API_ENDPOINTS["reset_job"]["method"],
            API_ENDPOINTS["reset_job"]["endpoint"]
        ),
    )
    async def reset_job(params: Dict[str, Any]) -> List[TextContent]:
        """Reset a Databricks job."""
        try:
            logger.info(f"Resetting job with parameters: {params}")
            
            # Validate required parameters
            if "job_id" not in params:
                return missing_param_error("job_id")
                
            job_id = params.get("job_id")
            
            # Convert job_id to int if it's a string
            if isinstance(job_id, str) and job_id.isdigit():
                job_id = int(job_id)
            
            # If new_settings not provided, get current settings
            if "new_settings" not in params:
                try:
                    logger.info(f"No new settings provided, fetching current settings for job {job_id}")
                    job_info = await jobs.get_job(job_id)
                    new_settings = job_info.get("settings", {})
                    logger.info(f"Retrieved current settings for job {job_id}")
                except DatabricksAPIError as e:
                    logger.error(f"Error retrieving current job settings: {str(e)}")
                    return handle_api_error(e, "job", str(job_id))
            else:
                new_settings = params.get("new_settings")
                logger.info(f"Using provided new settings for job {job_id}")
            
            # Call the reset_job API with the prepared parameters
            result = await jobs.reset_job(job_id=job_id, new_settings=new_settings)
            logger.info(f"Job reset: {result}")
            return success_response(result)
        except DatabricksAPIError as e:
            # For API errors, use the handle_api_error function
            return handle_api_error(e, "job", str(params.get("job_id")))
        except Exception as e:
            # For general exceptions, create a custom error response
            error_message = f"Error resetting job {params.get('job_id')}: {str(e)}"
            logger.error(error_message)
            return [TextContent(
                type="text",
                text=json.dumps({"isError": True, "error": error_message})
            )]

    @server.tool(
        name="run_job",
        description=generate_tool_description(
            jobs.run_job,
            API_ENDPOINTS["run_job"]["method"],
            API_ENDPOINTS["run_job"]["endpoint"]
        ),
    )
    async def run_job(params: Dict[str, Any]) -> List[TextContent]:
        """Run a Databricks job."""
        job_id = params.get("job_id")
        if not job_id:
            return missing_param_error("job_id")
            
        try:
            # Validate required parameters
            if "job_id" not in params:
                return missing_param_error("job_id")
            
            # Start with required parameters
            api_params = {
                "job_id": params.get("job_id")
            }
            
            # Add optional parameters only if they exist
            optional_params = [
                "idempotency_token", "jar_params", "notebook_params",
                "python_params", "spark_submit_params", "python_named_params",
                "sql_params", "dbt_commands"
            ]
            
            for param in optional_params:
                if param in params and params[param] is not None:
                    api_params[param] = params[param]
            
            result = await jobs.run_job(**api_params)
            logger.info(f"Job run initiated: {result}")
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "job", params.get("job_id"))
        except Exception as e:
            logger.error(f"Error running job: {str(e)}")
            return handle_api_error(e)

    @server.tool(
        name="list_runs",
        description=generate_tool_description(
            jobs.list_runs,
            API_ENDPOINTS["list_runs"]["method"],
            API_ENDPOINTS["list_runs"]["endpoint"]
        ),
    )
    async def list_runs(params: Dict[str, Any]) -> List[TextContent]:
        """List runs for Databricks jobs."""
        try:
            logger.info(f"Listing job runs with parameters: {params}")
            result = await jobs.list_runs(params)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "job runs")

    @server.tool(
        name="get_run",
        description=generate_tool_description(
            jobs.get_run,
            API_ENDPOINTS["get_run"]["method"],
            API_ENDPOINTS["get_run"]["endpoint"]
        ),
    )
    async def get_run(params: Dict[str, Any]) -> List[TextContent]:
        """Get a specific job run."""
        run_id = params.get("run_id")
        if not run_id:
            return missing_param_error("run_id")
            
        try:
            # Convert run_id to int if it's a string
            if isinstance(run_id, str) and run_id.isdigit():
                run_id = int(run_id)
                
            logger.info(f"Getting job run with ID: {run_id}")
            result = await jobs.get_run(run_id)
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "job run", str(run_id))

    @server.tool(
        name="cancel_run",
        description=generate_tool_description(
            jobs.cancel_run,
            API_ENDPOINTS["cancel_run"]["method"],
            API_ENDPOINTS["cancel_run"]["endpoint"]
        ),
    )
    async def cancel_run(params: Dict[str, Any]) -> List[TextContent]:
        """Cancel a job run."""
        run_id = params.get("run_id")
        if not run_id:
            return missing_param_error("run_id")
            
        try:
            # Convert run_id to int if it's a string
            if isinstance(run_id, str) and run_id.isdigit():
                run_id = int(run_id)
                
            logger.info(f"Cancelling job run with ID: {run_id}")
            result = await jobs.cancel_run(run_id)
            logger.info(f"Job run cancelled: {result}")
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "job run", str(run_id))

    @server.tool(
        name="delete_run",
        description=generate_tool_description(
            jobs.delete_run,
            API_ENDPOINTS["delete_run"]["method"],
            API_ENDPOINTS["delete_run"]["endpoint"]
        ),
    )
    async def delete_run(params: Dict[str, Any]) -> List[TextContent]:
        """Delete a job run."""
        run_id = params.get("run_id")
        if not run_id:
            return missing_param_error("run_id")
            
        try:
            # Convert run_id to int if it's a string
            if isinstance(run_id, str) and run_id.isdigit():
                run_id = int(run_id)
                
            logger.info(f"Deleting job run with ID: {run_id}")
            result = await jobs.delete_run(run_id)
            logger.info(f"Job run deleted: {result}")
            return success_response(result)
        except DatabricksAPIError as e:
            return handle_api_error(e, "job run", str(run_id))

    @server.tool(
        name="get_job_run_output",
        description=generate_tool_description(
            jobs.get_run_output,
            API_ENDPOINTS["get_run_output"]["method"],
            API_ENDPOINTS["get_run_output"]["endpoint"]
        ),
    )
    async def get_job_run_output(params: Dict[str, Any]) -> List[TextContent]:
        """Get the output for a job run."""
        try:
            logger.info(f"Getting output for job run with parameters: {params}")
            
            # Validate required parameters
            if "run_id" not in params:
                return missing_param_error("run_id")
                
            run_id = params.get("run_id")
            
            # Convert run_id to int if it's a string
            if isinstance(run_id, str) and run_id.isdigit():
                run_id = int(run_id)
            
            logger.info(f"Getting output for job run with ID: {run_id}")
            
            # First check if the run exists by trying to get its details
            try:
                # Attempt to get run details to check existence
                run_info = await jobs.get_run(run_id)
                
                # Check if run is in a state where output would be available
                run_state = run_info.get("state", {}).get("life_cycle_state", "").upper()
                if run_state in ["PENDING", "RUNNING", "WAITING"]:
                    logger.warning(f"Job run {run_id} is in state {run_state} and may not have output available yet")
                    return success_response({
                        "message": f"Job run {run_id} is in state {run_state} and may not have output available yet",
                        "run_state": run_state,
                        "run_info": run_info
                    })
            except DatabricksAPIError as e:
                # If run doesn't exist, return a specific error message
                if e.status_code == 404 or "RESOURCE_DOES_NOT_EXIST" in str(e):
                    logger.warning(f"Job run {run_id} does not exist")
                    return handle_api_error(e, "job run", str(run_id))
                # Log the error but continue to try get_run_output
                logger.warning(f"Error getting run info: {str(e)}")
            
            # Try to get the run output
            try:
                result = await jobs.get_run_output(run_id)
                return success_response(result)
            except DatabricksAPIError as e:
                # Handle specific error cases for run output
                if "not completed" in str(e).lower() or "still running" in str(e).lower():
                    logger.warning(f"Job run {run_id} is still running and output is not available")
                    return success_response({
                        "message": f"Job run {run_id} is still running and output is not available",
                    })
                elif e.status_code == 400:
                    # For Bad Request errors, provide a more helpful message
                    logger.warning(f"Bad request when getting job run output: {str(e)}")
                    return success_response({
                        "message": f"Unable to get output for job run {run_id}. The run may not be in a state where output is available, or the run ID may be invalid.",
                        "error": str(e)
                    })
                else:
                    return handle_api_error(e, "job run output", str(run_id))
                
        except DatabricksAPIError as e:
            return handle_api_error(e, "job run output", str(run_id))
        except Exception as e:
            logger.error(f"Error getting job run output: {str(e)}")
            return handle_api_error(e, "job run output", str(run_id)) 