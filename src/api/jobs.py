"""
API for managing Databricks jobs.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def create_job(job_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new Databricks job.
    
    Args:
        job_config: Job configuration
        
    Returns:
        Response containing the job ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Creating new job")
    return make_api_request("POST", "/api/2.0/jobs/create", data=job_config)


async def run_job(job_id: int, notebook_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Run a job now.
    
    Args:
        job_id: ID of the job to run
        notebook_params: Optional parameters for the notebook
        
    Returns:
        Response containing the run ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Running job: {job_id}")
    
    run_params = {"job_id": job_id}
    if notebook_params:
        run_params["notebook_params"] = notebook_params
        
    return make_api_request("POST", "/api/2.0/jobs/run-now", data=run_params)


async def list_jobs() -> Dict[str, Any]:
    """
    List all jobs.
    
    Returns:
        Response containing a list of jobs
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing all jobs")
    return make_api_request("GET", "/api/2.0/jobs/list")


async def get_job(job_id: int) -> Dict[str, Any]:
    """
    Get information about a specific job.
    
    Args:
        job_id: ID of the job
        
    Returns:
        Response containing job information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting information for job: {job_id}")
    return make_api_request("GET", "/api/2.0/jobs/get", params={"job_id": job_id})


async def update_job(job_id: int, new_settings: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update an existing job.
    
    Args:
        job_id: ID of the job to update
        new_settings: New job settings
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating job: {job_id}")
    
    update_data = {
        "job_id": job_id,
        "new_settings": new_settings
    }
    
    return make_api_request("POST", "/api/2.0/jobs/update", data=update_data)


async def delete_job(job_id: int) -> Dict[str, Any]:
    """
    Delete a job.
    
    Args:
        job_id: ID of the job to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting job: {job_id}")
    return make_api_request("POST", "/api/2.0/jobs/delete", data={"job_id": job_id})


async def get_run(run_id: int) -> Dict[str, Any]:
    """
    Get information about a specific job run.
    
    Args:
        run_id: ID of the run
        
    Returns:
        Response containing run information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting information for run: {run_id}")
    return make_api_request("GET", "/api/2.0/jobs/runs/get", params={"run_id": run_id})


async def cancel_run(run_id: int) -> Dict[str, Any]:
    """
    Cancel a job run.
    
    Args:
        run_id: ID of the run to cancel
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Cancelling run: {run_id}")
    return make_api_request("POST", "/api/2.0/jobs/runs/cancel", data={"run_id": run_id})


async def delete_run(run_id: int) -> Dict[str, Any]:
    """
    Delete a job run.
    
    Args:
        run_id: ID of the run to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting run: {run_id}")
    return make_api_request("POST", "/api/2.1/jobs/runs/delete", data={"run_id": run_id})


async def reset_job(job_id: int) -> Dict[str, Any]:
    """
    Reset a job to its original definition.
    
    Args:
        job_id: ID of the job to reset
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Resetting job: {job_id}")
    return make_api_request("POST", "/api/2.1/jobs/reset", data={"job_id": job_id})


async def get_run_output(run_id: int) -> Dict[str, Any]:
    """
    Get the output of a run.
    
    Args:
        run_id: ID of the run
        
    Returns:
        Response containing the run output
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting output for run: {run_id}")
    return make_api_request("GET", "/api/2.1/jobs/runs/get-output", params={"run_id": run_id})


async def list_runs(job_id: Optional[int] = None, active_only: bool = False, completed_only: bool = False, offset: int = 0, limit: int = 25) -> Dict[str, Any]:
    """
    List job runs.
    
    Args:
        job_id: Optional ID of the job
        active_only: If true, only active runs will be returned
        completed_only: If true, only completed runs will be returned
        offset: Offset for pagination
        limit: Maximum number of runs to return
        
    Returns:
        Response containing a list of runs
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Listing runs for job: {job_id}")
    
    params = {
        "offset": offset,
        "limit": limit
    }
    
    if job_id is not None:
        params["job_id"] = job_id
    
    if active_only:
        params["active_only"] = "true"
    
    if completed_only:
        params["completed_only"] = "true"
    
    return make_api_request("GET", "/api/2.1/jobs/runs/list", params=params) 