"""
API for managing Databricks jobs.

This module provides functions for interacting with the Databricks Jobs API.
It is part of the workflow API group that includes jobs and pipelines.
"""

import logging
from typing import Any, Dict, List, Optional, Union

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)

async def create_job(
    name: str,
    tasks: List[Dict[str, Any]],
    email_notifications: Optional[Dict[str, Any]] = None,
    timeout_seconds: Optional[int] = None,
    schedule: Optional[Dict[str, Any]] = None,
    max_concurrent_runs: Optional[int] = None,
    job_clusters: Optional[List[Dict[str, Any]]] = None,
    tags: Optional[Dict[str, str]] = None,
    format: Optional[str] = None,
    continuous: Optional[Dict[str, Any]] = None,
    git_source: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Create a new Databricks job.
    
    Args:
        name: The name of the job
        tasks: List of task specifications
        email_notifications: Optional email notification settings
        timeout_seconds: Optional timeout in seconds
        schedule: Optional schedule information
        max_concurrent_runs: Optional maximum number of concurrent job runs
        job_clusters: Optional job-level cluster specifications
        tags: Optional tags for the job
        format: Optional format for job definition
        continuous: Optional continuous execution configuration
        git_source: Optional git source information
        **kwargs: Additional parameters to pass to the API
        
    Returns:
        Response containing the job ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating job: {name}")
    
    data = {
        "name": name,
        "tasks": tasks,
    }
    
    # Add optional parameters
    optional_params = {
        "email_notifications": email_notifications,
        "timeout_seconds": timeout_seconds,
        "schedule": schedule,
        "max_concurrent_runs": max_concurrent_runs,
        "job_clusters": job_clusters,
        "tags": tags,
        "format": format,
        "continuous": continuous,
        "git_source": git_source,
    }
    
    # Add optional parameters if they exist
    for key, value in optional_params.items():
        if value is not None:
            data[key] = value
    
    # Add any additional parameters
    for key, value in kwargs.items():
        data[key] = value
    
    return make_api_request("POST", "/api/2.1/jobs/create", data=data)


async def list_jobs(
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    expand_tasks: Optional[bool] = None,
    name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    List all jobs in the workspace.
    
    Args:
        limit: Maximum number of jobs to return
        offset: Offset to start listing jobs from
        expand_tasks: Whether to include task/parameter details
        name: Filter jobs by name
        
    Returns:
        Response containing the list of jobs
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing jobs")
    
    params = {}
    if limit is not None:
        params["limit"] = limit
    if offset is not None:
        params["offset"] = offset
    if expand_tasks is not None:
        params["expand_tasks"] = expand_tasks
    if name is not None:
        params["name"] = name
    
    return make_api_request("GET", "/api/2.1/jobs/list", params=params)


async def get_job(job_id: int) -> Dict[str, Any]:
    """
    Get information about a specific job.
    
    Args:
        job_id: The ID of the job
        
    Returns:
        Response containing the job information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting job info for {job_id}")
    return make_api_request("GET", "/api/2.1/jobs/get", params={"job_id": job_id})


async def update_job(
    job_id: int,
    new_settings: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Update a job's settings.
    
    Args:
        job_id: The ID of the job to update
        new_settings: The new settings for the job
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating job {job_id}")
    data = {
        "job_id": job_id,
        "new_settings": new_settings,
    }
    return make_api_request("POST", "/api/2.1/jobs/update", data=data)


async def delete_job(job_id: int) -> Dict[str, Any]:
    """
    Delete a job.
    
    Args:
        job_id: The ID of the job to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting job {job_id}")
    return make_api_request("POST", "/api/2.1/jobs/delete", data={"job_id": job_id})


async def run_job(
    job_id: int,
    idempotency_token: Optional[str] = None,
    jar_params: Optional[List[str]] = None,
    notebook_params: Optional[Dict[str, str]] = None,
    python_params: Optional[List[str]] = None,
    spark_submit_params: Optional[List[str]] = None,
    python_named_params: Optional[Dict[str, str]] = None,
    sql_params: Optional[Dict[str, str]] = None,
    dbt_commands: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Run a job now.
    
    Args:
        job_id: The ID of the job to run
        idempotency_token: Optional token for idempotent operations
        jar_params: Optional parameters for JAR tasks
        notebook_params: Optional parameters for notebook tasks
        python_params: Optional parameters for Python tasks
        spark_submit_params: Optional parameters for spark submit tasks
        python_named_params: Optional named parameters for Python tasks
        sql_params: Optional parameters for SQL tasks
        dbt_commands: Optional commands for dbt tasks
        
    Returns:
        Response containing the run ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Running job {job_id}")
    
    data = {"job_id": job_id}
    
    # Add optional parameters
    optional_params = {
        "idempotency_token": idempotency_token,
        "jar_params": jar_params,
        "notebook_params": notebook_params,
        "python_params": python_params,
        "spark_submit_params": spark_submit_params,
        "python_named_params": python_named_params,
        "sql_params": sql_params,
        "dbt_commands": dbt_commands,
    }
    
    for key, value in optional_params.items():
        if value is not None:
            data[key] = value
    
    return make_api_request("POST", "/api/2.1/jobs/run-now", data=data)


async def run_job_submit(
    tasks: List[Dict[str, Any]],
    run_name: Optional[str] = None,
    timeout_seconds: Optional[int] = None,
    idempotency_token: Optional[str] = None,
    access_control_list: Optional[List[Dict[str, Any]]] = None,
    git_source: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Submit a one-time run.
    
    Args:
        tasks: List of task specifications
        run_name: Optional name for the run
        timeout_seconds: Optional timeout in seconds
        idempotency_token: Optional token for idempotent operations
        access_control_list: Optional access control list
        git_source: Optional git source information
        **kwargs: Additional parameters to pass to the API
        
    Returns:
        Response containing the run ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Submitting job run")
    
    data = {"tasks": tasks}
    
    # Add optional parameters
    optional_params = {
        "run_name": run_name,
        "timeout_seconds": timeout_seconds,
        "idempotency_token": idempotency_token,
        "access_control_list": access_control_list,
        "git_source": git_source,
    }
    
    for key, value in optional_params.items():
        if value is not None:
            data[key] = value
    
    # Add any additional parameters
    for key, value in kwargs.items():
        data[key] = value
    
    return make_api_request("POST", "/api/2.1/jobs/runs/submit", data=data)


async def get_run(run_id: int) -> Dict[str, Any]:
    """
    Get the metadata of a specific run.
    
    Args:
        run_id: The ID of the run
        
    Returns:
        Response containing the run information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting run info for {run_id}")
    return make_api_request("GET", "/api/2.1/jobs/runs/get", params={"run_id": run_id})


async def cancel_run(run_id: int) -> Dict[str, Any]:
    """
    Cancel a run.
    
    Args:
        run_id: The ID of the run to cancel
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Canceling run {run_id}")
    return make_api_request("POST", "/api/2.1/jobs/runs/cancel", data={"run_id": run_id})


async def reset_job(job_id: int, new_settings: Dict[str, Any]) -> Dict[str, Any]:
    """
    Reset a job by recreating all of its settings.
    
    Args:
        job_id: The ID of the job to reset
        new_settings: The new settings for the job
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Resetting job {job_id}")
    
    data = {
        "job_id": job_id,
        "new_settings": new_settings
    }
    
    return make_api_request("POST", "/api/2.1/jobs/reset", data=data)


async def delete_run(run_id: int) -> Dict[str, Any]:
    """
    Delete a job run.
    
    Args:
        run_id: The ID of the run to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting run {run_id}")
    return make_api_request("POST", "/api/2.1/jobs/runs/delete", data={"run_id": run_id})


async def get_run_output(run_id: int) -> Dict[str, Any]:
    """
    Get the output of a run.
    
    Args:
        run_id: The ID of the run
        
    Returns:
        Response containing the run output
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting output for run {run_id}")
    return make_api_request("GET", "/api/2.1/jobs/runs/get-output", params={"run_id": run_id})


async def list_runs(
    job_id: Optional[int] = None,
    active_only: Optional[bool] = None,
    completed_only: Optional[bool] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
    run_type: Optional[str] = None,
    expand_tasks: Optional[bool] = None,
    start_time_from: Optional[int] = None,
    start_time_to: Optional[int] = None,
) -> Dict[str, Any]:
    """
    List job runs.
    
    Args:
        job_id: Optional ID of the job
        active_only: Optional filter for active runs only
        completed_only: Optional filter for completed runs only
        offset: Optional offset for pagination
        limit: Optional limit for pagination
        run_type: Optional run type filter
        expand_tasks: Optional boolean to expand task information
        start_time_from: Optional start time filter (lower bound)
        start_time_to: Optional start time filter (upper bound)
        
    Returns:
        Response containing the list of runs
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing job runs")
    
    params = {}
    optional_params = {
        "job_id": job_id,
        "active_only": active_only,
        "completed_only": completed_only,
        "offset": offset,
        "limit": limit,
        "run_type": run_type,
        "expand_tasks": expand_tasks,
        "start_time_from": start_time_from,
        "start_time_to": start_time_to,
    }
    
    for key, value in optional_params.items():
        if value is not None:
            params[key] = value
    
    return make_api_request("GET", "/api/2.1/jobs/runs/list", params=params) 