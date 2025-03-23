"""
API for managing Databricks workflow runs.

This module provides functions for interacting with the Databricks Workflow Runs API.
It is part of the workflow API group that includes jobs and run now requests.
"""

import logging
from typing import Any, Dict, List, Optional, Union

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def list_runs(
    job_id: Optional[int] = None,
    start_time_from: Optional[int] = None,
    start_time_to: Optional[int] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    expand_tasks: Optional[bool] = None,
    run_type: Optional[str] = None,
) -> Dict[str, Any]:
    """
    List job runs in descending order by start time.
    
    Args:
        job_id: Optional ID of the job to list runs for
        start_time_from: Optional start time in milliseconds from epoch
        start_time_to: Optional end time in milliseconds from epoch
        limit: Optional maximum number of runs to return
        offset: Optional offset for paging
        expand_tasks: Optional flag to include task and cluster details
        run_type: Optional filter by run type ("JOB_RUN" or "SUBMIT_RUN" or "WORKFLOW_RUN")
        
    Returns:
        Response containing the list of runs
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing workflow runs")
    
    params = {}
    
    if job_id is not None:
        params["job_id"] = job_id
    
    if start_time_from is not None:
        params["start_time_from"] = start_time_from
    
    if start_time_to is not None:
        params["start_time_to"] = start_time_to
    
    if limit is not None:
        params["limit"] = limit
    
    if offset is not None:
        params["offset"] = offset
    
    if expand_tasks is not None:
        params["expand_tasks"] = expand_tasks
    
    if run_type is not None:
        params["run_type"] = run_type
    
    try:
        return await make_api_request("GET", "/api/2.1/jobs/runs/list", params=params)
    except Exception as e:
        logger.error(f"Failed to list workflow runs: {str(e)}")
        raise DatabricksAPIError(f"Failed to list workflow runs: {str(e)}")


async def get_run(run_id: int) -> Dict[str, Any]:
    """
    Get a specific job run.
    
    Args:
        run_id: ID of the run to get
        
    Returns:
        Response containing the run details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting workflow run: {run_id}")
    
    params = {
        "run_id": run_id
    }
    
    try:
        return await make_api_request("GET", "/api/2.1/jobs/runs/get", params=params)
    except Exception as e:
        logger.error(f"Failed to get workflow run: {str(e)}")
        raise DatabricksAPIError(f"Failed to get workflow run: {str(e)}")


async def get_run_output(run_id: int) -> Dict[str, Any]:
    """
    Get the output for a specific job run.
    
    Args:
        run_id: ID of the run to get output for
        
    Returns:
        Response containing the run output
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting output for workflow run: {run_id}")
    
    params = {
        "run_id": run_id
    }
    
    try:
        return await make_api_request("GET", "/api/2.1/jobs/runs/get-output", params=params)
    except Exception as e:
        logger.error(f"Failed to get workflow run output: {str(e)}")
        raise DatabricksAPIError(f"Failed to get workflow run output: {str(e)}")


async def export_run(
    run_id: int,
    views_to_export: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Export and retrieve a job run.
    
    Args:
        run_id: ID of the run to export
        views_to_export: Optional list of views to export (e.g., "CODE", "DASHBOARDS", "DIRECT_RESULTS")
        
    Returns:
        Response containing the exported run
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Exporting workflow run: {run_id}")
    
    data = {
        "run_id": run_id
    }
    
    if views_to_export is not None:
        data["views_to_export"] = views_to_export
    
    try:
        return await make_api_request("POST", "/api/2.1/jobs/runs/export", data=data)
    except Exception as e:
        logger.error(f"Failed to export workflow run: {str(e)}")
        raise DatabricksAPIError(f"Failed to export workflow run: {str(e)}")


async def cancel_run(
    run_id: int,
) -> Dict[str, Any]:
    """
    Cancel a job run.
    
    Args:
        run_id: ID of the run to cancel
        
    Returns:
        Empty response if successful
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Cancelling workflow run: {run_id}")
    
    data = {
        "run_id": run_id
    }
    
    try:
        return await make_api_request("POST", "/api/2.1/jobs/runs/cancel", data=data)
    except Exception as e:
        logger.error(f"Failed to cancel workflow run: {str(e)}")
        raise DatabricksAPIError(f"Failed to cancel workflow run: {str(e)}")


async def submit_run(
    run_name: Optional[str] = None,
    tasks: Optional[List[Dict[str, Any]]] = None,
    job_clusters: Optional[List[Dict[str, Any]]] = None,
    timeout_seconds: Optional[int] = None,
    idempotency_token: Optional[str] = None,
    access_control_list: Optional[List[Dict[str, Any]]] = None,
    git_source: Optional[Dict[str, Any]] = None,
    run_as: Optional[Dict[str, Any]] = None,
    webhook_notifications: Optional[Dict[str, Any]] = None,
    notification_settings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Submit a one-time run.
    
    Args:
        run_name: Optional name for the run
        tasks: Optional list of task specifications
        job_clusters: Optional list of job cluster specifications
        timeout_seconds: Optional timeout in seconds
        idempotency_token: Optional idempotency token for the run
        access_control_list: Optional access control list
        git_source: Optional Git source
        run_as: Optional user to run as
        webhook_notifications: Optional webhook notifications
        notification_settings: Optional notification settings
        
    Returns:
        Response containing the submitted run details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Submitting workflow run")
    
    data = {}
    
    if run_name is not None:
        data["run_name"] = run_name
    
    if tasks is not None:
        data["tasks"] = tasks
    
    if job_clusters is not None:
        data["job_clusters"] = job_clusters
    
    if timeout_seconds is not None:
        data["timeout_seconds"] = timeout_seconds
    
    if idempotency_token is not None:
        data["idempotency_token"] = idempotency_token
    
    if access_control_list is not None:
        data["access_control_list"] = access_control_list
    
    if git_source is not None:
        data["git_source"] = git_source
    
    if run_as is not None:
        data["run_as"] = run_as
    
    if webhook_notifications is not None:
        data["webhook_notifications"] = webhook_notifications
    
    if notification_settings is not None:
        data["notification_settings"] = notification_settings
    
    try:
        return await make_api_request("POST", "/api/2.1/jobs/runs/submit", data=data)
    except Exception as e:
        logger.error(f"Failed to submit workflow run: {str(e)}")
        raise DatabricksAPIError(f"Failed to submit workflow run: {str(e)}")


async def delete_run(
    run_id: int,
) -> Dict[str, Any]:
    """
    Delete a job run.
    
    Args:
        run_id: ID of the run to delete
        
    Returns:
        Empty response if successful
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting workflow run: {run_id}")
    
    data = {
        "run_id": run_id
    }
    
    try:
        return await make_api_request("POST", "/api/2.1/jobs/runs/delete", data=data)
    except Exception as e:
        logger.error(f"Failed to delete workflow run: {str(e)}")
        raise DatabricksAPIError(f"Failed to delete workflow run: {str(e)}")


async def repair_run(
    run_id: int,
    rerun_tasks: List[str],
    rerun_all_failed_tasks: Optional[bool] = None,
    latest_repair_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Repair a job run by re-running failed tasks.
    
    Args:
        run_id: ID of the run to repair
        rerun_tasks: List of task keys to rerun
        rerun_all_failed_tasks: Optional flag to rerun all failed tasks
        latest_repair_id: Optional latest repair ID
        
    Returns:
        Response containing the repaired run details
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Repairing workflow run: {run_id}")
    
    data = {
        "run_id": run_id,
        "rerun_tasks": rerun_tasks,
    }
    
    if rerun_all_failed_tasks is not None:
        data["rerun_all_failed_tasks"] = rerun_all_failed_tasks
    
    if latest_repair_id is not None:
        data["latest_repair_id"] = latest_repair_id
    
    try:
        return await make_api_request("POST", "/api/2.1/jobs/runs/repair", data=data)
    except Exception as e:
        logger.error(f"Failed to repair workflow run: {str(e)}")
        raise DatabricksAPIError(f"Failed to repair workflow run: {str(e)}")


async def cancel_all_runs(
    job_id: int,
) -> Dict[str, Any]:
    """
    Cancel all active runs for a job.
    
    Args:
        job_id: ID of the job to cancel runs for
        
    Returns:
        Empty response if successful
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Cancelling all runs for job: {job_id}")
    
    data = {
        "job_id": job_id
    }
    
    try:
        return await make_api_request("POST", "/api/2.1/jobs/runs/cancel-all", data=data)
    except Exception as e:
        logger.error(f"Failed to cancel all workflow runs: {str(e)}")
        raise DatabricksAPIError(f"Failed to cancel all workflow runs: {str(e)}") 