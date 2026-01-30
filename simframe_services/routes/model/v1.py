"""V1 API routes for k2simFrame interface.

This module contains the original API routes for backward compatibility.
All routes maintain their original paths without versioning prefix.
"""

import time
from typing import TYPE_CHECKING

from fastapi import HTTPException
from fastapi import status as http_status
from fastapi.responses import JSONResponse

from poly_lithic.src.logging_utils import get_logger
from simframe_services.schemas import LatticeJob

if TYPE_CHECKING:
    from simframe_services.plugins import k2simFrame

logger = get_logger()


def setup_v1_routes(interface: "k2simFrame"):
    """Setup V1 API routes on the FastAPI app.

    Args:
        interface: The k2simFrame interface instance
    """
    app = interface.app

    @app.get("/ping", status_code=http_status.HTTP_200_OK)
    async def ping():
        """Health check endpoint."""
        return JSONResponse(
            content={
                "message": "pong",
            },
            status_code=http_status.HTTP_200_OK,
        )

    @app.get("/get_settings", status_code=http_status.HTTP_200_OK)
    async def get_settings():
        """Get model settings (called by SimFrame via wrangler)."""
        return JSONResponse(
            content={
                "model": interface.model_name,
                "lattice_section": interface.settings.get("lattice_section", ""),
                "beam_properties": interface.settings.get("beam_properties", []),
                "machine_settings": interface.settings.get("machine_settings", []),
            },
            status_code=http_status.HTTP_200_OK,
        )

    @app.post("/submit_lattice", status_code=http_status.HTTP_202_ACCEPTED)
    async def submit_lattice(job: LatticeJob):
        """Submit a lattice simulation job (called by wrangler).

        Expected input:
            - model: str
            - beam: dict (json)
            - lattice_name: str
            - lattice: dict (json)
            - job_id: str (optional)
        """
        job_data = job.model_dump()
        job_id = job_data.get("job_id")  # must have job_id
        if not job_id:
            raise HTTPException(
                status_code=http_status.HTTP_400_BAD_REQUEST,
                detail="job_id is required",
            )

        else:
            # Store job - this is the single source of truth
            interface.jobs[job_id] = {
                "data": job_data,
                "submitted_at": time.time(),
                "status": "queued",
                "last_retrieved": None,
            }

            # If no current job, make this the current one
            if interface.current_job_id is None:
                interface.current_job_id = job_id
                interface.jobs[job_id]["status"] = "processing"
                logger.info(f"Job {job_id} set as current job")

            logger.info(
                f"Received job {job_id}: {job_data.get('lattice_name')} "
                f"(status: {interface.jobs[job_id]['status']})"
            )

        return JSONResponse(
            content={
                "message": "Job accepted",
                "job_id": job_id,
                "status": interface.jobs[job_id]["status"],
            },
            status_code=http_status.HTTP_202_ACCEPTED,
        )

    @app.get("/get_jobs", status_code=http_status.HTTP_200_OK)
    async def get_jobs():
        """Get list of all jobs (for debugging)."""
        return JSONResponse(
            content={
                "current_job_id": interface.current_job_id,
                "jobs": {
                    job_id: {
                        "status": job_info["status"],
                        "submitted_at": job_info["submitted_at"],
                        "lattice_name": job_info["data"].get("lattice_name"),
                    }
                    for job_id, job_info in interface.jobs.items()
                },
            },
            status_code=http_status.HTTP_200_OK,
        )

    @app.get("/get_result", status_code=http_status.HTTP_200_OK)
    async def get_result(job_id: str):
        """Get result for a specific job (called by wrangler)."""
        if job_id not in interface.results:
            # Check if job exists but isn't complete
            if job_id in interface.jobs:
                status = interface.jobs[job_id]["status"]
                raise HTTPException(
                    status_code=http_status.HTTP_202_ACCEPTED,
                    detail=f"Job {job_id} is still {status}",
                )
            raise HTTPException(
                status_code=http_status.HTTP_404_NOT_FOUND,
                detail=f"Job {job_id} not found",
            )

        result = interface.results[job_id]
        logger.info(f"Returning result for job {job_id}")

        return JSONResponse(
            content={"job_id": job_id, "beam": result.get("beam", {})},
            status_code=http_status.HTTP_200_OK,
        )

    @app.get("/status", status_code=http_status.HTTP_200_OK)
    async def get_status():
        """Get model status (for debugging)."""
        return JSONResponse(
            content={
                "model_id": interface.model_id,
                "model_name": interface.model_name,
                "api_url": f"http://{interface.hostname}:{interface.port}",
                "variables": interface.variable_list,
                "current_job_id": interface.current_job_id,
                "queued_jobs": len(
                    [j for j in interface.jobs.values() if j["status"] == "queued"]
                ),
                "processing_jobs": len(
                    [j for j in interface.jobs.values() if j["status"] == "processing"]
                ),
                "completed_jobs": len(interface.results),
            },
            status_code=http_status.HTTP_200_OK,
        )

    @app.delete("/clear_result", status_code=http_status.HTTP_200_OK)
    async def clear_result(job_id: str):
        """Clear result for a specific job (for debugging)."""
        if job_id in interface.results:
            del interface.results[job_id]
            logger.info(f"Cleared result for job {job_id}")
            return JSONResponse(
                content={"message": f"Result for job {job_id} cleared"},
                status_code=http_status.HTTP_200_OK,
            )
        else:
            raise HTTPException(
                status_code=http_status.HTTP_404_NOT_FOUND,
                detail=f"Result for job {job_id} not found",
            )
