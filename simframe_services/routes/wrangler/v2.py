"""V2 API routes for wrangler service.

This module contains the V2 API routes for the wrangler service.
Routes are prefixed with /v2 for versioning.

TODO: Implement V2 routes with enhanced functionality.
"""

from typing import TYPE_CHECKING

from fastapi import APIRouter
from fastapi import status as http_status
from fastapi.responses import JSONResponse
from fastapi import HTTPException
import requests
import asyncio
import time, httpx

if TYPE_CHECKING:
    from simframe_services.wrangler import WranglerServer

from simframe_services.schemas import ModelRegistration, LatticeJob


def setup_v2_routes(app, server: "WranglerServer"):
    """Setup V2 API routes on the FastAPI app.

    Args:
        app: The FastAPI application instance
        server: The WranglerServer instance
    """
    router = APIRouter(prefix="/v2", tags=["v2"])

    @router.get("/health", status_code=http_status.HTTP_200_OK)
    async def health_check():
        """V2 health check endpoint."""
        return JSONResponse(
            content={
                "status": "ok",
                "version": "2.0.0",
            },
            status_code=http_status.HTTP_200_OK,
        )

    @router.post("/register_model", status_code=http_status.HTTP_201_CREATED)
    async def register_model_v2(model: ModelRegistration):
        """V2 endpoint to register a new model."""
        if model.model_id in server.registered_models:
            raise HTTPException(
                status_code=http_status.HTTP_400_BAD_REQUEST,
                detail="Model ID already exists.",
            )
        server.registered_models[model.model_id] = model
        return {"message": "Model registered successfully.", "model": model}

    @router.delete("/delete_model/{model_id}", status_code=http_status.HTTP_200_OK)
    async def delete_model_v2(model_id: str):
        """V2 endpoint to delete a registered model."""
        if model_id not in server.registered_models:
            raise HTTPException(
                status_code=http_status.HTTP_404_NOT_FOUND, detail="Model not found."
            )
        del server.registered_models[model_id]
        return {"message": "Model deleted successfully."}

    @router.get("/models/{model_id}", response_model=ModelRegistration)
    async def get_model_v2(model_id: str):
        """V2 endpoint to get model registration information."""
        model = server.registered_models.get(model_id)
        if not model:
            raise HTTPException(
                status_code=http_status.HTTP_404_NOT_FOUND, detail="Model not found."
            )
        return model

    @router.get("/list_models/", response_model=dict)
    async def list_models_v2():
        """V2 endpoint to list all registered models."""
        return server.registered_models

    @router.get("/get_result/{job_id}", response_class=JSONResponse)
    async def get_result(job_id: str):
        # get results for a specific job
        if job_id not in server.results:
            # check if job is still processing
            if job_id in server.jobs:
                return JSONResponse(
                    content={"message": "Job is still processing."},
                    status_code=http_status.HTTP_202_ACCEPTED,
                )
            else:
                raise HTTPException(
                    status_code=http_status.HTTP_404_NOT_FOUND, detail="Job not found."
                )

        return JSONResponse(content=server.results[job_id])

    @router.post("/submit_job/{model_id}", response_class=JSONResponse)
    async def submit_lattice(model_id: str, lattice_job: LatticeJob):
        # forwards the lattice to the model for processing
        if model_id not in server.registered_models:
            raise HTTPException(
                status_code=http_status.HTTP_404_NOT_FOUND, detail="Model not found."
            )
        model = server.registered_models[model_id]

        response = requests.post(
            f"{model.api_url}/submit_lattice", json=lattice_job.model_dump()
        )

        if response.status_code != 202:
            raise HTTPException(
                status_code=http_status.HTTP_502_BAD_GATEWAY,
                detail="Failed to submit lattice to model.",
            )

        # start the async polling for results

        asyncio.create_task(
            _poll_model_for_results(server, model, response.json().get("job_id"))
        )

        return {
            "message": "Lattice submitted successfully.",
            "response": response.json(),
        }

    @router.get("/list_results/", response_class=JSONResponse)
    async def list_results():
        # list all results currently stored
        # we need to limit this to avoid huge responses
        limited_results = {
            job_id: server.results[job_id] for job_id in list(server.results)[:10]
        }
        return JSONResponse(content=limited_results)

    @router.get("/results_count/", response_class=JSONResponse)
    async def results_count():
        # return the count of results stored
        return JSONResponse(content={"results_count": len(server.results)})

    
    @router.delete("/clear_results/", response_class=JSONResponse)
    async def clear_results():
        # clear all stored results
        server.results.clear()
        return JSONResponse(content={"message": "All results cleared."})
    
    
    app.include_router(router)
    
    


async def _poll_model_for_results(
    server: "WranglerServer", model: ModelRegistration, job_id: str
):
    """Poll the model for results.

    Args:
        server: The WranglerServer instance
        model: The model registration
        job_id: The job ID to poll for
    """
    # start time
    start = time.time()  # we will timeout after 60 seconds
    async with httpx.AsyncClient() as client:
        while True:
            try:
                server.jobs[job_id] = {
                    "model_id": model.model_id,
                    "timestamp": time.time(),
                    "status": "processing",
                }
                response = await client.get(
                    f"{model.api_url}/get_result", params={"job_id": job_id}
                )
                print(
                    f"Polling {model.api_url}/get_result?job_id={job_id} - Status: {response.status_code}"
                )
                if response.status_code == 200:
                    # save results
                    server.results[job_id] = response.json()
                    server.add_kafka_job_id(job_id)
                    print(f"Saved results for job {job_id}.")
                    # clear job result from model
                    await client.delete(
                        f"{model.api_url}/clear_result", params={"job_id": job_id}
                    )
                    print(
                        f"Cleared results for job {job_id} from model {model.model_id}."
                    )

                    server.jobs[job_id]["status"] = "completed"

                    break
            except Exception as e:
                print(f"Error polling for results: {e}")

            # wait and poll again
            await asyncio.sleep(1)
            if time.time() - start > 60:
                # timeout after 60 seconds
                print(f"Timeout polling for results for job {job_id}")
                break
