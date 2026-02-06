"""V1 API routes for wrangler service.

This module contains the original API routes for the wrangler service.
All routes maintain their original paths without versioning prefix.
"""

import time
import asyncio
from typing import TYPE_CHECKING, Dict

import requests
import httpx
from fastapi import HTTPException
from fastapi import status as http_status
from fastapi.responses import JSONResponse

from simframe_services.schemas import ModelRegistration, LatticeJob

if TYPE_CHECKING:
    from simframe_services.wrangler import WranglerServer


def setup_v1_routes(app, server: "WranglerServer"):
    """Setup V1 API routes on the FastAPI app.

    Args:
        app: The FastAPI application instance
        server: The WranglerServer instance
    """

    @app.post("/register_model", status_code=http_status.HTTP_201_CREATED)
    async def register_model(model: ModelRegistration):
        """Register a new model."""

        if model.model_id in server.registered_models:
            raise HTTPException(
                status_code=http_status.HTTP_400_BAD_REQUEST,
                detail="Model ID already exists.",
            )
        server.registered_models[model.model_id] = model
        return {"message": "Model registered successfully.", "model": model}

    @app.delete("/delete_model/{model_id}", status_code=http_status.HTTP_200_OK)
    async def delete_model(model_id: str):
        """Delete a registered model."""
        if model_id not in server.registered_models:
            raise HTTPException(
                status_code=http_status.HTTP_404_NOT_FOUND, detail="Model not found."
            )
        del server.registered_models[model_id]
        return {"message": "Model deleted successfully."}

    @app.get("/models/{model_id}", response_model=ModelRegistration)
    async def get_model(model_id: str):
        """Get model registration information."""
        model = server.registered_models.get(model_id)
        if not model:
            raise HTTPException(
                status_code=http_status.HTTP_404_NOT_FOUND, detail="Model not found."
            )
        return model

    @app.get("/list_models/", response_model=Dict[str, ModelRegistration])
    async def list_models():
        return server.registered_models

    # proxied endpoints

    @app.get("/get_settings/{model_id}", response_class=JSONResponse)
    async def get_settings(model_id: str):
        # forwards the request to the model to get its settings
        if model_id not in server.registered_models:
            raise HTTPException(
                status_code=http_status.HTTP_404_NOT_FOUND, detail="Model not found."
            )
        model = server.registered_models[model_id]

        model_settings = requests.get(f"{model.api_url}/get_settings")

        if model_settings.status_code != 200:
            raise HTTPException(
                status_code=http_status.HTTP_502_BAD_GATEWAY,
                detail="Failed to get settings from model.",
            )
        return JSONResponse(content=model_settings.json())

    @app.post("/submit_lattice/{model_id}", response_class=JSONResponse)
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

    @app.get("/list_results/", response_class=JSONResponse)
    async def list_results():
        # list all results
        return JSONResponse(content=server.results)

    @app.get("/get_result/{job_id}", response_class=JSONResponse)
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
