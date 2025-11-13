from fastapi import FastAPI, HTTPException, Request
from fastapi import status as http_status
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel
from typing import Dict, Any, Optional

class PutRequest(BaseModel):
    """Request model for PUT operations."""
    value: Any


class ModelRegistration(BaseModel):
    """Model registration information."""
    model_id: str
    model_name: Optional[str] = None
    api_url: str = None
    timestamp: float


class LatticeJob(BaseModel):
    model: str
    beam: Dict[str, Any]
    lattice_name: str
    lattice: Dict[str, Any]
    job_id: Optional[str] = None