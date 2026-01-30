import time
from uuid import uuid4
from typing import Dict, Any, List, Optional
import threading
import socket

import uvicorn
from fastapi import FastAPI
import requests

from pydantic import BaseModel

from poly_lithic.src.logging_utils import get_logger
from poly_lithic.src.interfaces.BaseInterface import BaseInterface
from poly_lithic.src.transformers.BaseTransformer import BaseTransformer

from simframe_services.schemas import PutRequest, ModelRegistration, LatticeJob
from simframe_services.routes.model import setup_v1_routes

logger = get_logger()


class k2simFrame(BaseInterface):
    """Interface for poly_lithic models to receive jobs from the wrangler.

    The model exposes a REST API that the wrangler calls to:
    1. Get model settings (lattice_section, beam_properties, machine_settings)
    2. Submit lattice simulation jobs
    3. Retrieve simulation results

    Data flow:
    - Wrangler POST /submit_lattice → stores in self.jobs
    - poly_lithic get('jobs') → returns current job from self.jobs
    - poly_lithic put('results', data) → stores in self.results
    - Wrangler GET /get_result → retrieves from self.results

    Variable list:
    - 'jobs': Published by this interface (OUTPUT) - job data for processing
    - 'results': Accepted by this interface (INPUT) - processed results
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize the k2simFrame interface.

        Args:
            config: Configuration dictionary with:
                - host: Host to bind this model's API to (default: "0.0.0.0")
                - port: Port for this model's API (required)
                - model_name: Name of this model instance
                - wrangler_url: URL of wrangler for registration (optional)
                - variable_list: List of variables this interface handles (default: ["jobs", "results"])
        """
        super().__init__(config)

        # API server settings
        self.host = config.get("host", "0.0.0.0")
        self.port = config.get("port")
        if not self.port:
            raise ValueError("port is required in config")

        # Auto-detect hostname (works in containers and bare metal)
        self.hostname = socket.gethostname()

        # Model identification
        self.model_id = config.get("model_name", str(uuid4()))
        self.model_name = config.get("model_name", f"model-{self.port}")

        # Settings that SimFrame will query
        self.settings = config.get(
            "settings",
            {"lattice_section": "", "beam_properties": [], "machine_settings": []},
        )

        # Variable list - ESSENTIAL for poly_lithic messaging
        # Declares what variables this interface provides/accepts
        self.variable_list = config.get("variable_list", ["jobs", "results"])
        logger.info(f"Interface variables: {self.variable_list}")

        # Job storage - single source of truth
        self.jobs = {}  # job_id -> {data, submitted_at, status, last_retrieved}
        self.results = {}  # job_id -> {beam, completed_at}

        # Track current job being processed
        self.current_job_id = None

        # Track what was last returned to poly_lithic to detect changes
        self._last_job_returned = None

        # Setup FastAPI application
        self.app = FastAPI(title=f"{self.model_name} API")
        self._setup_routes()

        logger.info(
            f"k2simFrame interface initialized: {self.model_name} ({self.model_id})"
        )
        logger.info(f"API server binding to: {self.host}:{self.port}")
        logger.info(f"API accessible at: http://{self.hostname}:{self.port}")

        # Start API server in background thread
        self.server_thread = threading.Thread(target=self._run_api_server, daemon=True)
        self.server_thread.start()
        logger.info(f"Model API started on {self.host}:{self.port}")

        # Register with wrangler
        self._register_with_wrangler(
            config.get("wrangler_url", "http://localhost:8000")
        )

    def _setup_routes(self):
        """Setup FastAPI routes - delegates to v1 and v2 route modules."""
        setup_v1_routes(self)

    def _run_api_server(self):
        """Run the FastAPI server in a background thread."""
        uvicorn.run(self.app, host=self.host, port=self.port, log_level="warning")

    def _register_with_wrangler(self, wrangler_url: str):
        """Register this model with the wrangler."""
        logger.info(
            f"Registering model {self.model_name} with wrangler at {wrangler_url}"
        )

        try:
            # Use hostname for registration (works both in Docker and bare metal)
            registration = ModelRegistration(
                model_id=self.model_id,
                model_name=self.model_name,
                api_url=f"http://{self.hostname}:{self.port}",
                timestamp=time.time(),
            )
            response = requests.post(
                f"{wrangler_url}/register_model", json=registration.model_dump()
            )
            response.raise_for_status()
            logger.info(f"Successfully registered with wrangler: {response.json()}")
            return True
        except requests.RequestException as e:
            logger.warning(f"Failed to register with wrangler: {e}")
            logger.warning("Model will continue but won't be visible to wrangler")
            return False

    # ===== BaseInterface methods - poly_lithic messaging integration =====

    def get(self, name: str, **kwargs) -> tuple[str, Optional[Dict[str, Any]]]:
        """Get a value from job/result storage.

        Called by poly_lithic when a component subscribes to this variable.
        This interface PUBLISHES 'jobs' data to downstream components.

        Args:
            name: Variable name (must be in self.variable_list)

        Returns:
            Tuple of (name, value_dict) where value_dict has {'value': data, 'timestamp': ts}
            Returns None if no data available or data unchanged.
        """
        # Validate variable is in our list
        if name not in self.variable_list:
            logger.warning(
                f"Variable '{name}' not in variable_list: {self.variable_list}"
            )
            return name, None

        if name == "jobs":
            if self.current_job_id and self.current_job_id in self.jobs:
                job_info = self.jobs[self.current_job_id]
                job_data = job_info["data"]

                logger.info(f"Publishing job {self.current_job_id} to poly_lithic")
                self._last_job_returned = self.current_job_id
                job_info["last_retrieved"] = time.time()

                return name, {
                    "value": job_data,
                    "timestamp": job_info["submitted_at"],
                    "job_id": self.current_job_id,
                }
            else:
                # No current job
                logger.debug("No current job to publish")
                return name, None

        elif name == "results":
            # Results flow IN via put(), not OUT via get()
            # This is a sink variable, not a source
            logger.debug("Variable 'results' is an input, not published")
            return name, None

        else:
            logger.warning(f"Unknown variable '{name}' requested")
            return name, None

    def get_many(
        self, names: List[str], **kwargs
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """Get multiple values from storage.

        Used by poly_lithic to fetch multiple variables at once.
        """
        # print(names)
        # print(self.jobs)
        # print(self.results)
        output_dict = {}
        for name in names:
            if name not in self.variable_list:
                logger.warning(f"Skipping '{name}' - not in variable_list")
                continue
            _, value = self.get(name)
            if value is not None:
                output_dict[name] = value
        return output_dict

    def put(self, name: str, value: Any, **kwargs):
        """Store processed results.

        Called by poly_lithic when a component publishes results.
        This interface ACCEPTS 'results' data from upstream components.

        Args:
            name: Variable name (must be in self.variable_list)
            value: Result data (beam dict)
        """
        # Validate variable is in our list
        if name not in self.variable_list:
            logger.warning(
                f"Rejecting put to '{name}' - not in variable_list: {self.variable_list}"
            )
            return

        if name == "results":
            # Store result for current job
            if self.current_job_id:
                job_id = self.current_job_id

                self.results[job_id] = {"beam": value, "completed_at": time.time()}

                # Remove completed job from jobs dict
                if job_id in self.jobs:
                    del self.jobs[job_id]
                    logger.info(f"Job {job_id} completed and removed from jobs queue")

                # Move to next queued job if any
                self.current_job_id = None
                self._last_job_returned = None

                for jid, jinfo in self.jobs.items():
                    if jinfo["status"] == "queued":
                        self.current_job_id = jid
                        jinfo["status"] = "processing"
                        logger.info(f"Started processing queued job {jid}")
                        break

                if self.current_job_id is None:
                    logger.info("No more queued jobs")
            else:
                logger.warning("Received results but no current job")

        elif name == "jobs":
            logger.warning(
                f"Unexpected put to 'jobs' - this variable is published, not accepted"
            )

        else:
            logger.warning(f"Unexpected put to variable '{name}'")

    def put_many(self, data: Dict[str, Any], **kwargs):
        """Store multiple values.

        Used by poly_lithic to update multiple variables at once.
        """
        for name, value in data.items():
            self.put(name, value)

    def monitor(self, name: str, handler: callable, **kwargs):
        """Set up monitoring for a variable (not implemented for REST)."""
        logger.warning(f"Monitor not implemented for REST interface")

    def set_settings(self, settings: Dict[str, Any]):
        """Update model settings (can be called programmatically)."""
        self.settings.update(settings)
        logger.info(f"Updated settings: {self.settings}")

    def close(self):
        """Cleanup resources."""
        logger.info(f"k2simFrame interface closed: {self.model_name}")

    def is_running(self) -> bool:
        """Check if the API server is running."""
        return self.server_thread is not None and self.server_thread.is_alive()


class k2simFrameTransformer(BaseTransformer):
    """A transformer that uses the k2simFrame interface."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        logger.info("k2simFrameTransformer initialized with k2simFrame interface")
        self.updated = False
        self.latest_transformed = {}

    def transform(self):
        """Example transform method that could be expanded"""

        logger.info("k2simFrameTransformer transform called")
        # Here you would implement the logic to interact with the k2simFrame interface
        # For example, fetching jobs, processing them, and putting results back

    def handler(self, name, value):
        # print("Handling message in k2simFrameTransformer")
        # print(name, value)
        self.latest_transformed = {"results": value}
        self.updated = True
