"""API route modules for wrangler service."""

from simframe_services.routes.wrangler.v1 import setup_v1_routes
from simframe_services.routes.wrangler.v2 import setup_v2_routes

__all__ = ["setup_v1_routes", "setup_v2_routes"]
