"""API route modules for k2simFrame interface."""

from simframe_services.routes.v1 import setup_v1_routes
from simframe_services.routes.v2 import setup_v2_routes

__all__ = ['setup_v1_routes', 'setup_v2_routes']
