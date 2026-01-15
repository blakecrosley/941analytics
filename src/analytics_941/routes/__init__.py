"""
Analytics dashboard routes.

Modular route system using Jinja2 templates for cleaner separation of concerns.
"""

from .dashboard import create_dashboard_router

__all__ = ["create_dashboard_router"]
