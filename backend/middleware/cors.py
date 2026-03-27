"""
backend/middleware/cors.py
CORS middleware configuration.
"""
from fastapi.middleware.cors import CORSMiddleware


def add_cors(app):
    app.add_middleware(
        CORSMiddleware,
        allow_origins     = ["*"],   # tighten to specific domains in production
        allow_credentials = True,
        allow_methods     = ["*"],
        allow_headers     = ["*"],
    )
