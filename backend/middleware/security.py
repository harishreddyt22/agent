"""
backend/middleware/security.py
Security headers middleware.
"""
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"]        = "SAMEORIGIN"
        response.headers["X-XSS-Protection"]       = "1; mode=block"
        return response


def add_security_headers(app):
    app.add_middleware(SecurityHeadersMiddleware)
