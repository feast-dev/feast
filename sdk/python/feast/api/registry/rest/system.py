from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from feast.version import get_version


class VersionResponse(BaseModel):
    """Runtime version information for the Feast server."""

    version: str


def get_system_router() -> APIRouter:
    router = APIRouter()

    @router.get("/version", response_model=VersionResponse, tags=["System"])
    def get_feast_version() -> VersionResponse:
        """Return the installed Feast package version for this process."""
        return VersionResponse(version=get_version())

    return router
