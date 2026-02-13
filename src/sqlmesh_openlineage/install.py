"""Install Open-Metadata integration for SQLMesh CLI usage."""
from __future__ import annotations

import os
import typing as t

_installed = False


def install(
    url: t.Optional[str] = None,
    namespace: str = "sqlmesh",
    api_key: t.Optional[str] = None,
) -> None:
    """
    Install Open-Metadata integration for CLI usage.

    Call this in your config.py BEFORE importing Config.
    Uses SQLMesh's set_console() to inject Open-Metadata lineage emission.

    Args:
        url: Open-Metadata API URL. Falls back to OPENMETADATA_URL or OPENLINEAGE_URL env var.
        namespace: Database service name in Open-Metadata. Falls back to OPENMETADATA_NAMESPACE or OPENLINEAGE_NAMESPACE env var.
        api_key: Optional JWT token. Falls back to OPENMETADATA_API_KEY or OPENLINEAGE_API_KEY env var.

    Example:
        # config.py
        import sqlmesh_openlineage

        sqlmesh_openlineage.install(
            url="http://localhost:8585/api",
            namespace="my_database_service",
            api_key="your_jwt_token",
        )

        from sqlmesh.core.config import Config
        config = Config(...)
    """
    global _installed

    if _installed:
        return

    from sqlmesh.core.console import set_console, create_console
    from sqlmesh_openlineage.console import OpenLineageConsole

    # Resolve config from args or env vars
    # Support both OPENMETADATA_* and OPENLINEAGE_* for backwards compatibility
    resolved_url = url or os.environ.get("OPENMETADATA_URL") or os.environ.get("OPENLINEAGE_URL")
    resolved_namespace = (
        namespace
        or os.environ.get("OPENMETADATA_NAMESPACE")
        or os.environ.get("OPENLINEAGE_NAMESPACE")
        or "sqlmesh"
    )
    resolved_api_key = (
        api_key
        or os.environ.get("OPENMETADATA_API_KEY")
        or os.environ.get("OPENLINEAGE_API_KEY")
    )

    if not resolved_url:
        raise ValueError(
            "Open-Metadata URL required. Pass url= or set OPENMETADATA_URL env var."
        )

    # Create the default console for the current environment
    default_console = create_console()

    # Wrap it with Open-Metadata emission
    ol_console = OpenLineageConsole(
        wrapped=default_console,
        url=resolved_url,
        namespace=resolved_namespace,
        api_key=resolved_api_key,
    )

    # Set as global console - SQLMesh's CLI will use this
    set_console(ol_console)

    _installed = True


def is_installed() -> bool:
    """Check if Open-Metadata integration is installed."""
    return _installed
