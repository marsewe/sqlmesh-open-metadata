"""Example SQLMesh config.py with Open-Metadata integration.

This example shows how to configure SQLMesh to automatically emit lineage
to Open-Metadata.

Prerequisites:
1. Open-Metadata instance running at http://localhost:8585
2. Database service created in Open-Metadata matching the namespace
3. Tables cataloged in Open-Metadata (use ingestion connectors)
"""

import sqlmesh_openlineage

# Install Open-Metadata integration before importing Config
# This will intercept SQLMesh's console and emit lineage automatically
sqlmesh_openlineage.install(
    url="http://localhost:8585/api",
    namespace="my_database_service",  # Must match database service name in Open-Metadata
    api_key="your_jwt_token_here",  # Get from Open-Metadata UI > Settings > Bots
)

from sqlmesh.core.config import Config

config = Config(
    model_defaults={
        "dialect": "duckdb",
    },
    gateways={
        "local": {
            "connection": {
                "type": "duckdb",
                "database": ":memory:",
            }
        }
    },
    default_gateway="local",
)
