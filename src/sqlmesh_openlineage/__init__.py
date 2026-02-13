"""SQLMesh Open-Metadata integration.

This package provides Open-Metadata lineage emission for SQLMesh without requiring
any modifications to SQLMesh itself. It uses SQLMesh's set_console() API to
inject a custom Console wrapper that emits lineage to Open-Metadata.

## Quick Start (CLI Users)

Add this to your `config.py`:

```python
import sqlmesh_openlineage

sqlmesh_openlineage.install(
    url="http://localhost:8585/api",
    namespace="my_database_service",
    api_key="your_jwt_token",
)

from sqlmesh.core.config import Config
config = Config(...)
```

Then run `sqlmesh run` as normal.

## Environment Variables

You can also configure via environment variables:

```bash
export OPENMETADATA_URL=http://localhost:8585/api
export OPENMETADATA_NAMESPACE=my_database_service
export OPENMETADATA_API_KEY=your_jwt_token  # optional
```

## Programmatic Usage

```python
from sqlmesh_openlineage import OpenLineageConsole
from sqlmesh.core.console import set_console, create_console

console = OpenLineageConsole(
    wrapped=create_console(),
    url="http://localhost:8585/api",
    namespace="my_database_service",
    api_key="your_jwt_token",
)
set_console(console)
```
"""

from sqlmesh_openlineage.install import install, is_installed
from sqlmesh_openlineage.console import OpenLineageConsole
from sqlmesh_openlineage.emitter import OpenLineageEmitter

__version__ = "0.1.0"

__all__ = [
    "install",
    "is_installed",
    "OpenLineageConsole",
    "OpenLineageEmitter",
]
