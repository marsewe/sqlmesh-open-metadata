# DRAFT, created by github agent

# sqlmesh-openlineage

Open-Metadata integration for SQLMesh. Automatically emits lineage to Open-Metadata for table and column-level lineage tracking.

## Features

- **Table-level lineage**: Track which models depend on which upstream models
- **Column-level lineage**: Track which columns flow from source to destination
- **Schema capture**: Column names and types for each model
- **Automatic emission**: Lineage is automatically sent to Open-Metadata during SQLMesh runs
- **Per-model lineage**: Lineage edges created for each model evaluation

## Prerequisites

Before using this package, you need to:

1. **Set up Open-Metadata**: Have an Open-Metadata instance running (e.g., `http://localhost:8585`)
2. **Create a Database Service**: Create a database service in Open-Metadata with a name matching your `namespace` parameter
3. **Catalog Tables**: Use Open-Metadata's ingestion framework to catalog your tables first, or the lineage emission will be skipped for tables that don't exist

The package will attempt to look up tables by their fully qualified name (FQN) in the format: `<namespace>.<catalog>.<schema>.<table>`. If a table doesn't exist in Open-Metadata, lineage for that table will be silently skipped.

## Installation

```bash
pip install sqlmesh-openlineage
```

Or with uv:

```bash
uv add sqlmesh-openlineage
```

## Quick Start (CLI Users)

**Note:** This package requires Python-based SQLMesh configuration (`config.py`), not YAML configuration.

Add this to your `config.py`:

```python
import sqlmesh_openlineage

sqlmesh_openlineage.install(
    url="http://localhost:8585/api",
    namespace="my_database_service",
    api_key="your_jwt_token",
)

from sqlmesh.core.config import Config

config = Config(
    # ... your existing config
)
```

Then run `sqlmesh run` as normal. Lineage will be emitted to Open-Metadata for each model evaluation.

## Environment Variables

You can also configure via environment variables:

```bash
export OPENMETADATA_URL=http://localhost:8585/api
export OPENMETADATA_NAMESPACE=my_database_service
export OPENMETADATA_API_KEY=your_jwt_token
```

Then in `config.py`:

```python
import sqlmesh_openlineage
sqlmesh_openlineage.install()  # reads from env vars
```

## How It Works

This package uses SQLMesh's `set_console()` API to inject a custom Console wrapper. The wrapper intercepts per-snapshot lifecycle events and emits corresponding lineage to Open-Metadata:

- When a model evaluation completes successfully, lineage edges are created from upstream tables to the output table
- Column-level lineage is extracted using SQLMesh's built-in lineage analysis
- Tables must exist in Open-Metadata before lineage can be created

## Lineage Emission

| SQLMesh Event | Open-Metadata Action | Data Included |
|---------------|---------------------|---------------|
| Model evaluation success | Add lineage edge | Table-level lineage, column-level lineage |
| Model evaluation failure | (None) | Failures are not tracked in lineage |

## Column-Level Lineage

The integration automatically extracts column-level lineage using SQLMesh's built-in lineage analysis. For example, if you have:

```sql
-- customers.sql
SELECT customer_id, name, email FROM raw_customers

-- customer_summary.sql
SELECT
    c.customer_id,
    c.name as customer_name,
    COUNT(o.order_id) as total_orders
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
```

The lineage will show that `customer_summary.customer_name` traces back to `customers.name`.

## Testing with Open-Metadata

```bash
# Start Open-Metadata (requires Docker)
docker run -d -p 8585:8585 --name openmetadata openmetadata/server:latest

# Configure and run SQLMesh
export OPENMETADATA_URL=http://localhost:8585/api
export OPENMETADATA_NAMESPACE=my_database_service
export OPENMETADATA_API_KEY=your_jwt_token
sqlmesh run

# View lineage at http://localhost:8585
```

**Note:** You'll need to:
1. Create a database service in Open-Metadata matching your `namespace`
2. Ensure tables exist in Open-Metadata before lineage can be created
3. Use Open-Metadata ingestion to initially catalog your tables, or create them manually

## Development

```bash
# Install dependencies
uv sync --dev

# Run tests (unit + integration)
uv run pytest tests/ -v
```

## License

MIT
