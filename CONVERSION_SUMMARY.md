# Open-Metadata Conversion Summary

This document summarizes the conversion from OpenLineage/Marquez to Open-Metadata.

## What Changed

### 1. Dependencies (pyproject.toml)
- **Removed**: `openlineage-python>=1.0.0`
- **Added**: `openmetadata-ingestion>=1.0.0`
- Updated keywords and description

### 2. Core Implementation

#### emitter.py
- Replaced `OpenLineageClient` with `OpenMetadata` client
- Changed from event-based model (START/COMPLETE/FAIL) to lineage-based model
- Implemented `AddLineageRequest` API for creating lineage edges
- Added table lookup/caching logic (tables must exist in Open-Metadata)
- Lineage is now emitted in `emit_snapshot_complete()` only

#### datasets.py
- Removed OpenLineage dataset conversion functions
- Added `snapshot_to_table_fqn()` for Open-Metadata FQN format
- Added `snapshot_to_column_lineage()` to create ColumnLineage objects
- Simplified implementation (no more facets)

#### install.py
- Updated environment variable support (OPENMETADATA_* with OPENLINEAGE_* fallback)
- Updated docstrings and examples

#### facets.py
- Converted to stub file (Open-Metadata doesn't use facets)
- Functions now return empty dicts

#### console.py
- Updated docstrings to reference Open-Metadata
- No logic changes (still intercepts same lifecycle events)

### 3. Documentation

#### README.md
- Updated all examples to use Open-Metadata
- Added Prerequisites section explaining setup requirements
- Updated environment variables
- Changed testing instructions from Marquez to Open-Metadata

#### docker-compose.yml
- Replaced Marquez stack with Open-Metadata server
- Simplified to single service

#### example_config.py (new)
- Added example configuration file
- Shows proper setup with comments

### 4. Tests

#### conftest.py
- Updated mock to use Open-Metadata client

#### test_datasets.py
- Removed tests for deprecated functions
- Added tests for new functions

## Key Differences: OpenLineage vs Open-Metadata

### OpenLineage (Before)
- **Event-based**: Emit START/COMPLETE/FAIL events
- **Real-time**: Events sent as jobs run
- **Self-contained**: Creates datasets automatically
- **Facets**: Rich metadata via facets

### Open-Metadata (After)
- **Lineage-based**: Create lineage edges between tables
- **Post-completion**: Lineage sent after successful completion
- **Requires setup**: Tables must exist in Open-Metadata first
- **Simpler model**: Just lineage edges with optional column lineage

## Migration Guide for Users

### Before (OpenLineage)
```python
import sqlmesh_openlineage

sqlmesh_openlineage.install(
    url="http://localhost:5000",
    namespace="my_project",
)
```

### After (Open-Metadata)
```python
import sqlmesh_openlineage

sqlmesh_openlineage.install(
    url="http://localhost:8585/api",
    namespace="my_database_service",  # Must match DB service in Open-Metadata
    api_key="your_jwt_token",
)
```

### Additional Setup Required
1. Start Open-Metadata instance
2. Create a Database Service in Open-Metadata UI
3. Use Open-Metadata ingestion to catalog your tables
4. Then run SQLMesh with this integration

## Testing Considerations

- Unit tests still work (mocked client)
- Integration tests require actual Open-Metadata instance
- Tables must be pre-cataloged for lineage to work
- Silent failures if tables don't exist (logged as warnings)

## Backwards Compatibility

- Environment variables: OPENLINEAGE_* env vars still work as fallback
- Package name: Still `sqlmesh-openlineage` (not renamed to avoid breaking changes)
- API: Same `install()` function with same parameters
