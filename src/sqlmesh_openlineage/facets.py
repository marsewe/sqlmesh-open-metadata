"""Placeholder for compatibility - Open-Metadata doesn't use facets like OpenLineage."""
from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    from sqlmesh.core.snapshot.execution_tracker import QueryExecutionStats


def build_run_facets(
    duration_ms: t.Optional[int] = None,
    execution_stats: t.Optional["QueryExecutionStats"] = None,
) -> t.Dict[str, t.Any]:
    """Build run facets - not used in Open-Metadata."""
    return {}


def build_output_facets(
    execution_stats: t.Optional["QueryExecutionStats"] = None,
) -> t.Dict[str, t.Any]:
    """Build output dataset facets - not used in Open-Metadata."""
    return {}
