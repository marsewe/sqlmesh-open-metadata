"""Open-Metadata event emitter for SQLMesh."""
from __future__ import annotations

import logging
import typing as t
from datetime import datetime, timezone

if t.TYPE_CHECKING:
    from sqlmesh.core.snapshot import Snapshot
    from sqlmesh.core.snapshot.definition import Interval
    from sqlmesh.core.snapshot.execution_tracker import QueryExecutionStats

logger = logging.getLogger(__name__)


class OpenLineageEmitter:
    """Emits Open-Metadata lineage for SQLMesh operations."""

    def __init__(
        self,
        url: str,
        namespace: str = "sqlmesh",
        api_key: t.Optional[str] = None,
    ):
        from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
            OpenMetadataConnection,
        )
        from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
            OpenMetadataJWTClientConfig,
        )
        from metadata.ingestion.ometa.ometa_api import OpenMetadata

        self.namespace = namespace
        self.url = url
        self.api_key = api_key

        # Create Open-Metadata connection
        if api_key:
            server_config = OpenMetadataConnection(
                hostPort=url,
                authProvider="openmetadata",
                securityConfig=OpenMetadataJWTClientConfig(jwtToken=api_key),
            )
        else:
            # For testing or non-authenticated setups
            server_config = OpenMetadataConnection(
                hostPort=url,
            )

        self.client = OpenMetadata(server_config)

        # Cache for table entities - we'll need to fetch them to get IDs
        self._table_cache: t.Dict[str, t.Any] = {}

    def _get_or_create_table(self, table_fqn: str) -> t.Optional[t.Any]:
        """Get or create a table entity in Open-Metadata.

        For now, we'll try to get the table. In a real implementation,
        you might want to create tables that don't exist.
        """
        if table_fqn in self._table_cache:
            return self._table_cache[table_fqn]

        try:
            from metadata.generated.schema.entity.data.table import Table

            table_entity = self.client.get_by_name(entity=Table, fqn=table_fqn)
            if table_entity:
                self._table_cache[table_fqn] = table_entity
                return table_entity
        except Exception:
            # Table doesn't exist yet - could create it here if needed
            pass

        return None

    def emit_snapshot_start(
        self,
        snapshot: "Snapshot",
        run_id: str,
    ) -> None:
        """Emit lineage when snapshot evaluation starts.

        In Open-Metadata, we emit lineage once when we know the dependencies.
        Unlike OpenLineage's START/COMPLETE events, Open-Metadata uses a simpler
        lineage model.
        """
        # We'll emit the actual lineage in emit_snapshot_complete
        # This is just a placeholder for compatibility
        pass

    def emit_snapshot_complete(
        self,
        snapshot: "Snapshot",
        run_id: str,
        interval: t.Optional["Interval"] = None,
        duration_ms: t.Optional[int] = None,
        execution_stats: t.Optional["QueryExecutionStats"] = None,
    ) -> None:
        """Emit lineage to Open-Metadata when snapshot completes."""
        from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
        from metadata.generated.schema.type.entityLineage import (
            EntitiesEdge,
            LineageDetails,
            ColumnLineage,
        )
        from metadata.generated.schema.type.entityReference import EntityReference
        from metadata.generated.schema.entity.data.table import Table

        from sqlmesh_openlineage.datasets import (
            snapshot_to_table_fqn,
            snapshot_to_column_lineage,
        )

        if not snapshot.is_model:
            return

        # Get output table FQN
        output_fqn = snapshot_to_table_fqn(snapshot, self.namespace)

        # Try to get the output table entity
        output_table = self._get_or_create_table(output_fqn)
        if not output_table:
            # Table doesn't exist in Open-Metadata yet, skip lineage
            return

        # Get parent snapshots and emit lineage for each
        for parent_id in snapshot.parents:
            # Build parent FQN
            # Note: parent_id.name should contain the full table identifier
            # (e.g., 'catalog.schema.table' or 'schema.table' depending on SQLMesh config)
            parent_fqn = f"{self.namespace}.{parent_id.name}"
            parent_table = self._get_or_create_table(parent_fqn)

            if not parent_table:
                # Parent table doesn't exist, skip this lineage edge
                continue

            # Build column lineage
            column_lineages = snapshot_to_column_lineage(snapshot, parent_id.name, self.namespace)

            # Create lineage details with column lineage if available
            lineage_details = None
            if column_lineages:
                lineage_details = LineageDetails(
                    columnsLineage=column_lineages,
                )

            # Create lineage request
            add_lineage_request = AddLineageRequest(
                edge=EntitiesEdge(
                    fromEntity=EntityReference(id=parent_table.id, type="table"),
                    toEntity=EntityReference(id=output_table.id, type="table"),
                    lineageDetails=lineage_details,
                ),
            )

            try:
                self.client.add_lineage(data=add_lineage_request)
            except Exception as e:
                # Use logging instead of print
                logger.warning(
                    f"Failed to add lineage from {parent_fqn} to {output_fqn}: {e}"
                )

    def emit_snapshot_fail(
        self,
        snapshot: "Snapshot",
        run_id: str,
        error: t.Union[str, Exception],
    ) -> None:
        """Handle snapshot failure.

        Open-Metadata doesn't have a direct equivalent to OpenLineage FAIL events.
        We could potentially log this or create a data quality incident, but for
        now we'll just skip it.
        """
        # Could log to Open-Metadata data quality if desired
        pass
