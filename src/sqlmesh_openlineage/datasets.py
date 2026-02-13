"""Convert SQLMesh Snapshots to Open-Metadata format."""
from __future__ import annotations

import typing as t
from collections import defaultdict

if t.TYPE_CHECKING:
    from sqlmesh.core.snapshot import Snapshot
    from sqlmesh.core.model import Model


def snapshot_to_table_name(snapshot: "Snapshot") -> str:
    """Convert snapshot to table name (without namespace)."""
    qvn = snapshot.qualified_view_name
    parts = [qvn.catalog, qvn.schema_name, qvn.table]
    return ".".join(p for p in parts if p)


def snapshot_to_table_fqn(snapshot: "Snapshot", namespace: str) -> str:
    """Convert snapshot to fully qualified name for Open-Metadata.

    Open-Metadata FQN format: <service>.<database>.<schema>.<table>
    For example: demo_pg.postgres.public.actor
    """
    table_name = snapshot_to_table_name(snapshot)
    return f"{namespace}.{table_name}"


def snapshot_to_column_lineage(
    snapshot: "Snapshot",
    parent_name: str,
    namespace: str,
) -> t.List[t.Any]:
    """Extract column-level lineage for Open-Metadata.

    Returns a list of ColumnLineage objects showing which upstream
    columns flow into each output column from a specific parent.
    """
    from metadata.generated.schema.type.entityLineage import ColumnLineage

    if not snapshot.is_model:
        return []

    model = snapshot.model
    if not model:
        return []

    columns = getattr(model, "columns_to_types", None)
    if not columns:
        return []

    column_lineages: t.List[ColumnLineage] = []

    try:
        from sqlmesh.core.lineage import lineage
        from sqlglot import exp

        for col_name in columns.keys():
            try:
                # Get lineage for this column
                node = lineage(col_name, model, trim_selects=False)

                # Walk the lineage tree to find source columns from this parent
                from_columns: t.List[str] = []

                for lineage_node in node.walk():
                    # Skip nodes that have downstream (not leaf nodes)
                    if lineage_node.downstream:
                        continue

                    # Find the source table
                    table = lineage_node.expression.find(exp.Table)
                    if table:
                        # Get table name components
                        table_parts = [table.catalog, table.db, table.name]
                        table_name = ".".join(p for p in table_parts if p)

                        # Check if this table matches the parent we're looking for
                        # Use exact matching on the table name component or full path
                        if table.name == parent_name or table_name == parent_name:
                            # Get column name
                            source_col = exp.to_column(lineage_node.name).name

                            # Build FQN for source column
                            # Note: parent_name should contain the full table identifier
                            from_col_fqn = f"{namespace}.{parent_name}.{source_col}"
                            from_columns.append(from_col_fqn)

                if from_columns:
                    # Build FQN for target column
                    output_fqn = snapshot_to_table_fqn(snapshot, namespace)
                    to_col_fqn = f"{output_fqn}.{col_name}"

                    column_lineages.append(
                        ColumnLineage(
                            fromColumns=from_columns,
                            toColumn=to_col_fqn,
                        )
                    )

            except Exception:
                # Skip columns we can't trace
                continue

    except Exception:
        # If lineage extraction fails, return empty list
        pass

    return column_lineages
