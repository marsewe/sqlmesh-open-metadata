"""Tests for dataset conversion."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock


class TestDatasetConversion:
    """Tests for snapshot to dataset conversion."""

    def test_snapshot_to_table_name(self, mock_snapshot):
        """Test table name extraction from snapshot."""
        from sqlmesh_openlineage.datasets import snapshot_to_table_name

        name = snapshot_to_table_name(mock_snapshot)
        assert name == "catalog.schema.test_model"

    def test_snapshot_to_table_name_no_catalog(self):
        """Test table name without catalog."""
        from sqlmesh_openlineage.datasets import snapshot_to_table_name

        snapshot = MagicMock()
        qualified_name = MagicMock()
        qualified_name.catalog = None
        qualified_name.schema_name = "schema"
        qualified_name.table = "model"
        snapshot.qualified_view_name = qualified_name

        name = snapshot_to_table_name(snapshot)
        assert name == "schema.model"

    def test_snapshot_to_table_fqn(self, mock_snapshot):
        """Test fully qualified name for Open-Metadata."""
        from sqlmesh_openlineage.datasets import snapshot_to_table_fqn

        fqn = snapshot_to_table_fqn(mock_snapshot, namespace="test_service")
        assert fqn == "test_service.catalog.schema.test_model"

    def test_snapshot_to_column_lineage_empty(self, mock_snapshot):
        """Test column lineage with no parents."""
        from sqlmesh_openlineage.datasets import snapshot_to_column_lineage

        mock_snapshot.parents = []

        lineages = snapshot_to_column_lineage(
            mock_snapshot, parent_name="parent_model", namespace="test"
        )

        # Should return empty list since there are no matching parents
        assert isinstance(lineages, list)
