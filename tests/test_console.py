"""Tests for OpenLineageConsole."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch


class TestOpenLineageConsole:
    """Tests for OpenLineageConsole wrapper."""

    def test_init(self, mock_console, mock_openlineage_client):
        """Test console initialization."""
        from sqlmesh_openlineage.console import OpenLineageConsole

        console = OpenLineageConsole(
            wrapped=mock_console,
            url="http://localhost:5000",
            namespace="test",
        )

        assert console._wrapped is mock_console
        assert console._emitter.namespace == "test"
        assert len(console._active_runs) == 0

    def test_getattr_delegates(self, mock_console, mock_openlineage_client):
        """Test that unknown attributes delegate to wrapped console."""
        from sqlmesh_openlineage.console import OpenLineageConsole

        mock_console.some_method = MagicMock(return_value="result")

        console = OpenLineageConsole(
            wrapped=mock_console,
            url="http://localhost:5000",
            namespace="test",
        )

        result = console.some_method()
        assert result == "result"
        mock_console.some_method.assert_called_once()

    def test_start_snapshot_emits_start_event(
        self, mock_console, mock_snapshot, mock_openlineage_client
    ):
        """Test that start_snapshot_evaluation_progress emits START event."""
        from sqlmesh_openlineage.console import OpenLineageConsole

        console = OpenLineageConsole(
            wrapped=mock_console,
            url="http://localhost:5000",
            namespace="test",
        )

        with patch.object(console._emitter, "emit_snapshot_start") as mock_emit:
            console.start_snapshot_evaluation_progress(mock_snapshot, audit_only=False)

            mock_emit.assert_called_once()
            assert mock_snapshot.name in console._active_runs

        # Verify delegation
        mock_console.start_snapshot_evaluation_progress.assert_called_once_with(
            mock_snapshot, False
        )

    def test_update_snapshot_emits_complete_event(
        self, mock_console, mock_snapshot, mock_openlineage_client
    ):
        """Test that update_snapshot_evaluation_progress emits COMPLETE event."""
        from sqlmesh_openlineage.console import OpenLineageConsole

        console = OpenLineageConsole(
            wrapped=mock_console,
            url="http://localhost:5000",
            namespace="test",
        )

        # Simulate start first
        console._active_runs[mock_snapshot.name] = "test-run-id"

        interval = MagicMock()

        with patch.object(console._emitter, "emit_snapshot_complete") as mock_emit:
            console.update_snapshot_evaluation_progress(
                snapshot=mock_snapshot,
                interval=interval,
                batch_idx=0,
                duration_ms=1000,
                num_audits_passed=1,
                num_audits_failed=0,
            )

            mock_emit.assert_called_once()
            assert mock_snapshot.name not in console._active_runs

    def test_update_snapshot_emits_fail_on_audit_failure(
        self, mock_console, mock_snapshot, mock_openlineage_client
    ):
        """Test that audit failures emit FAIL event."""
        from sqlmesh_openlineage.console import OpenLineageConsole

        console = OpenLineageConsole(
            wrapped=mock_console,
            url="http://localhost:5000",
            namespace="test",
        )

        console._active_runs[mock_snapshot.name] = "test-run-id"

        interval = MagicMock()

        with patch.object(console._emitter, "emit_snapshot_fail") as mock_emit:
            console.update_snapshot_evaluation_progress(
                snapshot=mock_snapshot,
                interval=interval,
                batch_idx=0,
                duration_ms=1000,
                num_audits_passed=0,
                num_audits_failed=2,
            )

            mock_emit.assert_called_once()
            assert "audit" in mock_emit.call_args[1]["error"].lower()

    def test_start_passes_snapshots_dict_to_emitter(
        self, mock_console, mock_snapshot, mock_openlineage_client
    ):
        """Test that start_snapshot_evaluation_progress passes current_snapshots to emitter."""
        from sqlmesh_openlineage.console import OpenLineageConsole

        console = OpenLineageConsole(
            wrapped=mock_console,
            url="http://localhost:5000",
            namespace="test",
        )

        with patch.object(console._emitter, "emit_snapshot_start") as mock_emit:
            console.start_snapshot_evaluation_progress(mock_snapshot, audit_only=False)

            mock_emit.assert_called_once()
            call_kwargs = mock_emit.call_args[1]
            assert "snapshots" in call_kwargs
            assert call_kwargs["snapshots"] is console._current_snapshots

    def test_complete_passes_snapshots_dict_to_emitter(
        self, mock_console, mock_snapshot, mock_openlineage_client
    ):
        """Test that update_snapshot_evaluation_progress passes current_snapshots to emitter."""
        from sqlmesh_openlineage.console import OpenLineageConsole

        console = OpenLineageConsole(
            wrapped=mock_console,
            url="http://localhost:5000",
            namespace="test",
        )

        console._active_runs[mock_snapshot.name] = "test-run-id"

        interval = MagicMock()

        with patch.object(console._emitter, "emit_snapshot_complete") as mock_emit:
            console.update_snapshot_evaluation_progress(
                snapshot=mock_snapshot,
                interval=interval,
                batch_idx=0,
                duration_ms=1000,
                num_audits_passed=1,
                num_audits_failed=0,
            )

            mock_emit.assert_called_once()
            call_kwargs = mock_emit.call_args[1]
            assert "snapshots" in call_kwargs
            assert call_kwargs["snapshots"] is console._current_snapshots


    def test_emit_failure_does_not_crash_console(
        self, mock_console, mock_snapshot, mock_openlineage_client
    ):
        """Transport errors should not propagate to SQLMesh."""
        from sqlmesh_openlineage.console import OpenLineageConsole

        console = OpenLineageConsole(
            wrapped=mock_console,
            url="http://localhost:5000",
            namespace="test",
        )

        console._emitter.emit_snapshot_start = MagicMock(
            side_effect=ConnectionError("unreachable")
        )

        # Should not raise
        console.start_snapshot_evaluation_progress(mock_snapshot, audit_only=False)

        # Delegation should still happen
        mock_console.start_snapshot_evaluation_progress.assert_called_once_with(
            mock_snapshot, False
        )

    def test_emit_complete_failure_does_not_crash_console(
        self, mock_console, mock_snapshot, mock_openlineage_client
    ):
        """Transport errors on complete should not propagate."""
        from sqlmesh_openlineage.console import OpenLineageConsole

        console = OpenLineageConsole(
            wrapped=mock_console,
            url="http://localhost:5000",
            namespace="test",
        )

        console._active_runs[mock_snapshot.name] = "test-run-id"
        console._emitter.emit_snapshot_complete = MagicMock(
            side_effect=ConnectionError("unreachable")
        )

        interval = MagicMock()

        # Should not raise
        console.update_snapshot_evaluation_progress(
            snapshot=mock_snapshot,
            interval=interval,
            batch_idx=0,
            duration_ms=1000,
            num_audits_passed=1,
            num_audits_failed=0,
        )

        # Delegation should still happen
        mock_console.update_snapshot_evaluation_progress.assert_called_once()


class TestEmitterFacets:
    """Tests for new emitter facets (job type, SQL, processing engine, source code)."""

    def test_build_job_facets_includes_job_type(self, mock_snapshot, mock_openlineage_client):
        """Test that _build_job_facets includes JobTypeJobFacet."""
        from sqlmesh_openlineage.emitter import OpenLineageEmitter

        emitter = OpenLineageEmitter(url="http://localhost:5000", namespace="test")
        facets = emitter._build_job_facets(mock_snapshot)

        assert "jobType" in facets
        assert facets["jobType"].processingType == "BATCH"
        assert facets["jobType"].integration == "SQLMESH"
        assert facets["jobType"].jobType == "MODEL"

    def test_build_job_facets_includes_sql(self, mock_snapshot, mock_openlineage_client):
        """Test that _build_job_facets includes SQLJobFacet when query is available."""
        from sqlmesh_openlineage.emitter import OpenLineageEmitter

        mock_snapshot.model.query = MagicMock()
        mock_snapshot.model.query.__str__ = lambda self: "SELECT id, name FROM source"

        emitter = OpenLineageEmitter(url="http://localhost:5000", namespace="test")
        facets = emitter._build_job_facets(mock_snapshot)

        assert "sql" in facets
        assert facets["sql"].query == "SELECT id, name FROM source"

    def test_build_job_facets_no_sql_when_no_query(self, mock_snapshot, mock_openlineage_client):
        """Test that SQL facet is omitted when model has no query."""
        from sqlmesh_openlineage.emitter import OpenLineageEmitter

        mock_snapshot.model.query = None

        emitter = OpenLineageEmitter(url="http://localhost:5000", namespace="test")
        facets = emitter._build_job_facets(mock_snapshot)

        assert "sql" not in facets

    def test_build_job_facets_source_code_location(
        self, mock_snapshot, mock_openlineage_client
    ):
        """Test that _build_job_facets includes SourceCodeLocationJobFacet."""
        from sqlmesh_openlineage.emitter import OpenLineageEmitter

        mock_snapshot.model._path = "/path/to/models/test_model.sql"

        emitter = OpenLineageEmitter(url="http://localhost:5000", namespace="test")
        facets = emitter._build_job_facets(mock_snapshot)

        assert "sourceCodeLocation" in facets
        assert facets["sourceCodeLocation"].type == "file"
        assert facets["sourceCodeLocation"].url == "file:///path/to/models/test_model.sql"

    def test_build_job_facets_no_source_code_when_no_path(
        self, mock_snapshot, mock_openlineage_client
    ):
        """Test that source code location is omitted when model has no _path."""
        from sqlmesh_openlineage.emitter import OpenLineageEmitter

        # Simulate missing _path by raising AttributeError
        del mock_snapshot.model._path

        emitter = OpenLineageEmitter(url="http://localhost:5000", namespace="test")
        facets = emitter._build_job_facets(mock_snapshot)

        assert "sourceCodeLocation" not in facets

    def test_build_processing_engine_facet(self, mock_openlineage_client):
        """Test that _build_processing_engine_facet returns correct facet."""
        from sqlmesh_openlineage.emitter import OpenLineageEmitter

        emitter = OpenLineageEmitter(url="http://localhost:5000", namespace="test")
        facets = emitter._build_processing_engine_facet()

        assert "processing_engine" in facets
        pe = facets["processing_engine"]
        assert pe.name == "SQLMesh"
        assert pe.version  # should be non-empty
        assert pe.openlineageAdapterVersion == "0.1.0"

    def test_emitter_with_api_key_uses_client_options(self):
        """Verify api_key creates OpenLineageClientOptions, not dict."""
        from sqlmesh_openlineage.emitter import OpenLineageEmitter
        from openlineage.client.client import OpenLineageClientOptions

        with patch("openlineage.client.OpenLineageClient") as mock_cls:
            mock_cls.return_value = MagicMock()
            emitter = OpenLineageEmitter(url="http://test:5000", api_key="my-key")
            call_kwargs = mock_cls.call_args[1]
            assert isinstance(call_kwargs["options"], OpenLineageClientOptions)
            assert call_kwargs["options"].api_key == "my-key"

    def test_producer_uri(self, mock_openlineage_client):
        """Test that PRODUCER constant is a proper URI."""
        from sqlmesh_openlineage.emitter import PRODUCER

        assert PRODUCER == "https://github.com/sidequery/sqlmesh-openlineage"
