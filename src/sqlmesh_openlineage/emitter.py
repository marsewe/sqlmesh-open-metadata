"""OpenLineage event emitter for SQLMesh."""
from __future__ import annotations

import logging
import typing as t
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

if t.TYPE_CHECKING:
    from sqlmesh.core.snapshot import Snapshot
    from sqlmesh.core.snapshot.definition import Interval
    from sqlmesh.core.snapshot.execution_tracker import QueryExecutionStats

PRODUCER = "https://github.com/sidequery/sqlmesh-openlineage"


class OpenLineageEmitter:
    """Emits OpenLineage events for SQLMesh operations."""

    def __init__(
        self,
        url: str,
        namespace: str = "sqlmesh",
        api_key: t.Optional[str] = None,
    ):
        from openlineage.client import OpenLineageClient
        from openlineage.client.client import OpenLineageClientOptions

        self.namespace = namespace
        self.url = url

        # Use console transport for console:// URLs (for testing)
        if url.startswith("console://"):
            from openlineage.client.transport.console import ConsoleTransport, ConsoleConfig

            self.client = OpenLineageClient(transport=ConsoleTransport(ConsoleConfig()))
        elif api_key:
            self.client = OpenLineageClient(
                url=url,
                options=OpenLineageClientOptions(api_key=api_key),
            )
        else:
            self.client = OpenLineageClient(url=url)

    def _build_job_facets(self, snapshot: "Snapshot") -> t.Dict[str, t.Any]:
        """Build job facets including SQL, job type, and source code location."""
        from openlineage.client.facet_v2 import job_type_job, sql_job, source_code_location_job

        facets: t.Dict[str, t.Any] = {}

        # JobTypeJobFacet: identify as SQLMesh batch job
        facets["jobType"] = job_type_job.JobTypeJobFacet(
            processingType="BATCH",
            integration="SQLMESH",
            jobType="MODEL",
        )

        # SQLJobFacet: include the model SQL query
        try:
            if snapshot.is_model and snapshot.model:
                query = snapshot.model.query
                if query is not None:
                    sql_str = str(query)
                    if sql_str:
                        facets["sql"] = sql_job.SQLJobFacet(query=sql_str)
        except Exception:
            pass

        # SourceCodeLocationJobFacet: include model file path
        try:
            if snapshot.is_model and snapshot.model:
                model_path = getattr(snapshot.model, "_path", None)
                if model_path is not None:
                    path_str = str(model_path)
                    if path_str:
                        facets["sourceCodeLocation"] = (
                            source_code_location_job.SourceCodeLocationJobFacet(
                                type="file",
                                url=f"file://{path_str}",
                            )
                        )
        except Exception:
            pass

        return facets

    def _build_processing_engine_facet(self) -> t.Dict[str, t.Any]:
        """Build run facets for processing engine info."""
        from openlineage.client.facet_v2 import processing_engine_run

        facets: t.Dict[str, t.Any] = {}

        try:
            from sqlmesh import __version__ as sqlmesh_version
        except ImportError:
            sqlmesh_version = "unknown"

        try:
            from sqlmesh_openlineage import __version__ as adapter_version
        except ImportError:
            adapter_version = "unknown"

        facets["processing_engine"] = processing_engine_run.ProcessingEngineRunFacet(
            version=sqlmesh_version,
            name="SQLMesh",
            openlineageAdapterVersion=adapter_version,
        )

        return facets

    def emit_snapshot_start(
        self,
        snapshot: "Snapshot",
        run_id: str,
        snapshots: t.Optional[t.Dict[str, "Snapshot"]] = None,
    ) -> None:
        """Emit a START event for snapshot evaluation."""
        from openlineage.client.event_v2 import RunEvent, RunState, Run, Job

        from sqlmesh_openlineage.datasets import (
            snapshot_to_output_dataset,
            snapshot_to_input_datasets,
        )

        inputs = snapshot_to_input_datasets(snapshot, self.namespace, snapshots=snapshots)
        output = snapshot_to_output_dataset(snapshot, self.namespace)

        job_facets = self._build_job_facets(snapshot)
        run_facets = self._build_processing_engine_facet()

        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId=run_id, facets=run_facets),
            job=Job(namespace=self.namespace, name=snapshot.name, facets=job_facets),
            inputs=inputs,
            outputs=[output] if output else [],
            producer=PRODUCER,
        )
        try:
            self.client.emit(event)
        except Exception:
            logger.warning("Failed to emit %s event for %s", event.eventType, snapshot.name, exc_info=True)

    def emit_snapshot_complete(
        self,
        snapshot: "Snapshot",
        run_id: str,
        interval: t.Optional["Interval"] = None,
        duration_ms: t.Optional[int] = None,
        execution_stats: t.Optional["QueryExecutionStats"] = None,
        snapshots: t.Optional[t.Dict[str, "Snapshot"]] = None,
    ) -> None:
        """Emit a COMPLETE event for snapshot evaluation."""
        from openlineage.client.event_v2 import RunEvent, RunState, Run, Job

        from sqlmesh_openlineage.datasets import (
            snapshot_to_output_dataset,
            snapshot_to_input_datasets,
        )
        from sqlmesh_openlineage.facets import build_run_facets, build_output_facets

        run_facets = build_run_facets(
            duration_ms=duration_ms,
            execution_stats=execution_stats,
        )
        run_facets.update(self._build_processing_engine_facet())

        output = snapshot_to_output_dataset(
            snapshot,
            self.namespace,
            facets=build_output_facets(execution_stats),
        )

        inputs = snapshot_to_input_datasets(snapshot, self.namespace, snapshots=snapshots)

        job_facets = self._build_job_facets(snapshot)

        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId=run_id, facets=run_facets),
            job=Job(namespace=self.namespace, name=snapshot.name, facets=job_facets),
            inputs=inputs,
            outputs=[output] if output else [],
            producer=PRODUCER,
        )
        try:
            self.client.emit(event)
        except Exception:
            logger.warning("Failed to emit %s event for %s", event.eventType, snapshot.name, exc_info=True)

    def emit_snapshot_fail(
        self,
        snapshot: "Snapshot",
        run_id: str,
        error: t.Union[str, Exception],
    ) -> None:
        """Emit a FAIL event for snapshot evaluation."""
        from openlineage.client.event_v2 import RunEvent, RunState, Run, Job
        from openlineage.client.facet_v2 import error_message_run

        error_msg = str(error)

        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(
                runId=run_id,
                facets={
                    "errorMessage": error_message_run.ErrorMessageRunFacet(
                        message=error_msg,
                        programmingLanguage="python",
                    )
                },
            ),
            job=Job(namespace=self.namespace, name=snapshot.name),
            producer=PRODUCER,
        )
        try:
            self.client.emit(event)
        except Exception:
            logger.warning("Failed to emit %s event for %s", event.eventType, snapshot.name, exc_info=True)
