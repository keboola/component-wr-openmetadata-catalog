"""keboola.wr-openmetadata-catalog — Keboola -> OpenMetadata catalog + lineage writer.

``run()`` is a thin orchestrator (spec 6.2): validate -> resolve the project set
(Tier-1 row, or Tier-2 enumerate+mint with graceful degradation) -> open the
optional SSH tunnel -> probe the OM version -> per project run the catalog,
pipeline, lineage and tombstone passes with three-way merge -> write the
snapshot/state/report -> raise at the end in collect_and_fail mode.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Any, ClassVar

from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import BaseType, ColumnDefinition
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import MessageType, SelectElement, ValidationResult
from keboola.vcr import BaseSanitizer, DefaultSanitizer

import report as report_mod
from client import ssh_proxy
from client.job_queue_reader import JobQueueReader
from client.manage_client import ManageClient, ManageScopeError
from client.om_client import OMAuthError, OMClient, OMClientError, OMPreconditionFailed
from client.storage_reader import SourceBucket, SourceTable, StorageReader, resolve_storage_credentials
from configuration import BranchFilter, Configuration, FailureMode, ProjectScope, Stage
from lineage.column_lineage import extract_column_lineage
from lineage.dialect import dialect_for
from mapping import fqn as fqn_mod
from mapping import lineage_builder
from mapping.entity_builder import EntityBuilder
from mapping.pipeline_builder import PipelineBuilder
from merge import (
    OUR_LINEAGE_SOURCES,
    OWNED_PIPELINE_FIELDS,
    OWNED_TABLE_FIELDS,
    SnapshotStore,
    ThreeWayMerger,
)
from report import RunReport
from sync import StateManager, TombstonePlanner, bucket_digest, should_process_bucket

logger = logging.getLogger(__name__)


class CredentialScrubber(BaseSanitizer):
    """Recursive, name-agnostic scrubber for *foreign* credentials in bodies.

    ``DefaultSanitizer`` only redacts the handful of secret field names *this*
    component knows about. But some recorded responses echo the configuration of
    *other* components in the project — most dangerously
    ``GET /v2/storage/branch/.../components?include=configuration`` — whose secret
    field names cannot be enumerated ahead of time (foreign RSA/PKCS#8 private
    keys, cloud access-key ids, third-party tokens). This sanitizer walks every
    recorded request/response body and redacts, recursively:

    * any value whose **field name** matches (case-insensitive, separators
      ignored) a credential pattern — ``private_key`` / ``snowflake_private_key``
      / ``access_key`` / ``aws_access_key_id`` / ``accessKeyId`` / ``api_key`` /
      ``api_key_id`` / ``aws_key_id`` / ``secret`` / ``password`` / ``token`` and
      any name ending ``_key`` / ``_secret`` / ``_password`` / ``_token`` or
      containing ``access_key``;
    * any string whose **shape** is a credential — a PEM/PKCS#8 private-key block
      or an AWS access-key id (``AKIA[0-9A-Z]{16}``) — under *any* field name.

    Non-secret fields are preserved: SQL blocks, storage input/output mappings,
    table/column names, ids, and the storage ``primary_key`` list (explicitly
    excluded from the ``*_key`` rule). It runs at record time only, so it affects
    future recordings; already-recorded cassettes are untouched.
    """

    REPLACEMENT: ClassVar[str] = "REDACTED"

    # Matched against the field name lower-cased with every non-alphanumeric
    # character stripped, so ``private_key`` / ``privateKey`` / ``PRIVATE-KEY``
    # all normalize to ``privatekey``.
    _NAME_SUBSTRINGS: ClassVar[tuple[str, ...]] = (
        "privatekey",
        "accesskey",  # access_key, aws_access_key_id, accessKeyId, *ACCESS_KEY*
        "apikey",  # api_key, api_key_id
        "awskeyid",
        "secret",
        "password",
        "token",
    )
    _NAME_SUFFIXES: ClassVar[tuple[str, ...]] = ("key", "secret", "password", "token")
    # Benign ``*_key`` field names that are NOT credentials and must survive.
    _NAME_ALLOWLIST: ClassVar[frozenset[str]] = frozenset({"primarykey"})

    _PEM_RE: ClassVar[re.Pattern[str]] = re.compile(
        r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----",
        re.DOTALL,
    )
    _AWS_KEY_RE: ClassVar[re.Pattern[str]] = re.compile(r"AKIA[0-9A-Z]{16}")

    scrub_before_read: bool = False

    @classmethod
    def _is_credential_name(cls, key: str) -> bool:
        norm = re.sub(r"[^a-z0-9]", "", key.lower())
        if norm in cls._NAME_ALLOWLIST:
            return False
        if any(needle in norm for needle in cls._NAME_SUBSTRINGS):
            return True
        return any(norm.endswith(suffix) for suffix in cls._NAME_SUFFIXES)

    @classmethod
    def _scrub_scalar(cls, value: str) -> str:
        """Redact credential-shaped substrings (PEM block, AWS key id) in a string."""
        value = cls._PEM_RE.sub(cls.REPLACEMENT, value)
        return cls._AWS_KEY_RE.sub(cls.REPLACEMENT, value)

    @classmethod
    def _scrub_value(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return {
                key: (cls.REPLACEMENT if isinstance(key, str) and cls._is_credential_name(key) else cls._scrub_value(v))
                for key, v in value.items()
            }
        if isinstance(value, list):
            return [cls._scrub_value(item) for item in value]
        if isinstance(value, str):
            return cls._scrub_scalar(value)
        return value

    @classmethod
    def scrub_body_text(cls, body: str) -> str:
        """Scrub a body: field-name + value-shape redaction over JSON, shape-only over non-JSON."""
        if not body:
            return body
        try:
            data = json.loads(body)
        except json.JSONDecodeError, ValueError:
            # Not JSON — still strip credential-shaped substrings from the raw text.
            return cls._scrub_scalar(body)
        return json.dumps(cls._scrub_value(data))

    def before_record_request(self, request: Any) -> Any:
        body = getattr(request, "body", None)
        if body:
            if isinstance(body, bytes):
                request.body = self.scrub_body_text(body.decode("utf-8", errors="ignore")).encode("utf-8")
            elif isinstance(body, str):
                request.body = self.scrub_body_text(body)
        return request

    def before_record_response(self, response: dict) -> dict:
        body = response.get("body") if isinstance(response, dict) else None
        if isinstance(body, dict) and "string" in body:
            raw = body["string"]
            if isinstance(raw, bytes):
                body["string"] = self.scrub_body_text(raw.decode("utf-8", errors="ignore")).encode("utf-8")
            elif isinstance(raw, str):
                body["string"] = self.scrub_body_text(raw)
        return response


# VCR sanitizers (spec 7) — consumed by the datadirtest recorder, which loads this
# module-level list via runpy. ``keboola.vcr`` is a production-transitive dependency
# of ``keboola.component``, so this import is also safe in the ``--no-dev`` image.
#
# ``DefaultSanitizer`` covers every secret this writer handles in two ways:
#   1. It whitelists request/response headers to {content-type, content-length,
#      accept}, so the three auth headers that carry our tokens are stripped from
#      every recorded interaction: ``Authorization: Bearer <#bot_token>`` (OM),
#      ``X-StorageApi-Token`` (Keboola Storage token / KBC_TOKEN / Tier-2 minted
#      token) and ``X-KBC-ManageApiToken`` (``#manage_token``).
#   2. It redacts sensitive JSON fields by name in request/response bodies. We add
#      ``token`` — the read-only Storage token the Management API returns in the
#      Tier-2 mint response body (``POST /manage/projects/{id}/tokens``) and that
#      the component then round-trips as a header — plus the ``#``-prefixed config
#      keys, so no secret value survives inside a body either.
#
# The OM host and the Keboola stack host are deliberately NOT sanitized: both are
# public (the OM public sandbox / a public stack URL) and the request URI is the
# VCR match key, so rewriting it would break replay. The SSH ``#private_key`` never
# crosses HTTP (it is consumed by the sshtunnel bastion), so it cannot reach a
# cassette; it is listed for defense-in-depth only.
#
# ``CredentialScrubber`` is the second, name-agnostic layer (see its docstring):
# ``DefaultSanitizer`` can only redact field names it was told about in advance,
# but some responses — notably ``GET /v2/storage/.../components?include=configuration``
# — echo *every other* component's configuration, whose secret field names this
# writer cannot enumerate. The scrubber redacts any credential-shaped field name or
# value recursively so a future recording can never bake in a foreign private key
# or cloud access-key id. It runs at record time only, so the already-recorded
# cassettes are unaffected.
VCR_SANITIZERS = [
    DefaultSanitizer(
        additional_sensitive_fields=[
            "token",  # Tier-2 minted read-only Storage token in the mint response body
            "botToken",
            "jwtToken",
            "privateKey",
            "#bot_token",
            "#storage_token",
            "#manage_token",
            "#private_key",
        ],
    ),
    CredentialScrubber(),
]

_BASE_TYPE_FACTORY = {
    "STRING": BaseType.string,
    "INTEGER": BaseType.integer,
    "TIMESTAMP": BaseType.timestamp,
}


@dataclass
class ProjectContext:
    project_id: str
    project_name: str
    storage_token: str
    storage_url: str


@dataclass
class _ProjectRun:
    """Per-project mutable run context shared across the passes."""

    ctx: ProjectContext
    reader: StorageReader
    entities: EntityBuilder
    pipelines: PipelineBuilder
    seen_table_fqns: set[str] = field(default_factory=set)
    # DatabaseSchema FQNs this run actually enumerated (in-scope buckets only) —
    # the tombstone pass reconciles *only* within these, so allowlist/denylist
    # excluded buckets are never a deletion scope (spec risk #4).
    seen_schema_fqns: set[str] = field(default_factory=set)
    # Cataloged table FQN -> its (sanitised) column names. The column-lineage pass
    # validates each columnsLineage entry against this so it never references a
    # column an endpoint table lacks (empty-stub tables map to an empty set).
    column_catalog: dict[str, set[str]] = field(default_factory=dict)
    pipeline_fqn_by_config: dict[str, str] = field(default_factory=dict)
    id_cache: dict[str, str | None] = field(default_factory=dict)
    failures: int = 0


class Component(ComponentBase):
    """Push-based OpenMetadata catalog writer."""

    def __init__(self) -> None:
        super().__init__()
        self._config: Configuration | None = None

    # ---------------------------------------------------------------- run()

    def run(self) -> None:
        config = self._load_configuration()
        env = self._read_environment()
        report = RunReport(run_id=env["run_id"], config_row_id=env["config_row_id"])
        state = StateManager(self.get_state_file() or {})
        state.run_count += 1

        projects, degraded_reason = self._resolve_projects(config, env)
        proxy = ssh_proxy.maybe_open_tunnel(config)
        try:
            om = self._build_om_client(config, proxy)
            om.probe_version()
            self._apply_version_override(config, om)
            server_version = om.server_version
            version_changed = state.om_version_seen not in (None, server_version)
            snapshot = self._load_snapshot(config, env, projects, state)
            if degraded_reason:
                report.record(
                    project_id=None,
                    entity_type="Project",
                    entity_fqn="*",
                    action=report_mod.ACTION_DEGRADED,
                    detail=degraded_reason,
                )
            for project in projects:
                self._process_project(config, om, project, env, state, snapshot, report, version_changed)
            state.om_version_seen = server_version
        finally:
            if proxy is not None:
                proxy.close()

        self._write_snapshot(snapshot)
        # Record the (stable, explicit) snapshot destination so the next run reads
        # it back as the three-way-merge base (spec 2.3). write_always on the
        # snapshot manifest means the table is uploaded even when the run fails.
        state.snapshot_table = report_mod.SNAPSHOT_DESTINATION
        self._write_report(report)
        self.write_state_file(state.to_dict())
        self._finalize(config, report)

    # -------------------------------------------------------- configuration

    def _load_configuration(self) -> Configuration:
        # The platform sets the root log level to DEBUG from the job `debug` param;
        # no manual setLevel is needed here.
        self._config = Configuration(**self.configuration.parameters)
        return self._config

    @staticmethod
    def _read_environment() -> dict:
        return {
            "stack_id": os.environ.get("KBC_STACKID"),
            "token": os.environ.get("KBC_TOKEN"),
            "url": os.environ.get("KBC_URL"),
            "project_name": os.environ.get("KBC_PROJECTNAME"),
            "run_id": os.environ.get("KBC_RUNID") or "local",
            "config_row_id": os.environ.get("KBC_CONFIGROWID"),
        }

    @staticmethod
    def _ui_base(storage_url: str) -> str:
        return storage_url.rstrip("/")

    def _build_om_client(self, config: Configuration, proxy: ssh_proxy.SshProxy | None) -> OMClient:
        host = proxy.local_om_host if proxy and proxy.local_om_host else config.om_host
        verify_ssl = proxy is None  # a loopback tunnel terminates TLS at the bastion
        return OMClient(host, config.bot_token, verify_ssl=verify_ssl)

    @staticmethod
    def _apply_version_override(config: Configuration, om: OMClient) -> None:
        """Break-glass: force the server version used for the ``is_2_0_or_newer`` gate.

        Called right after ``om.probe_version()`` so the optional ``om_version_override``
        wins over the auto-probed ``/system/version`` value. Normally unset; useful when
        the probe reports a version whose gated 2.0 surface must be forced on/off.
        """
        if config.om_version_override:
            om.server_version = config.om_version_override

    # ------------------------------------------------------ project resolve

    def _resolve_projects(self, config: Configuration, env: dict) -> tuple[list[ProjectContext], str | None]:
        if config.project_scope == ProjectScope.ALL_PROJECTS:
            return self._resolve_tier2(config, env)
        return [self._resolve_host_or_row(config, env)], None

    def _resolve_host_or_row(self, config: Configuration, env: dict) -> ProjectContext:
        token, url = resolve_storage_credentials(
            row_token=config.storage_token,
            injected_token=env["token"],
            injected_url=env["url"],
        )
        reader = StorageReader(url, token)
        info = reader.verify_token()
        owner = info.get("owner", {}) if isinstance(info, dict) else {}
        return ProjectContext(
            project_id=str(owner.get("id") or "unknown"),
            project_name=config.project_name_override or owner.get("name") or env["project_name"] or "project",
            storage_token=token,
            storage_url=url,
        )

    def _resolve_tier2(self, config: Configuration, env: dict) -> tuple[list[ProjectContext], str | None]:
        host = (env["url"] or "https://connection.keboola.com").rstrip("/")
        client = ManageClient(host, config.manage_token or "")
        try:
            minted = client.enumerate_and_mint(config.organization_id)
        except ManageScopeError as exc:
            logger.warning("Tier-2 enumeration failed (%s); degrading to the host project.", exc)
            return [self._resolve_host_or_row(config, env)], f"Tier-2 org enumeration unavailable: {exc}"
        contexts = [
            ProjectContext(
                project_id=m.project_id,
                project_name=m.project_name,
                storage_token=m.storage_token,
                storage_url=m.storage_url,
            )
            for m in minted
        ]
        return contexts, None

    # -------------------------------------------------------------- snapshot

    def _load_snapshot(
        self,
        config: Configuration,
        env: dict,
        projects: list[ProjectContext],
        state: StateManager,
    ) -> SnapshotStore:
        snapshot = SnapshotStore()
        if not state.snapshot_table or not env["token"] or not env["url"]:
            return snapshot
        reader = StorageReader(env["url"], env["token"])
        rows = reader.read_snapshot_rows(state.snapshot_table)
        snapshot.load_rows(rows)
        return snapshot

    # ------------------------------------------------------ per-project run

    def _process_project(
        self,
        config: Configuration,
        om: OMClient,
        ctx: ProjectContext,
        env: dict,
        state: StateManager,
        snapshot: SnapshotStore,
        report: RunReport,
        version_changed: bool,
    ) -> None:
        production_only = config.branch_filter == BranchFilter.PRODUCTION_ONLY
        reader = StorageReader(ctx.storage_url, ctx.storage_token, production_only=production_only)
        ui_base = self._ui_base(ctx.storage_url)
        run = _ProjectRun(
            ctx=ctx,
            reader=reader,
            entities=EntityBuilder(
                config.resolve_service_name(env["stack_id"]), ctx.project_name, ctx.project_id, ui_base
            ),
            pipelines=PipelineBuilder(
                config.resolve_service_name(env["stack_id"]), ctx.project_name, ctx.project_id, ui_base
            ),
        )
        merger = ThreeWayMerger(config.merge_mode)

        self._catalog_pass(config, om, run, state, snapshot, report, merger, version_changed)
        if config.write_pipelines:
            self._pipeline_pass(config, om, run, snapshot, report, merger, env)
        if config.write_lineage or config.write_column_lineage:
            self._lineage_pass(config, om, run, report)
        self._tombstone_pass(om, run, report)

    def _catalog_pass(
        self,
        config: Configuration,
        om: OMClient,
        run: _ProjectRun,
        state: StateManager,
        snapshot: SnapshotStore,
        report: RunReport,
        merger: ThreeWayMerger,
        version_changed: bool,
    ) -> None:
        self._upsert(
            om,
            run,
            "databaseServices",
            "DatabaseService",
            run.entities.database_service_body()["name"],
            run.entities.database_service_body(),
            (),
            snapshot,
            report,
            merger,
        )
        self._upsert(
            om,
            run,
            "databases",
            "Database",
            fqn_mod.database_fqn(run.entities.service_name, run.entities.project),
            run.entities.database_body(),
            ("displayName", "sourceUrl"),
            snapshot,
            report,
            merger,
        )

        full_refresh_due = state.full_refresh_due()
        for bucket in run.reader.list_buckets():
            if not self._bucket_in_scope(config, bucket):
                continue
            # This bucket is in scope this run: its schema is a valid tombstone scope.
            schema_fqn = fqn_mod.schema_fqn(run.entities.service_name, run.entities.project, bucket.path or bucket.name)
            run.seen_schema_fqns.add(schema_fqn)
            tables = list(run.reader.iter_tables(bucket.id))
            digest = bucket_digest(vars(bucket), [vars(t) for t in tables])
            if not should_process_bucket(
                previous_digest=state.bucket_digest(run.ctx.project_id, bucket.id),
                current_digest=digest,
                full_refresh=config.full_refresh,
                version_changed=version_changed,
                full_refresh_due=full_refresh_due,
            ):
                report.record(
                    project_id=run.ctx.project_id,
                    entity_type="Schema",
                    entity_fqn=bucket.id,
                    action=report_mod.ACTION_SKIPPED_UNCHANGED,
                )
                # still mark tables as seen so tombstoning does not delete unchanged tables
                for table in tables:
                    table_fqn = fqn_mod.table_fqn(
                        run.entities.service_name, run.entities.project, bucket.path or bucket.name, table.name
                    )
                    run.seen_table_fqns.add(table_fqn)
                    self._record_column_catalog(run, table_fqn, table)
                continue

            self._upsert(
                om,
                run,
                "databaseSchemas",
                "Schema",
                schema_fqn,
                run.entities.schema_body(bucket),
                ("displayName", "description", "sourceUrl"),
                snapshot,
                report,
                merger,
            )
            bucket_ok = True
            for table in tables:
                built = run.entities.table_body(bucket, table)
                run.seen_table_fqns.add(built.fqn)
                self._record_column_catalog(run, built.fqn, table)
                ok = self._upsert(
                    om, run, "tables", "Table", built.fqn, built.body, OWNED_TABLE_FIELDS, snapshot, report, merger
                )
                bucket_ok = bucket_ok and ok
                if built.view_source_fqn:
                    self._put_lineage(om, run, report, lineage_builder.view_edge(built.fqn, built.view_source_fqn))
            # advance-after-success: persist the digest only once the bucket's writes succeeded
            if bucket_ok:
                state.set_bucket_digest(run.ctx.project_id, bucket.id, digest)

    @staticmethod
    def _record_column_catalog(run: _ProjectRun, table_fqn: str, table: SourceTable) -> None:
        """Record a table's cataloged column set (empty for an unmaterialised stub).

        Feeds the column-lineage pass so a ``columnsLineage`` entry never references
        a column the endpoint table does not actually hold (OM 400).
        """
        run.column_catalog[table_fqn] = {fqn_mod.sanitize_name(col.name) for col in table.columns}

    @staticmethod
    def _bucket_in_scope(config: Configuration, bucket: SourceBucket) -> bool:
        # The component's own bookkeeping bucket (report/snapshot tables) is always
        # excluded, so the writer never catalogs itself even under the default scope.
        if bucket.id == report_mod.OUTPUT_BUCKET:
            return False
        stages = {s.value for s in config.stages} or {Stage.IN.value, Stage.OUT.value}
        if bucket.stage not in stages:
            return False
        if config.bucket_allowlist and bucket.id not in config.bucket_allowlist:
            return False
        return not (config.bucket_denylist and bucket.id in config.bucket_denylist)

    def _pipeline_pass(
        self,
        config: Configuration,
        om: OMClient,
        run: _ProjectRun,
        snapshot: SnapshotStore,
        report: RunReport,
        merger: ThreeWayMerger,
        env: dict,
    ) -> None:
        self._upsert(
            om,
            run,
            "pipelineServices",
            "PipelineService",
            run.pipelines.pipeline_service_body()["name"],
            run.pipelines.pipeline_service_body(),
            (),
            snapshot,
            report,
            merger,
        )
        for component in run.reader.list_component_configs():
            component_id = str(component.get("id") or component.get("componentId") or "")
            for cfg in component.get("configurations") or []:
                if not (run.pipelines.is_flow(component_id) or self._is_producing(cfg)):
                    continue
                built = run.pipelines.build_pipeline(component_id, cfg)
                run.pipeline_fqn_by_config[str(cfg.get("id"))] = built.fqn
                self._upsert(
                    om,
                    run,
                    "pipelines",
                    "Pipeline",
                    built.fqn,
                    built.body,
                    OWNED_PIPELINE_FIELDS,
                    snapshot,
                    report,
                    merger,
                )
                if config.write_pipeline_status:
                    self._push_pipeline_status(run, built.fqn, cfg, env, om, report)

    @staticmethod
    def _is_producing(cfg: dict) -> bool:
        storage = (cfg.get("configuration") or {}).get("storage") or {}
        return bool((storage.get("output") or {}).get("tables") or (storage.get("input") or {}).get("tables"))

    def _push_pipeline_status(self, run, pipeline_fqn, cfg, env, om, report) -> None:
        job_id = ((cfg.get("configuration") or {}).get("_lastJobId")) or cfg.get("lastJobId")
        if not job_id or not env["url"]:
            return
        queue_url = env["url"].replace("connection.", "queue.")
        try:
            events = JobQueueReader(queue_url, run.ctx.storage_token).get_lineage_events(str(job_id))
            record = JobQueueReader.summarize_run(events)
            if record is not None:
                om.put_pipeline_status(pipeline_fqn, record.to_status_body())
        except OMAuthError:
            # Auth stays fatal, consistent with the per-entity write paths (_upsert /
            # _put_lineage) — an invalid #bot_token must fail the run, not be swallowed.
            raise
        except (UserException, OMClientError) as exc:
            # ``put_pipeline_status`` -> ``_request`` raises OMNotFound/OMClientError (NOT
            # UserException) on a 404/other 4xx-5xx — e.g. a status PUT to a pipeline whose
            # upsert failed. This is a best-effort push (like _put_lineage / _tombstone_schema):
            # record the failure and continue rather than propagating an uncaught exit-2.
            logger.warning("Pipeline status skipped for %s: %s", pipeline_fqn, exc)
            report.record(
                project_id=run.ctx.project_id,
                entity_type="Pipeline",
                entity_fqn=pipeline_fqn,
                action=report_mod.ACTION_FAILED,
                detail=f"pipeline status: {exc}",
            )

    def _lineage_pass(self, config: Configuration, om: OMClient, run: _ProjectRun, report: RunReport) -> None:
        edges: list[lineage_builder.LineageEdge] = []
        for component in run.reader.list_component_configs():
            component_id = str(component.get("id") or component.get("componentId") or "")
            for cfg in component.get("configurations") or []:
                storage = (cfg.get("configuration") or {}).get("storage") or {}
                pipeline_fqn = run.pipeline_fqn_by_config.get(str(cfg.get("id")))
                if config.write_lineage:
                    edges += lineage_builder.declared_edges(
                        storage,
                        service_name=run.entities.service_name,
                        project=run.entities.project,
                        pipeline_fqn=pipeline_fqn,
                    )
                if config.write_column_lineage:
                    edges += self._column_edges_for(config, run, component_id, cfg, storage, pipeline_fqn, report)
        self._refresh_lineage(om, run, report, edges)

    def _column_edges_for(self, config, run, component_id, cfg, storage, pipeline_fqn, report):
        dialect = dialect_for(component_id=component_id)
        if not dialect.is_sql:
            return []
        statements = self._statements_of(cfg)
        in_map = {
            t.get("destination"): t.get("source")
            for t in (storage.get("input") or {}).get("tables") or []
            if t.get("destination") and t.get("source")
        }
        out_map = {
            t.get("source"): t.get("destination")
            for t in (storage.get("output") or {}).get("tables") or []
            if t.get("source") and t.get("destination")
        }
        result = extract_column_lineage(statements, in_map=in_map, out_map=out_map, dialect=dialect)
        for note in result.unresolved_notes:
            report.record(
                project_id=run.ctx.project_id,
                entity_type="Column",
                entity_fqn=str(cfg.get("id")),
                action=report_mod.ACTION_UNRESOLVED,
                detail=note,
            )
        return lineage_builder.column_edges(
            result,
            service_name=run.entities.service_name,
            project=run.entities.project,
            pipeline_fqn=pipeline_fqn,
            column_catalog=run.column_catalog,
        )

    @staticmethod
    def _statements_of(cfg: dict) -> list[tuple[str, str]]:
        params = (cfg.get("configuration") or {}).get("parameters") or {}
        statements: list[tuple[str, str]] = []
        for block in params.get("blocks") or []:
            for code in block.get("codes") or []:
                for script in code.get("script") or []:
                    statements.append((code.get("name", "?"), script))
        for query in params.get("queries") or []:
            statements.append(("legacy", query))
        return statements

    def _refresh_lineage(self, om, run, report, edges) -> None:
        # drop our stale edges (never Manual) on affected targets, then add current
        for target_fqn in {e.to_fqn for e in edges}:
            for source in OUR_LINEAGE_SOURCES:
                try:
                    om.delete_lineage_by_source("table", target_fqn, source)
                except Exception:
                    logger.debug("lineage cleanup skipped for %s/%s", target_fqn, source, exc_info=True)
        for edge in edges:
            self._put_lineage(om, run, report, edge)

    def _put_lineage(self, om, run, report, edge) -> None:
        request = lineage_builder.to_add_lineage_request(edge, lambda f, t: self._resolve_id(om, run, f, t))
        if request is None:
            return
        try:
            om.put_lineage(request)
            report.record(
                project_id=run.ctx.project_id,
                entity_type="Lineage",
                entity_fqn=f"{edge.from_fqn}->{edge.to_fqn}",
                action=report_mod.ACTION_UPDATED,
                detail=edge.source,
            )
        except OMAuthError:
            raise
        except Exception as exc:  # noqa: BLE001 - a single edge failure is per-entity
            report.record(
                project_id=run.ctx.project_id,
                entity_type="Lineage",
                entity_fqn=f"{edge.from_fqn}->{edge.to_fqn}",
                action=report_mod.ACTION_FAILED,
                detail=str(exc),
            )

    def _resolve_id(self, om: OMClient, run: _ProjectRun, fqn: str, entity_type: str) -> str | None:
        cache_key = f"{entity_type}:{fqn}"
        if cache_key not in run.id_cache:
            kind = "pipelines" if entity_type == "pipeline" else "tables"
            entity = om.get_by_fqn(kind, fqn)
            run.id_cache[cache_key] = entity.get("id") if entity else None
        return run.id_cache[cache_key]

    def _tombstone_pass(self, om: OMClient, run: _ProjectRun, report: RunReport) -> None:
        """Reconcile stale OM tables — scoped to the buckets this run enumerated.

        Scope-safety (spec risk #4 / §6.2 step 8): reconciliation happens *per
        DatabaseSchema* the run actually enumerated, never over the whole Database.
        Buckets excluded by the stage / allowlist / denylist filters — or not
        enumerated at all this run — are never a deletion scope, so a scoped run
        (e.g. a ``bucket_allowlist`` subset) can never tombstone entities that
        belong to the rest of the catalog. The delete path stays fail-closed within
        each schema (skip on a listing error or an implausibly short scope).
        """
        for schema_fqn in sorted(run.seen_schema_fqns):
            seen_in_schema = sorted(f for f in run.seen_table_fqns if f.startswith(f"{schema_fqn}."))
            self._tombstone_schema(om, run, report, schema_fqn, seen_in_schema)

    def _tombstone_schema(
        self,
        om: OMClient,
        run: _ProjectRun,
        report: RunReport,
        schema_fqn: str,
        seen_in_schema: list[str],
    ) -> None:
        if om.is_2_0_or_newer:
            # 2.0+ deleteStale, scoped to this DatabaseSchema (not the whole Database).
            body = TombstonePlanner.deletestale_body(schema_fqn, "databaseSchema", seen_in_schema, dry_run=False)
            try:
                om.delete_stale(body)
            except Exception as exc:  # noqa: BLE001 - tombstoning never fails the run
                logger.warning("deleteStale failed for %s (fail-closed): %s", schema_fqn, exc)
            return
        try:
            listed = [
                fqn
                for t in om.list_entities("tables", {"databaseSchema": schema_fqn})
                if (fqn := t.get("fullyQualifiedName"))
            ]
        except Exception as exc:  # noqa: BLE001 - fail closed on listing error
            logger.warning("Tombstone listing failed for %s (fail-closed): %s", schema_fqn, exc)
            return
        plan = TombstonePlanner.self_diff(listed, seen_in_schema)
        if plan.blocked:
            logger.warning("Tombstoning skipped for %s (fail-closed): %s", schema_fqn, plan.fail_closed_reason)
            return
        for stale_fqn in plan.to_delete:
            entity = om.get_by_fqn("tables", stale_fqn)
            if entity and entity.get("id"):
                om.soft_delete("tables", entity["id"])
                report.record(
                    project_id=run.ctx.project_id,
                    entity_type="Table",
                    entity_fqn=stale_fqn,
                    action=report_mod.ACTION_TOMBSTONED,
                )

    # ---------------------------------------------------------- merge upsert

    def _upsert(self, om, run, kind, entity_type, entity_fqn, desired, owned_fields, snapshot, report, merger) -> bool:
        """Create-or-merge one entity; returns True on success (for advance-after-success)."""
        try:
            current = om.get_by_fqn(kind, entity_fqn, fields="columns,tableConstraints" if kind == "tables" else None)
            base = snapshot.base_fields(entity_fqn)
            decision = merger.merge(
                desired=desired, current=current, base=base, owned_fields=owned_fields or tuple(desired.keys())
            )
            status_code = None
            if decision.is_create:
                created = om.put_entity(kind, desired)
                status_code = 200
                if created.get("id"):
                    run.id_cache[f"{'pipeline' if entity_type == 'Pipeline' else 'table'}:{entity_fqn}"] = created["id"]
            elif decision.patch:
                self._apply_patch(om, kind, entity_fqn, decision.patch)
                status_code = 200
            snapshot.record(
                entity_fqn,
                entity_type,
                decision.snapshot_fields or {k: desired[k] for k in (owned_fields or desired) if k in desired},
            )
            report.record(
                project_id=run.ctx.project_id,
                entity_type=entity_type,
                entity_fqn=entity_fqn,
                action=decision.action,
                om_status_code=status_code,
                detail=",".join(decision.diverged_fields),
            )
            return True
        except OMAuthError:
            raise
        except Exception as exc:  # noqa: BLE001 - per-entity failure handled per failure_mode
            return self._handle_entity_failure(run, report, entity_type, entity_fqn, exc)

    @staticmethod
    def _apply_patch(om: OMClient, kind: str, fqn: str, patch: list[dict]) -> None:
        try:
            om.patch_entity(kind, fqn, patch)
        except OMPreconditionFailed:
            om.patch_entity(kind, fqn, patch)  # refetch-and-retry-once (server merges on 412)

    def _handle_entity_failure(self, run, report, entity_type, entity_fqn, exc) -> bool:
        run.failures += 1
        report.record(
            project_id=run.ctx.project_id,
            entity_type=entity_type,
            entity_fqn=entity_fqn,
            action=report_mod.ACTION_FAILED,
            detail=str(exc),
        )
        if self._config and self._config.failure_mode == FailureMode.FAIL_FAST:
            raise UserException(f"Failed writing {entity_type} {entity_fqn}: {exc}") from exc
        return False

    # ------------------------------------------------------------- outputs

    def _write_report(self, report: RunReport) -> None:
        schema = {
            entry["name"]: ColumnDefinition(
                data_types=_BASE_TYPE_FACTORY.get(entry["base_type"], BaseType.string)(),
                primary_key=entry["primary_key"],
            )
            for entry in report_mod.report_schema()
        }
        table = self.create_out_table_definition(
            f"{report_mod.REPORT_TABLE}.csv",
            destination=report_mod.REPORT_DESTINATION,
            schema=schema,
            primary_key=list(report_mod.REPORT_PRIMARY_KEY),
            incremental=True,
            write_always=True,
            has_header=True,
        )
        self._write_rows(table.full_path, report_mod.REPORT_COLUMNS, report.rows())
        self.write_manifest(table)

    def _write_snapshot(self, snapshot: SnapshotStore) -> None:
        rows = report_mod.snapshot_rows(snapshot.entries())
        table = self.create_out_table_definition(
            f"{report_mod.SNAPSHOT_TABLE}.csv",
            destination=report_mod.SNAPSHOT_DESTINATION,
            schema=list(report_mod.SNAPSHOT_COLUMNS),
            primary_key=list(report_mod.SNAPSHOT_PRIMARY_KEY),
            incremental=True,
            write_always=True,
            has_header=True,
        )
        self._write_rows(table.full_path, report_mod.SNAPSHOT_COLUMNS, rows)
        self.write_manifest(table)

    @staticmethod
    def _write_rows(path: str, columns, rows) -> None:
        with open(path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(columns))
            writer.writeheader()
            for row in rows:
                writer.writerow({col: row.get(col, "") for col in columns})

    def _finalize(self, config: Configuration, report: RunReport) -> None:
        counts = report.counts()
        logger.info("Run summary: %s", dict(counts))
        if config.failure_mode == FailureMode.COLLECT_AND_FAIL and report.has_failures():
            raise UserException(
                f"{counts.get(report_mod.ACTION_FAILED, 0)} entity write(s) failed; see catalog_run_report."
            )

    # -------------------------------------------------------- sync actions

    @sync_action("testConnection")
    def test_connection(self) -> ValidationResult:
        config = Configuration(**self.configuration.parameters)
        env = self._read_environment()
        om = OMClient(config.om_host, config.bot_token)
        try:
            om.probe_version()
            self._apply_version_override(config, om)
            version = om.server_version
            # /system/version is unauthenticated (OM JwtFilter.EXCLUDED_ENDPOINTS),
            # so it only checks reachability. Follow with an authenticated call so an
            # invalid/expired #bot_token is surfaced here instead of only at run time.
            om.verify_auth()
        except OMAuthError as exc:
            raise UserException(str(exc)) from exc
        token, url = resolve_storage_credentials(
            row_token=config.storage_token, injected_token=env["token"], injected_url=env["url"]
        )
        StorageReader(url, token).verify_token()
        return ValidationResult(
            f"Connected to OpenMetadata {version} (bot token valid); Storage token valid.", MessageType.SUCCESS
        )

    @sync_action("listBuckets")
    def list_buckets(self) -> list[SelectElement]:
        config = Configuration(**self.configuration.parameters)
        env = self._read_environment()
        token, url = resolve_storage_credentials(
            row_token=config.storage_token, injected_token=env["token"], injected_url=env["url"]
        )
        reader = StorageReader(url, token, production_only=config.branch_filter == BranchFilter.PRODUCTION_ONLY)
        return [SelectElement(value=b.id, label=f"{b.id} ({b.display_name or b.name})") for b in reader.list_buckets()]


if __name__ == "__main__":
    try:
        Component().execute_action()
    except UserException as exc:
        logger.error(str(exc))
        sys.exit(1)
    except Exception:
        logger.exception("Component failed with an unexpected error")
        sys.exit(2)
