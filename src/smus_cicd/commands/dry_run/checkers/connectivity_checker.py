"""Connectivity and reachability checker (Phase 4).

Verifies that target AWS resources are reachable:
- DataZone domain via ``datazone:GetDomain``
- DataZone project existence via ``get_project_by_name()``
- S3 buckets via ``s3:HeadBucket``
- Airflow environment reachability when workflow bootstrap actions are configured

Requirements: 6.1, 6.2, 6.3, 6.4, 6.5
"""

from __future__ import annotations

import logging
from typing import List, Set

from smus_cicd.commands.dry_run.checkers import get_project_connections
from smus_cicd.commands.dry_run.models import DryRunContext, Finding, Severity
from smus_cicd.helpers.boto3_client import create_client

logger = logging.getLogger(__name__)


class ConnectivityChecker:
    """Verifies reachability of target AWS resources."""

    def check(self, context: DryRunContext) -> List[Finding]:
        findings: List[Finding] = []

        config = context.config
        region = config.get("region")

        # Resolve domain_id — may come from config directly, or via
        # domain name/tags lookup when the manifest uses tags.
        domain_id = config.get("domain_id")
        if not domain_id:
            try:
                from smus_cicd.helpers.utils import _resolve_domain_id

                domain_id = _resolve_domain_id(config, region)
            except Exception:
                pass

        # Resolve project_name — may come from config or target_config.
        project_cfg = getattr(context.target_config, "project", None)
        project_name = config.get("project_name")
        if not project_name:
            project_name = getattr(project_cfg, "name", None) if project_cfg else None
        create_flag = getattr(project_cfg, "create", False) if project_cfg else False

        # --- Step 1: Verify DataZone domain reachability (Req 6.1) ---
        self._check_domain(domain_id, region, findings)

        # --- Step 2: Verify DataZone project existence (Req 6.2) ---
        self._check_project(project_name, domain_id, region, create_flag, findings)

        # --- Step 3: Verify S3 bucket accessibility (Req 6.3) ---
        self._check_s3_buckets(context, region, findings)

        # --- Step 4: Verify Airflow environment reachability (Req 6.4) ---
        self._check_airflow(context, region, findings)

        return findings

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _check_domain(
        self,
        domain_id: str | None,
        region: str,
        findings: List[Finding],
    ) -> None:
        """Call ``datazone:GetDomain`` to verify domain reachability."""
        if not domain_id:
            findings.append(
                Finding(
                    severity=Severity.ERROR,
                    message="No domain_id in config; skipping domain reachability check",
                    service="datazone",
                )
            )
            return

        try:
            dz = create_client("datazone", region=region)
            dz.get_domain(identifier=domain_id)
            findings.append(
                Finding(
                    severity=Severity.OK,
                    message=f"DataZone domain '{domain_id}' is reachable",
                    resource=domain_id,
                    service="datazone",
                )
            )
        except Exception as exc:
            error_code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
            detail = f"{error_code} — {exc}" if error_code else str(exc)
            findings.append(
                Finding(
                    severity=Severity.ERROR,
                    message=f"DataZone domain '{domain_id}' is unreachable: {detail}",
                    resource=domain_id,
                    service="datazone",
                    details={"error_code": error_code} if error_code else None,
                )
            )

    def _check_project(
        self,
        project_name: str | None,
        domain_id: str | None,
        region: str,
        create_flag: bool,
        findings: List[Finding],
    ) -> None:
        """Check project existence via ``get_project_by_name()``.

        ``create_flag`` must mirror ``ProjectChecker``'s own ``project.create``
        check exactly: a missing project with ``create: false`` has to come out
        as an ERROR in both phases. Previously this method ignored ``create``
        entirely and always reported WARNING for a missing project, while
        ``ProjectChecker`` (Phase 5, same report) reported ERROR for the exact
        same ``create: false`` case — surfacing contradictory severities for
        one fact in the same dry-run output ("project not found, might be
        created" next to "project not found, deployment would fail").
        """
        if not project_name or not domain_id:
            findings.append(
                Finding(
                    severity=Severity.ERROR,
                    message=(
                        "No project_name or domain_id in config; "
                        "skipping project existence check"
                    ),
                    service="datazone",
                )
            )
            return

        try:
            from smus_cicd.helpers.datazone import get_project_by_name

            project = get_project_by_name(project_name, domain_id, region)
            if project is not None:
                findings.append(
                    Finding(
                        severity=Severity.OK,
                        message=(
                            f"DataZone project '{project_name}' exists "
                            f"in domain '{domain_id}'"
                        ),
                        resource=project_name,
                        service="datazone",
                    )
                )
            elif create_flag:
                findings.append(
                    Finding(
                        severity=Severity.WARNING,
                        message=(
                            f"DataZone project '{project_name}' not found "
                            f"in domain '{domain_id}', but create=True — "
                            "it would be created during deployment."
                        ),
                        resource=project_name,
                        service="datazone",
                    )
                )
            else:
                findings.append(
                    Finding(
                        severity=Severity.ERROR,
                        message=(
                            f"DataZone project '{project_name}' not found "
                            f"in domain '{domain_id}' and create=False. "
                            "Deployment would fail."
                        ),
                        resource=project_name,
                        service="datazone",
                    )
                )
        except Exception as exc:
            findings.append(
                Finding(
                    severity=Severity.ERROR,
                    message=(
                        f"Failed to check project '{project_name}' "
                        f"in domain '{domain_id}': {exc}"
                    ),
                    resource=project_name,
                    service="datazone",
                )
            )

    def _check_s3_buckets(
        self,
        context: DryRunContext,
        region: str,
        findings: List[Finding],
    ) -> None:
        """Call ``s3:HeadBucket`` for each unique S3 bucket.

        Bucket names are resolved through DataZone project connections when
        possible.  If the project or connection cannot be resolved, a warning
        is emitted instead of a false-positive error.
        """
        resolved, unresolved = self._resolve_s3_buckets(context, region, findings)

        # Warn about connections that couldn't be resolved
        for conn_name in sorted(unresolved):
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    message=(
                        f"Could not resolve S3 bucket for connection "
                        f"'{conn_name}'; skipping reachability check. "
                        f"The actual bucket is determined at deploy time "
                        f"via the DataZone project connection."
                    ),
                    resource=conn_name,
                    service="s3",
                )
            )

        if not resolved:
            return

        s3 = create_client("s3", region=region)

        for bucket in sorted(resolved):
            try:
                s3.head_bucket(Bucket=bucket)
                findings.append(
                    Finding(
                        severity=Severity.OK,
                        message=f"S3 bucket '{bucket}' is accessible",
                        resource=bucket,
                        service="s3",
                    )
                )
            except Exception as exc:
                error_code = (
                    getattr(exc, "response", {}).get("Error", {}).get("Code", "")
                )
                detail = f"{error_code} — {exc}" if error_code else str(exc)
                findings.append(
                    Finding(
                        severity=Severity.ERROR,
                        message=f"S3 bucket '{bucket}' is not accessible: {detail}",
                        resource=bucket,
                        service="s3",
                        details={"error_code": error_code} if error_code else None,
                    )
                )

    def _resolve_s3_buckets(
        self,
        context: DryRunContext,
        region: str,
        findings: List[Finding],
    ) -> tuple:
        """Resolve S3 bucket names from DataZone project connections.

        Returns ``(resolved_buckets, unresolved_connection_names)`` where
        *resolved_buckets* is a set of real bucket names obtained from the
        connection's ``s3Uri`` and *unresolved_connection_names* is a set of
        connection names that could not be resolved (project missing, API
        error, etc.).
        """
        resolved: Set[str] = set()
        unresolved: Set[str] = set()

        target = context.target_config
        if not target or not target.deployment_configuration:
            return resolved, unresolved

        storage_items = target.deployment_configuration.storage or []
        connection_names: Set[str] = set()
        for item in storage_items:
            conn = getattr(item, "connectionName", None)
            if conn:
                connection_names.add(conn)

        if not connection_names:
            return resolved, unresolved

        # Try to resolve connections via DataZone project info
        project_connections = get_project_connections(context, region)

        for conn_name in connection_names:
            conn_info = (
                project_connections.get(conn_name) if project_connections else None
            )
            if conn_info:
                s3_uri = conn_info.get("s3Uri", "")
                if s3_uri:
                    bucket = s3_uri.replace("s3://", "").rstrip("/").split("/")[0]
                    if bucket:
                        resolved.add(bucket)
                        continue
            # Could not resolve — record as unresolved
            unresolved.add(conn_name)

        return resolved, unresolved

    def _check_airflow(
        self,
        context: DryRunContext,
        region: str,
        findings: List[Finding],
    ) -> None:
        """Check Airflow environment reachability if workflow bootstrap actions exist."""
        if not self._has_workflow_bootstrap_actions(context):
            return

        try:
            from smus_cicd.helpers.airflow_serverless import list_workflows

            list_workflows(region=region, max_results=1)
            findings.append(
                Finding(
                    severity=Severity.OK,
                    message="Airflow Serverless environment is reachable",
                    service="airflow-serverless",
                )
            )
        except Exception as exc:
            findings.append(
                Finding(
                    severity=Severity.ERROR,
                    message=(f"Airflow Serverless environment is unreachable: {exc}"),
                    service="airflow-serverless",
                    details={"error": str(exc)},
                )
            )

    @staticmethod
    def _has_workflow_bootstrap_actions(context: DryRunContext) -> bool:
        """Return True if any bootstrap action type starts with 'workflow.'."""
        target = context.target_config
        if not target:
            return False

        bootstrap = getattr(target, "bootstrap", None)
        if not bootstrap:
            return False

        actions = getattr(bootstrap, "actions", None) or []
        return any(
            getattr(action, "type", "").startswith("workflow.") for action in actions
        )
