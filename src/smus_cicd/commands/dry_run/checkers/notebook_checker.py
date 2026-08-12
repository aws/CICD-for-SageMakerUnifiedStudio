"""Notebook sync dry-run checker (Phase: Notebook Sync).

Validates notebook sync prerequisites when a bundle contains a
``notebooks/notebook_export_manifest.json`` file:

  1. Verify the target project's ``default.s3_shared`` connection exists and
     the resolved S3 bucket is reachable (HEAD request).
  2. Verify IAM permissions via SimulatePrincipalPolicy:
       datazone:StartNotebookSync, datazone:UpdateNotebook,
       datazone:GetNotebook, datazone:ListNotebooks, s3:PutObject
  3. Report the number of notebooks that would be synced from the manifest's
     ``notebookCount`` field.

Mirrors ``CatalogChecker`` in structure — reports WARNING (not ERROR) for
missing S3 connection or denied permissions so the dry-run continues.

Requirements: 8.1, 8.2, 8.3, 8.4, 8.5
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from smus_cicd.commands.dry_run.models import DryRunContext, Finding, Severity

logger = logging.getLogger(__name__)

# IAM actions that the caller needs for notebook sync
_REQUIRED_NOTEBOOK_ACTIONS = [
    "datazone:StartNotebookSync",
    "datazone:UpdateNotebook",
    "datazone:GetNotebook",
    "datazone:ListNotebooks",
    "s3:PutObject",
]

_NOTEBOOK_MANIFEST_PATH = "notebooks/notebook_export_manifest.json"


def _is_notebook_sync_disabled(context: DryRunContext) -> bool:
    """Return True if notebook sync is disabled in deployment_configuration."""
    target = context.target_config
    if not target:
        return False
    dep_cfg = getattr(target, "deployment_configuration", None)
    if not dep_cfg:
        return False
    nb_cfg = getattr(dep_cfg, "notebooks", None)
    if not nb_cfg:
        return False
    if isinstance(nb_cfg, dict):
        return nb_cfg.get("disable", False)
    return getattr(nb_cfg, "disable", False)


class NotebookChecker:
    """Validates notebook sync prerequisites during dry-run."""

    def check(self, context: DryRunContext) -> List[Finding]:
        findings: List[Finding] = []

        # Skip if manifest has no notebook export or sync is disabled
        if _NOTEBOOK_MANIFEST_PATH not in context.bundle_files:
            findings.append(
                Finding(
                    severity=Severity.OK,
                    message="Bundle contains no notebook manifest — notebook sync will be skipped.",
                    service="datazone",
                )
            )
            return findings

        if _is_notebook_sync_disabled(context):
            findings.append(
                Finding(
                    severity=Severity.OK,
                    message="Notebook sync is disabled in deployment_configuration — skipping.",
                    service="datazone",
                )
            )
            return findings

        # ── 1. Read notebookCount from the manifest ───────────────────────
        notebook_count = self._read_notebook_count(context, findings)

        # Report the planned sync count
        findings.append(
            Finding(
                severity=Severity.OK,
                message=(
                    f"Notebook sync would process {notebook_count} notebook(s) "
                    f"from the bundle manifest."
                ),
                service="datazone",
                details={"notebookCount": notebook_count},
            )
        )

        # ── 2. Check S3 connection ────────────────────────────────────────
        s3_uri = self._check_s3_connection(context, findings)

        # ── 3. Check IAM permissions ─────────────────────────────────────
        if context.target_region:
            self._check_iam_permissions(context, s3_uri, findings)

        return findings

    # ── helpers ──────────────────────────────────────────────────────────────

    def _read_notebook_count(
        self, context: DryRunContext, findings: List[Finding]
    ) -> int:
        """Extract notebookCount from the manifest in the bundle."""
        if not context.bundle_path:
            return 0
        try:
            import json
            import zipfile

            from smus_cicd.helpers.bundle_storage import ensure_bundle_local

            local_path = ensure_bundle_local(
                context.bundle_path, context.target_region or "us-east-1"
            )
            with zipfile.ZipFile(local_path, "r") as zf:
                if _NOTEBOOK_MANIFEST_PATH in zf.namelist():
                    with zf.open(_NOTEBOOK_MANIFEST_PATH) as mf:
                        data = json.load(mf)
                    return int(data.get("metadata", {}).get("notebookCount", 0))
        except Exception as exc:
            logger.debug("Could not read notebookCount from bundle: %s", exc)
        return 0

    def _check_s3_connection(
        self, context: DryRunContext, findings: List[Finding]
    ) -> Optional[str]:
        """
        Verify the target project's ``default.s3_shared`` connection exists and
        the resolved S3 bucket is reachable via a HEAD request.

        Returns the s3Uri on success, None otherwise.
        Appends a WARNING finding on any failure.
        """
        try:
            from smus_cicd.commands.dry_run.checkers import get_project_connections

            connections = get_project_connections(
                context, context.target_region or "us-east-1"
            )
            s3_conn = connections.get("default.s3_shared", {})
            s3_uri = s3_conn.get("s3Uri", "")

            if not s3_uri:
                findings.append(
                    Finding(
                        severity=Severity.WARNING,
                        message=(
                            "Target project 'default.s3_shared' connection "
                            "not found or has no s3Uri. Notebook .ipynb files "
                            "cannot be uploaded before sync."
                        ),
                        service="s3",
                        resource="default.s3_shared",
                    )
                )
                return None

            # HEAD the bucket to verify reachability
            bucket = s3_uri.replace("s3://", "").split("/")[0]
            try:
                from smus_cicd.helpers.boto3_client import create_client

                s3_client = create_client("s3", region=context.target_region or "us-east-1")
                s3_client.head_bucket(Bucket=bucket)
                findings.append(
                    Finding(
                        severity=Severity.OK,
                        message=f"S3 connection 'default.s3_shared' bucket '{bucket}' is reachable.",
                        service="s3",
                        resource="default.s3_shared",
                    )
                )
            except Exception as exc:
                findings.append(
                    Finding(
                        severity=Severity.WARNING,
                        message=(
                            f"S3 bucket '{bucket}' from 'default.s3_shared' connection "
                            f"is not reachable: {exc}"
                        ),
                        service="s3",
                        resource="default.s3_shared",
                    )
                )
                return None

            return s3_uri

        except Exception as exc:
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    message=f"Could not resolve S3 connection for notebook sync: {exc}",
                    service="s3",
                    resource="default.s3_shared",
                )
            )
            return None

    def _check_iam_permissions(
        self,
        context: DryRunContext,
        s3_uri: Optional[str],
        findings: List[Finding],
    ) -> None:
        """
        Check IAM permissions via SimulatePrincipalPolicy.

        Reports a WARNING finding for each denied action.
        """
        region = context.target_region or "us-east-1"

        try:
            from smus_cicd.helpers.boto3_client import create_client

            sts = create_client("sts", region=region)
            caller = sts.get_caller_identity()
            caller_arn = caller.get("Arn", "")
            account_id = caller.get("Account", "")

            if not caller_arn:
                findings.append(
                    Finding(
                        severity=Severity.WARNING,
                        message="Could not determine caller ARN for IAM permission check.",
                        service="iam",
                    )
                )
                return

            # Build resource ARNs for simulation
            domain_id = context.target_domain_id or "*"
            datazone_resource = f"arn:aws:datazone:{region}:{account_id}:domain/{domain_id}"

            s3_resource = "*"
            if s3_uri:
                bucket = s3_uri.replace("s3://", "").split("/")[0]
                s3_resource = f"arn:aws:s3:::{bucket}/*"

            # Map each action to its resource ARN
            action_resources: Dict[str, str] = {
                "datazone:StartNotebookSync": datazone_resource,
                "datazone:UpdateNotebook": datazone_resource,
                "datazone:GetNotebook": datazone_resource,
                "datazone:ListNotebooks": datazone_resource,
                "s3:PutObject": s3_resource,
            }

            iam = create_client("iam", region=region)

            for action, resource_arn in action_resources.items():
                try:
                    resp = iam.simulate_principal_policy(
                        PolicySourceArn=caller_arn,
                        ActionNames=[action],
                        ResourceArns=[resource_arn],
                    )
                    for result in resp.get("EvaluationResults", []):
                        decision = result.get("EvalDecision", "")
                        if decision != "allowed":
                            findings.append(
                                Finding(
                                    severity=Severity.WARNING,
                                    message=(
                                        f"IAM permission '{action}' is denied "
                                        f"on resource '{resource_arn}' "
                                        f"(decision: {decision}). "
                                        f"Notebook sync may fail at deploy time."
                                    ),
                                    service="iam",
                                    resource=action,
                                    details={
                                        "action": action,
                                        "resource": resource_arn,
                                        "decision": decision,
                                    },
                                )
                            )
                        else:
                            findings.append(
                                Finding(
                                    severity=Severity.OK,
                                    message=f"IAM permission '{action}' is allowed.",
                                    service="iam",
                                    resource=action,
                                )
                            )
                except Exception as exc:
                    logger.debug(
                        "Could not simulate policy for action %s: %s", action, exc
                    )
                    findings.append(
                        Finding(
                            severity=Severity.WARNING,
                            message=(
                                f"Could not verify IAM permission '{action}': {exc}. "
                                f"Ensure the caller has this permission for notebook sync."
                            ),
                            service="iam",
                            resource=action,
                        )
                    )

        except Exception as exc:
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    message=f"IAM permission check for notebook sync failed: {exc}",
                    service="iam",
                )
            )
